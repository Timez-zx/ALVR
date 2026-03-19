use alvr_common::{
    anyhow::Result, error, info, once_cell::sync::Lazy, parking_lot::Mutex, ConnectionError,
};
use alvr_packets::{
    LatencyTestConfig, LatencyTestControlMessage, LatencyTestFrameHeader, LatencyTestFrameReport,
    LatencyTestSensorPacket, LATENCY_TEST_DATA_PORT, LATENCY_TEST_FRAME_STREAM_ID,
    LATENCY_TEST_MAX_PACKET_SIZE, LATENCY_TEST_MAX_SHARD_DATA_SIZE, LATENCY_TEST_PORT,
    LATENCY_TEST_SENSOR_STREAM_ID,
};
use alvr_session::{SocketBufferSize, SocketProtocol};
use alvr_sockets::{StreamReceiver, StreamSender, StreamSocket, StreamSocketBuilder};
use std::{
    io::{Read, Write},
    net::{TcpListener, TcpStream},
    sync::{
        atomic::{AtomicBool, AtomicU64, Ordering},
        mpsc, Arc,
    },
    thread,
    time::{Duration, Instant},
};

const CONNECT_TIMEOUT: Duration = Duration::from_secs(5);
const READ_TIMEOUT: Duration = Duration::from_secs(1);
const DATA_RECV_TIMEOUT: Duration = Duration::from_millis(10);
const FRAME_RECV_TIMEOUT: Duration = Duration::from_millis(50);
const DRAIN_GRACE_PERIOD: Duration = Duration::from_millis(500);
const MAX_UNREAD_FRAMES: usize = 256;

/// Global latency test client instance
pub static LATENCY_TEST_CLIENT: Lazy<Mutex<LatencyTestClient>> =
    Lazy::new(|| Mutex::new(LatencyTestClient::new()));

pub struct LatencyTestClient {
    running: Arc<AtomicBool>,
    listener_thread: Option<thread::JoinHandle<()>>,
}

impl LatencyTestClient {
    pub fn new() -> Self {
        Self {
            running: Arc::new(AtomicBool::new(false)),
            listener_thread: None,
        }
    }

    pub fn start_listener(&mut self) -> Result<()> {
        info!("Latency test listener: start_listener() called");

        if self.running.load(Ordering::Relaxed) {
            info!("Latency test listener already running");
            return Ok(());
        }

        info!(
            "Latency test listener: attempting to bind to port {}",
            LATENCY_TEST_PORT
        );
        let listener = match TcpListener::bind(("0.0.0.0", LATENCY_TEST_PORT)) {
            Ok(l) => {
                info!("Latency test listener: bind successful");
                l
            }
            Err(e) => {
                error!("Latency test listener: bind failed: {}", e);
                return Err(e.into());
            }
        };
        listener.set_nonblocking(true)?;

        info!(
            "Latency test listener started on port {}",
            LATENCY_TEST_PORT
        );

        self.running.store(true, Ordering::Relaxed);
        let running = Arc::clone(&self.running);

        self.listener_thread = Some(thread::spawn(move || {
            listener_loop(listener, running);
        }));

        Ok(())
    }

    pub fn stop_listener(&mut self) {
        self.running.store(false, Ordering::Relaxed);
        if let Some(thread) = self.listener_thread.take() {
            thread.join().ok();
        }
        info!("Latency test listener stopped");
    }

    pub fn is_running(&self) -> bool {
        self.running.load(Ordering::Relaxed)
    }
}

impl Drop for LatencyTestClient {
    fn drop(&mut self) {
        self.stop_listener();
    }
}

fn listener_loop(listener: TcpListener, running: Arc<AtomicBool>) {
    while running.load(Ordering::Relaxed) {
        match listener.accept() {
            Ok((stream, addr)) => {
                info!("Latency test connection from {}", addr);
                if let Err(e) = handle_connection(stream, Arc::clone(&running)) {
                    error!("Latency test connection error: {}", e);
                }
            }
            Err(ref e) if e.kind() == std::io::ErrorKind::WouldBlock => {
                thread::sleep(Duration::from_millis(100));
            }
            Err(e) => {
                error!("Latency test listener error: {}", e);
                break;
            }
        }
    }
}

fn send_message(stream: &mut TcpStream, msg: &LatencyTestControlMessage) -> Result<()> {
    let data = bincode::serialize(msg)?;
    let len = data.len() as u32;
    stream.write_all(&len.to_be_bytes())?;
    stream.write_all(&data)?;
    stream.flush()?;
    Ok(())
}

fn recv_message(stream: &mut TcpStream) -> Result<LatencyTestControlMessage> {
    let mut len_buf = [0u8; 4];
    stream.read_exact(&mut len_buf)?;
    let len = u32::from_be_bytes(len_buf) as usize;

    let mut data = vec![0u8; len];
    stream.read_exact(&mut data)?;

    Ok(bincode::deserialize(&data)?)
}

/// Non-destructive TCP check: peek first to confirm >= 4 bytes available,
/// then read in blocking mode. Avoids partial-read corruption on a
/// nonblocking stream.
fn try_recv_message(stream: &mut TcpStream) -> Option<LatencyTestControlMessage> {
    let mut peek_buf = [0u8; 4];
    stream.set_nonblocking(true).ok()?;
    let peeked = stream.peek(&mut peek_buf).unwrap_or(0);
    stream.set_nonblocking(false).ok();
    if peeked < 4 {
        return None;
    }
    recv_message(stream).ok()
}

fn handle_connection(mut stream: TcpStream, running: Arc<AtomicBool>) -> Result<()> {
    stream.set_read_timeout(Some(READ_TIMEOUT))?;
    stream.set_nodelay(true)?;

    let config = match recv_message(&mut stream) {
        Ok(LatencyTestControlMessage::StartTest(config)) => {
            info!("Received latency test start command: {:?}", config);
            send_message(&mut stream, &LatencyTestControlMessage::Ack)?;
            config
        }
        Ok(other) => {
            error!("Unexpected message: {:?}", other);
            return Ok(());
        }
        Err(e) => {
            error!("Failed to receive start command: {}", e);
            return Err(e);
        }
    };

    let server_ip = stream.peer_addr()?.ip();
    run_latency_test(&mut stream, config, running, server_ip)?;

    Ok(())
}

/// Calculate expected shards count for a given frame size (same formula as server)
fn calculate_expected_shards_count(frame_size_kb: u32) -> u32 {
    let frame_size_bytes = frame_size_kb as usize * 1024;

    let header_size = bincode::serialized_size(&LatencyTestFrameHeader {
        frame_index: 0,
        client_send_timestamp_ns: 0,
        server_recv_timestamp_ns: 0,
        server_send_timestamp_ns: 0,
    })
    .unwrap_or(32) as usize;

    let first_shard_data = LATENCY_TEST_MAX_SHARD_DATA_SIZE - header_size;

    if frame_size_bytes <= first_shard_data {
        1
    } else {
        let remaining = frame_size_bytes - first_shard_data;
        1 + ((remaining as f64) / (LATENCY_TEST_MAX_SHARD_DATA_SIZE as f64)).ceil() as u32
    }
}

fn data_receive_loop(
    mut stream_socket: StreamSocket,
    recv_running: Arc<AtomicBool>,
    stop_flag: Arc<AtomicBool>,
) {
    while recv_running.load(Ordering::Relaxed) {
        match stream_socket.recv() {
            Ok(()) => (),
            Err(ConnectionError::TryAgain(_)) => continue,
            Err(e) => {
                error!("Latency test data receive error: {}", e);
                stop_flag.store(true, Ordering::Relaxed);
                break;
            }
        }
    }
}

fn frame_process_loop(
    mut frame_receiver: StreamReceiver<LatencyTestFrameHeader>,
    report_tx: mpsc::Sender<LatencyTestFrameReport>,
    start_time: Instant,
    expected_shards_count: u32,
    sent_frame_count: Arc<AtomicU64>,
) {
    let mut next_expected_frame_index = 0_u64;

    loop {
        match frame_receiver.recv(FRAME_RECV_TIMEOUT) {
            Ok(packet) => {
                let recv_time_ns = start_time.elapsed().as_nanos() as u64;
                let header = match packet.get_header() {
                    Ok(header) => header,
                    Err(e) => {
                        error!("Failed to decode latency test frame header: {}", e);
                        continue;
                    }
                };

                let sent_frames = sent_frame_count.load(Ordering::Relaxed);
                while next_expected_frame_index < header.frame_index
                    && next_expected_frame_index < sent_frames
                {
                    report_tx
                        .send(LatencyTestFrameReport {
                            frame_index: next_expected_frame_index,
                            shards_sent: expected_shards_count,
                            shards_received: 0,
                            rtt_us: 0,
                        })
                        .ok();
                    next_expected_frame_index += 1;
                }

                if header.frame_index < next_expected_frame_index {
                    continue;
                }

                let rtt_us =
                    recv_time_ns.saturating_sub(header.client_send_timestamp_ns) / 1000;
                report_tx
                    .send(LatencyTestFrameReport {
                        frame_index: header.frame_index,
                        shards_sent: expected_shards_count,
                        shards_received: expected_shards_count,
                        rtt_us,
                    })
                    .ok();
                next_expected_frame_index = header.frame_index + 1;
            }
            Err(ConnectionError::TryAgain(_)) => continue,
            Err(e) => {
                info!("Latency test frame receiver finished: {}", e);
                break;
            }
        }
    }

    let sent_frames = sent_frame_count.load(Ordering::Relaxed);
    while next_expected_frame_index < sent_frames {
        report_tx
            .send(LatencyTestFrameReport {
                frame_index: next_expected_frame_index,
                shards_sent: expected_shards_count,
                shards_received: 0,
                rtt_us: 0,
            })
            .ok();
        next_expected_frame_index += 1;
    }
}

/// TCP thread — drains completed reports and, every 200 ms, checks whether the
/// server has sent a StopTest command. Exits when `report_rx` is closed.
fn tcp_report_loop(
    mut stream: TcpStream,
    report_rx: mpsc::Receiver<LatencyTestFrameReport>,
    stop_flag: Arc<AtomicBool>,
) {
    let mut total_frames_reported: u64 = 0;

    loop {
        match report_rx.recv_timeout(Duration::from_millis(200)) {
            Ok(report) => {
                if send_message(&mut stream, &LatencyTestControlMessage::FrameReport(report))
                    .is_err()
                {
                    return;
                }
                total_frames_reported += 1;

                while let Ok(report) = report_rx.try_recv() {
                    send_message(&mut stream, &LatencyTestControlMessage::FrameReport(report))
                        .ok();
                    total_frames_reported += 1;
                }
            }
            Err(mpsc::RecvTimeoutError::Timeout) => {
                if let Some(msg) = try_recv_message(&mut stream) {
                    if matches!(msg, LatencyTestControlMessage::StopTest) {
                        info!("Received stop command");
                        stop_flag.store(true, Ordering::Relaxed);
                    }
                }
            }
            Err(mpsc::RecvTimeoutError::Disconnected) => break,
        }
    }

    while let Ok(report) = report_rx.try_recv() {
        send_message(&mut stream, &LatencyTestControlMessage::FrameReport(report)).ok();
        total_frames_reported += 1;
    }

    send_message(&mut stream, &LatencyTestControlMessage::TestComplete).ok();

    info!(
        "TCP report thread done: {} frames reported",
        total_frames_reported
    );
}

fn run_latency_test(
    stream: &mut TcpStream,
    config: LatencyTestConfig,
    running: Arc<AtomicBool>,
    server_ip: std::net::IpAddr,
) -> Result<()> {
    let frame_interval = Duration::from_secs_f64(1.0 / config.frame_rate_hz as f64);
    let expected_shards_count = calculate_expected_shards_count(config.frame_size_kb);

    info!(
        "Running latency test via StreamSocket: frame_rate={}Hz, frame_size={}KB ({} shards), duration={}s",
        config.frame_rate_hz, config.frame_size_kb, expected_shards_count, config.duration_secs
    );

    let builder = StreamSocketBuilder::listen_for_server(
        DATA_RECV_TIMEOUT,
        LATENCY_TEST_DATA_PORT,
        SocketProtocol::Udp,
        None,
        SocketBufferSize::Maximum,
        SocketBufferSize::Maximum,
    )?;
    let mut stream_socket = builder
        .accept_from_server(
            server_ip,
            LATENCY_TEST_DATA_PORT,
            LATENCY_TEST_MAX_PACKET_SIZE,
            CONNECT_TIMEOUT,
        )
        .map_err(|e| alvr_common::anyhow::anyhow!("{e}"))?;

    let mut sensor_sender: StreamSender<LatencyTestSensorPacket> =
        stream_socket.request_stream(LATENCY_TEST_SENSOR_STREAM_ID);
    let frame_receiver: StreamReceiver<LatencyTestFrameHeader> =
        stream_socket.subscribe_to_stream(LATENCY_TEST_FRAME_STREAM_ID, MAX_UNREAD_FRAMES);

    let stop_flag = Arc::new(AtomicBool::new(false));
    let recv_running = Arc::new(AtomicBool::new(true));
    let sent_frame_count = Arc::new(AtomicU64::new(0));

    let recv_thread = {
        let recv_running = Arc::clone(&recv_running);
        let stop_flag = Arc::clone(&stop_flag);
        thread::spawn(move || {
            data_receive_loop(stream_socket, recv_running, stop_flag);
        })
    };

    let (report_tx, report_rx) = mpsc::channel::<LatencyTestFrameReport>();
    let start_time = Instant::now();

    let process_thread = {
        let sent_frame_count = Arc::clone(&sent_frame_count);
        thread::spawn(move || {
            frame_process_loop(
                frame_receiver,
                report_tx,
                start_time,
                expected_shards_count,
                sent_frame_count,
            );
        })
    };

    let report_thread = {
        let stop_flag = Arc::clone(&stop_flag);
        let report_stream = stream.try_clone()?;
        thread::spawn(move || {
            tcp_report_loop(report_stream, report_rx, stop_flag);
        })
    };

    let total_frames = config.frame_rate_hz as u64 * config.duration_secs as u64;
    let mut current_frame_index: u64 = 0;
    let mut next_frame_time = Instant::now();

    while running.load(Ordering::Relaxed)
        && current_frame_index < total_frames
        && !stop_flag.load(Ordering::Relaxed)
    {
        if Instant::now() >= next_frame_time {
            let ts = start_time.elapsed().as_nanos() as u64;
            sensor_sender.send_header(&LatencyTestSensorPacket {
                frame_index: current_frame_index,
                client_send_timestamp_ns: ts,
            })?;
            sent_frame_count.store(current_frame_index + 1, Ordering::Relaxed);

            current_frame_index += 1;
            next_frame_time += frame_interval;
        }

        thread::sleep(Duration::from_millis(1));
    }

    thread::sleep(DRAIN_GRACE_PERIOD);
    recv_running.store(false, Ordering::Relaxed);
    recv_thread.join().ok();
    process_thread.join().ok();
    report_thread.join().ok();

    info!("Latency test completed");

    Ok(())
}
