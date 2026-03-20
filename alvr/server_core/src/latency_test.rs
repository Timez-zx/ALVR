use alvr_common::{
    anyhow::Result, error, info, once_cell::sync::Lazy, parking_lot::Mutex, warn,
    ConnectionError,
};
use alvr_packets::{
    LatencyTestConfig, LatencyTestControlMessage, LatencyTestFrameHeader, LatencyTestSensorPacket,
    LATENCY_TEST_DATA_PORT, LATENCY_TEST_FRAME_STREAM_ID, LATENCY_TEST_MAX_PACKET_SIZE,
    LATENCY_TEST_MAX_SHARD_DATA_SIZE, LATENCY_TEST_PORT, LATENCY_TEST_SENSOR_STREAM_ID,
};
use alvr_session::{SocketBufferSize, SocketProtocol};
use alvr_sockets::{StreamReceiver, StreamSender, StreamSocket, StreamSocketBuilder};
use std::{
    collections::HashMap,
    fs::File,
    io::{BufWriter, Read, Write},
    net::{IpAddr, SocketAddr, TcpStream},
    sync::{
        atomic::{AtomicBool, Ordering},
        Arc,
    },
    thread,
    time::{Duration, Instant},
};

const CONNECT_TIMEOUT: Duration = Duration::from_secs(5);
const READ_TIMEOUT: Duration = Duration::from_secs(1);
const DATA_RECV_TIMEOUT: Duration = Duration::from_millis(10);
const MAX_UNREAD_FRAMES: usize = 256;

/// Global latency test server instance
pub static LATENCY_TEST_SERVER: Lazy<Mutex<LatencyTestServer>> =
    Lazy::new(|| Mutex::new(LatencyTestServer::new()));

pub struct LatencyTestServer {
    running: Arc<AtomicBool>,
    test_thread: Option<thread::JoinHandle<()>>,
}

impl LatencyTestServer {
    pub fn new() -> Self {
        Self {
            running: Arc::new(AtomicBool::new(false)),
            test_thread: None,
        }
    }

    pub fn start(&mut self, config: LatencyTestConfig) -> Result<()> {
        if self.running.load(Ordering::Relaxed) {
            warn!("Latency test already running, stopping first");
            self.stop();
        }

        let client_ip: IpAddr = config.client_ip.parse()?;
        let remote_addr = SocketAddr::new(client_ip, LATENCY_TEST_PORT);

        info!("Connecting to {} for latency test", remote_addr);

        let stream = TcpStream::connect_timeout(&remote_addr, CONNECT_TIMEOUT)?;
        stream.set_read_timeout(Some(READ_TIMEOUT))?;
        stream.set_nodelay(true)?;

        info!("Connected to client for latency test");

        self.running.store(true, Ordering::Relaxed);
        let running = Arc::clone(&self.running);

        self.test_thread = Some(thread::spawn(move || {
            if let Err(e) = run_latency_test(stream, config, running) {
                error!("Latency test error: {}", e);
            }
        }));

        Ok(())
    }

    pub fn stop(&mut self) {
        self.running.store(false, Ordering::Relaxed);
        if let Some(thread) = self.test_thread.take() {
            thread.join().ok();
        }
        info!("Latency test stopped");
    }

    pub fn is_running(&self) -> bool {
        self.running.load(Ordering::Relaxed)
    }
}

impl Drop for LatencyTestServer {
    fn drop(&mut self) {
        self.stop();
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
/// then read in blocking mode. Avoids the partial-read corruption that
/// `read_exact` on a nonblocking stream can cause.
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

fn create_csv_file(config: &LatencyTestConfig) -> Result<BufWriter<File>> {
    let project_root = std::path::Path::new(env!("CARGO_MANIFEST_DIR"))
        .parent()
        .unwrap()
        .parent()
        .unwrap();
    let results_dir = project_root.join("results");
    std::fs::create_dir_all(&results_dir)?;

    let timestamp = chrono::Local::now().format("%Y%m%d_%H%M%S");
    let filename = format!(
        "latency_test_{}Hz_{}KB_{}.csv",
        config.frame_rate_hz, config.frame_size_kb, timestamp
    );
    let path = results_dir.join(&filename);

    let file = File::create(&path)?;
    let mut writer = BufWriter::new(file);

    writeln!(writer, "frame_index,shards_sent,shards_received,rtt_us")?;
    writer.flush()?;

    info!("Created latency test CSV file: {}", path.display());
    Ok(writer)
}

fn calculate_expected_shards_count(frame_size_kb: u32) -> Result<u32> {
    let frame_size_bytes = frame_size_kb as usize * 1024;
    let header_size = bincode::serialized_size(&LatencyTestFrameHeader {
        frame_index: 0,
        client_send_timestamp_ns: 0,
        server_recv_timestamp_ns: 0,
        server_send_timestamp_ns: 0,
    })? as usize;

    let first_shard_data = LATENCY_TEST_MAX_SHARD_DATA_SIZE - header_size;
    Ok(if frame_size_bytes <= first_shard_data {
        1
    } else {
        let remaining = frame_size_bytes - first_shard_data;
        1 + ((remaining as f64) / (LATENCY_TEST_MAX_SHARD_DATA_SIZE as f64)).ceil() as u32
    })
}

fn data_receive_loop(
    mut stream_socket: StreamSocket,
    recv_running: Arc<AtomicBool>,
    running: Arc<AtomicBool>,
) {
    while recv_running.load(Ordering::Relaxed) {
        match stream_socket.recv() {
            Ok(()) => (),
            Err(ConnectionError::TryAgain(_)) => continue,
            Err(e) => {
                warn!("Latency test data receive error: {}", e);
                running.store(false, Ordering::Relaxed);
                break;
            }
        }
    }
}

fn run_latency_test(
    mut stream: TcpStream,
    config: LatencyTestConfig,
    running: Arc<AtomicBool>,
) -> Result<()> {
    send_message(
        &mut stream,
        &LatencyTestControlMessage::StartTest(config.clone()),
    )?;

    match recv_message(&mut stream) {
        Ok(LatencyTestControlMessage::Ack) => {
            info!("Client acknowledged latency test start");
        }
        Ok(other) => {
            warn!("Unexpected message from client: {:?}", other);
        }
        Err(e) => {
            error!("Failed to receive ack from client: {}", e);
            return Err(e);
        }
    }

    let client_ip: IpAddr = config.client_ip.parse()?;
    let mut stream_socket = StreamSocketBuilder::connect_to_client(
        DATA_RECV_TIMEOUT,
        client_ip,
        LATENCY_TEST_DATA_PORT,
        SocketProtocol::Udp,
        None,
        SocketBufferSize::Maximum,
        SocketBufferSize::Maximum,
        LATENCY_TEST_MAX_PACKET_SIZE,
    )
    .map_err(|e| alvr_common::anyhow::anyhow!("{e}"))?;

    // Notify client that the server UDP socket is bound and ready. The client
    // must not send any sensor packets before receiving this signal, otherwise
    // the first packets may arrive before the socket is open and the resulting
    // ICMP Port Unreachable tears down the client's connected UDP socket.
    send_message(&mut stream, &LatencyTestControlMessage::DataReady)?;
    info!("Sent DataReady to client");

    let mut csv_writer = match create_csv_file(&config) {
        Ok(w) => Some(w),
        Err(e) => {
            error!("Failed to create CSV file: {}", e);
            None
        }
    };

    let mut frame_sender: StreamSender<LatencyTestFrameHeader> =
        stream_socket.request_stream(LATENCY_TEST_FRAME_STREAM_ID);
    let mut sensor_receiver: StreamReceiver<LatencyTestSensorPacket> =
        stream_socket.subscribe_to_stream(LATENCY_TEST_SENSOR_STREAM_ID, MAX_UNREAD_FRAMES);

    let recv_running = Arc::new(AtomicBool::new(true));
    let recv_thread = {
        let recv_running = Arc::clone(&recv_running);
        let running = Arc::clone(&running);
        thread::spawn(move || {
            data_receive_loop(stream_socket, recv_running, running);
        })
    };

    let frame_size_bytes = config.frame_size_kb as usize * 1024;
    let test_duration =
        Duration::from_secs(config.duration_secs as u64) + Duration::from_millis(500);
    let expected_shards_count = calculate_expected_shards_count(config.frame_size_kb)?;

    info!(
        "Starting latency test via StreamSocket: frame_size={}KB ({} shards), rate={}Hz, duration={}s, max_packet={}",
        config.frame_size_kb,
        expected_shards_count,
        config.frame_rate_hz,
        config.duration_secs,
        LATENCY_TEST_MAX_PACKET_SIZE
    );

    let mut frames_sent: HashMap<u64, u32> = HashMap::new();

    let start_time = Instant::now();
    let mut last_tcp_check = Instant::now();
    let tcp_check_interval = Duration::from_millis(50);

    while running.load(Ordering::Relaxed) && start_time.elapsed() < test_duration {
        match sensor_receiver.recv(DATA_RECV_TIMEOUT) {
            Ok(packet) => {
                let sensor_packet = match packet.get_header() {
                    Ok(packet) => packet,
                    Err(e) => {
                        warn!("Failed to decode latency test sensor packet: {}", e);
                        continue;
                    }
                };

                let server_recv_time = start_time.elapsed().as_nanos() as u64;
                let server_send_time = start_time.elapsed().as_nanos() as u64;
                let header = LatencyTestFrameHeader {
                    frame_index: sensor_packet.frame_index,
                    client_send_timestamp_ns: sensor_packet.client_send_timestamp_ns,
                    server_recv_timestamp_ns: server_recv_time,
                    server_send_timestamp_ns: server_send_time,
                };

                let mut buffer = match frame_sender.get_buffer(&header) {
                    Ok(buffer) => buffer,
                    Err(e) => {
                        warn!("Failed to allocate latency test frame buffer: {}", e);
                        break;
                    }
                };
                buffer.set_len(frame_size_bytes);

                if let Err(e) = frame_sender.send(buffer) {
                    warn!("Failed to send latency test frame: {}", e);
                    break;
                }

                frames_sent.insert(sensor_packet.frame_index, expected_shards_count);
            }
            Err(ConnectionError::TryAgain(_)) => {}
            Err(e) => {
                warn!("Latency test sensor receive error: {}", e);
                break;
            }
        }

        if last_tcp_check.elapsed() >= tcp_check_interval {
            let mut should_break = false;
            while let Some(msg) = try_recv_message(&mut stream) {
                match msg {
                    LatencyTestControlMessage::FrameReport(report) => {
                        let server_shards_sent = frames_sent.remove(&report.frame_index).unwrap_or(0);
                        if let Some(ref mut writer) = csv_writer {
                            if let Err(e) = writeln!(
                                writer,
                                "{},{},{},{}",
                                report.frame_index,
                                server_shards_sent,
                                report.shards_received,
                                report.rtt_us
                            ) {
                                error!("Failed to write to CSV: {}", e);
                            }
                        }
                    }
                    LatencyTestControlMessage::TestComplete => {
                        info!("Received test complete from client");
                        should_break = true;
                        break;
                    }
                    _ => {}
                }
            }
            last_tcp_check = Instant::now();
            if should_break {
                break;
            }
        }
    }

    recv_running.store(false, Ordering::Relaxed);
    recv_thread.join().ok();

    send_message(&mut stream, &LatencyTestControlMessage::StopTest)?;

    let drain_deadline = Instant::now() + Duration::from_secs(3);
    stream.set_read_timeout(Some(Duration::from_millis(100))).ok();
    while Instant::now() < drain_deadline {
        match recv_message(&mut stream) {
            Ok(LatencyTestControlMessage::FrameReport(report)) => {
                let server_shards_sent = frames_sent.remove(&report.frame_index).unwrap_or(0);
                if let Some(ref mut writer) = csv_writer {
                    writeln!(
                        writer,
                        "{},{},{},{}",
                        report.frame_index,
                        server_shards_sent,
                        report.shards_received,
                        report.rtt_us
                    )
                    .ok();
                }
            }
            Ok(LatencyTestControlMessage::TestComplete) => {
                info!("Received TestComplete from client");
                break;
            }
            Ok(_) => {}
            Err(ref e) => {
                let is_timeout = e
                    .downcast_ref::<std::io::Error>()
                    .map_or(false, |io| {
                        io.kind() == std::io::ErrorKind::TimedOut
                            || io.kind() == std::io::ErrorKind::WouldBlock
                    });
                if !is_timeout {
                    break;
                }
            }
        }
    }

    if let Some(ref mut writer) = csv_writer {
        writer.flush().ok();
    }

    info!("Latency test completed");

    running.store(false, Ordering::Relaxed);
    Ok(())
}
