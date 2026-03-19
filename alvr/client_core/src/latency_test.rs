use alvr_common::{anyhow::Result, error, info, once_cell::sync::Lazy, parking_lot::Mutex};
use alvr_packets::{
    LatencyTestConfig, LatencyTestControlMessage, LatencyTestFrameHeader, LatencyTestFrameReport,
    LatencyTestSensorPacket, LATENCY_TEST_DATA_PORT, LATENCY_TEST_MAX_SHARD_DATA_SIZE,
    LATENCY_TEST_PORT, LATENCY_TEST_SHARD_PREFIX_SIZE, LATENCY_TEST_STREAM_ID,
};
use socket2::{Domain, Socket, Type};
use std::{
    collections::{HashMap, HashSet},
    io::{Read, Write},
    net::{SocketAddr, TcpListener, TcpStream, UdpSocket},
    sync::{
        atomic::{AtomicBool, Ordering},
        mpsc, Arc,
    },
    thread,
    time::{Duration, Instant},
};

const READ_TIMEOUT: Duration = Duration::from_secs(1);
const UDP_RECV_TIMEOUT: Duration = Duration::from_millis(1);

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

/// Parse shard prefix (18 bytes, big-endian)
/// Returns: (stream_id, packet_index, shards_count, shard_index)
fn parse_shard_prefix(data: &[u8]) -> Option<(u16, u32, u32, u32)> {
    if data.len() < LATENCY_TEST_SHARD_PREFIX_SIZE {
        return None;
    }

    let stream_id = u16::from_be_bytes([data[4], data[5]]);
    let packet_index = u32::from_be_bytes([data[6], data[7], data[8], data[9]]);
    let shards_count = u32::from_be_bytes([data[10], data[11], data[12], data[13]]);
    let shard_index = u32::from_be_bytes([data[14], data[15], data[16], data[17]]);

    Some((stream_id, packet_index, shards_count, shard_index))
}

/// Calculate expected shards count for a given frame size (same formula as server)
fn calculate_expected_shards_count(frame_size_kb: u32) -> u32 {
    let frame_size_bytes = frame_size_kb as usize * 1024;

    // Calculate header size (same as server)
    let header_size = bincode::serialized_size(&LatencyTestFrameHeader {
        client_send_timestamp_ns: 0,
        server_recv_timestamp_ns: 0,
        server_send_timestamp_ns: 0,
    })
    .unwrap_or(24) as usize;

    // First shard has header, subsequent shards are pure data
    let first_shard_data = LATENCY_TEST_MAX_SHARD_DATA_SIZE - header_size;

    if frame_size_bytes <= first_shard_data {
        1
    } else {
        let remaining = frame_size_bytes - first_shard_data;
        1 + ((remaining as f64) / (LATENCY_TEST_MAX_SHARD_DATA_SIZE as f64)).ceil() as u32
    }
}

struct InProgressFrame {
    shards_count: u32,
    received_shards: HashSet<u32>,
    client_send_timestamp_ns: u64,
}

/// Events routed through a single channel from both the recv thread and the
/// main (send) thread into the process thread.  Keeping one channel avoids
/// the awkward `try_recv` poll that the old design required inside the I/O loop.
enum UdpEvent {
    /// Parsed shard metadata; the payload bytes are not needed for latency tracking.
    Shard {
        packet_index: u32,
        shards_count: u32,
        shard_index: u32,
        recv_time_ns: u64,
    },
    /// The main thread just sent frame `frame_index`; register it so the
    /// process thread is ready before the first shard can arrive.
    FrameReg {
        frame_index: u32,
        client_send_ts: u64,
    },
}

/// Pure I/O thread — only calls `recv_from` and forwards raw bytes.
/// No business logic lives here.
fn udp_recv_thread(
    socket: UdpSocket,
    start_time: Instant,
    event_tx: mpsc::Sender<UdpEvent>,
    recv_running: Arc<AtomicBool>,
) {
    // Only the prefix bytes matter; the payload is not used for latency tracking.
    let mut recv_buf = vec![0u8; LATENCY_TEST_SHARD_PREFIX_SIZE];

    while recv_running.load(Ordering::Relaxed) {
        match socket.recv(&mut recv_buf) {
            Ok(_) => {
                let recv_time_ns = start_time.elapsed().as_nanos() as u64;
                let Some((stream_id, packet_index, shards_count, shard_index)) =
                    parse_shard_prefix(&recv_buf)
                else {
                    continue;
                };
                if stream_id != LATENCY_TEST_STREAM_ID {
                    continue;
                }
                event_tx
                    .send(UdpEvent::Shard {
                        packet_index,
                        shards_count,
                        shard_index,
                        recv_time_ns,
                    })
                    .ok();
            }
            Err(ref e) if e.kind() == std::io::ErrorKind::WouldBlock => {}
            Err(_) => {}
        }
    }
}

/// Processing thread — receives `UdpEvent`s via a blocking channel, handles
/// shard assembly and frame tracking, and forwards completed reports.
/// Shuts down naturally when both senders (recv thread + main thread) are dropped.
fn process_loop(
    event_rx: mpsc::Receiver<UdpEvent>,
    report_tx: mpsc::Sender<LatencyTestFrameReport>,
) {
    let mut in_progress_frames: HashMap<u32, InProgressFrame> = HashMap::new();

    // Blocking recv drives the loop; exits when the channel is closed.
    while let Ok(event) = event_rx.recv() {
        match event {
            UdpEvent::FrameReg {
                frame_index,
                client_send_ts,
            } => {
                in_progress_frames.insert(
                    frame_index,
                    InProgressFrame {
                        shards_count: 0,
                        received_shards: HashSet::new(),
                        client_send_timestamp_ns: client_send_ts,
                    },
                );
            }

            UdpEvent::Shard {
                packet_index,
                shards_count,
                shard_index,
                recv_time_ns,
            } => {
                let (packet_idx, shard_idx) = (packet_index, shard_index);
                if let Some(frame) = in_progress_frames.get_mut(&packet_idx) {
                    frame.shards_count = shards_count;
                    frame.received_shards.insert(shard_idx);

                    if frame.received_shards.len() as u32 == shards_count {
                        let rtt_us =
                            recv_time_ns.saturating_sub(frame.client_send_timestamp_ns) / 1000;
                        report_tx
                            .send(LatencyTestFrameReport {
                                frame_index: packet_idx as u64,
                                shards_sent: shards_count,
                                shards_received: shards_count,
                                rtt_us,
                            })
                            .ok();
                        in_progress_frames.remove(&packet_idx);

                        // Evict older incomplete frames now that we know
                        // shards_count from the completed frame.
                        let older: Vec<u32> = in_progress_frames
                            .keys()
                            .filter(|&&idx| idx < packet_idx)
                            .copied()
                            .collect();
                        for old_idx in older {
                            if let Some(old) = in_progress_frames.remove(&old_idx) {
                                report_tx
                                    .send(LatencyTestFrameReport {
                                        frame_index: old_idx as u64,
                                        shards_sent: old.shards_count.max(shards_count),
                                        shards_received: old.received_shards.len() as u32,
                                        rtt_us: 0,
                                    })
                                    .ok();
                            }
                        }
                    }
                }
            }
        }
    }

    // Flush frames still in flight as incomplete.
    for (idx, frame) in in_progress_frames.drain() {
        report_tx
            .send(LatencyTestFrameReport {
                frame_index: idx as u64,
                shards_sent: frame.shards_count,
                shards_received: frame.received_shards.len() as u32,
                rtt_us: 0,
            })
            .ok();
    }
}

/// TCP thread — drains completed reports and, every 200 ms, checks whether the
/// server has sent a StopTest command.  Exits when `report_rx` is closed (i.e.
/// the process thread has finished flushing).
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
                // Batch-drain any additional reports that arrived in the meantime.
                while let Ok(report) = report_rx.try_recv() {
                    send_message(&mut stream, &LatencyTestControlMessage::FrameReport(report))
                        .ok();
                    total_frames_reported += 1;
                }
            }
            Err(mpsc::RecvTimeoutError::Timeout) => {
                // Opportunistically check for a stop command from the server.
                stream.set_nonblocking(true).ok();
                if let Ok(msg) = recv_message(&mut stream) {
                    if matches!(msg, LatencyTestControlMessage::StopTest) {
                        info!("Received stop command");
                        stop_flag.store(true, Ordering::Relaxed);
                    }
                }
                stream.set_nonblocking(false).ok();
            }
            Err(mpsc::RecvTimeoutError::Disconnected) => break,
        }
    }

    // Drain any reports that arrived between process thread flush and disconnect.
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
        "Running latency test: frame_rate={}Hz, frame_size={}KB ({} shards), duration={}s",
        config.frame_rate_hz, config.frame_size_kb, expected_shards_count, config.duration_secs
    );

    // Setup UDP socket with large receive buffer
    let udp_socket = Socket::new(Domain::IPV4, Type::DGRAM, None)?;
    udp_socket.set_reuse_address(true)?;
    udp_socket.set_recv_buffer_size(2 * 1024 * 1024)?;
    let udp_local_addr: SocketAddr = format!("0.0.0.0:{}", LATENCY_TEST_DATA_PORT).parse()?;
    udp_socket.bind(&udp_local_addr.into())?;
    udp_socket.set_read_timeout(Some(UDP_RECV_TIMEOUT))?;
    let udp_socket: UdpSocket = udp_socket.into();

    let fallback_remote_addr = SocketAddr::new(server_ip, LATENCY_TEST_DATA_PORT);
    info!(
        "UDP data plane ready: local={}, fallback_remote={}",
        udp_local_addr, fallback_remote_addr
    );

    // ── UDP address discovery ──────────────────────────────────────────
    // Wait for a warmup packet from the server to learn the real source
    // address (which may differ from TCP peer_addr under NAT / gateway).
    let udp_remote_addr = {
        let discovery_timeout = Duration::from_secs(10);
        let discovery_start = Instant::now();
        let mut recv_buf = vec![0u8; 65535];
        let mut discovered: Option<SocketAddr> = None;

        info!("Waiting for server UDP warmup packets to discover real address...");
        while running.load(Ordering::Relaxed) && discovery_start.elapsed() < discovery_timeout {
            match udp_socket.recv_from(&mut recv_buf) {
                Ok((_size, source_addr)) => {
                    info!("Discovered server UDP address: {}", source_addr);
                    discovered = Some(source_addr);
                    break;
                }
                Err(ref e) if e.kind() == std::io::ErrorKind::WouldBlock => {
                    thread::sleep(Duration::from_millis(1));
                }
                Err(e) => {
                    error!("UDP discovery recv error: {}", e);
                }
            }
        }

        match discovered {
            Some(addr) => addr,
            None => {
                info!(
                    "No warmup received within {}s, falling back to TCP peer: {}",
                    discovery_timeout.as_secs(),
                    fallback_remote_addr
                );
                fallback_remote_addr
            }
        }
    };
    info!("Using server UDP address: {}", udp_remote_addr);

    // ── Spawn threads ──────────────────────────────────────────────────
    // event_tx is shared by the recv thread and the main (send) loop.
    // Dropping both ends closes the channel and shuts down process_loop.
    let (event_tx, event_rx) = mpsc::channel::<UdpEvent>();
    let (report_tx, report_rx) = mpsc::channel::<LatencyTestFrameReport>();

    let start_time = Instant::now();
    let recv_running = Arc::new(AtomicBool::new(true));
    let recv_running_clone = Arc::clone(&recv_running);
    let recv_socket = udp_socket.try_clone()?;
    let recv_event_tx = event_tx.clone();

    let recv_thread = thread::spawn(move || {
        udp_recv_thread(recv_socket, start_time, recv_event_tx, recv_running_clone);
    });

    let process_thread = thread::spawn(move || {
        process_loop(event_rx, report_tx);
    });

    let stop_flag = Arc::new(AtomicBool::new(false));
    let stop_flag_clone = Arc::clone(&stop_flag);
    let report_stream = stream.try_clone()?;

    let report_thread = thread::spawn(move || {
        tcp_report_loop(report_stream, report_rx, stop_flag_clone);
    });

    // ── Main loop: send UDP sensor packets ────────────────────────────
    let total_frames = config.frame_rate_hz as u64 * config.duration_secs as u64;
    let mut current_frame_index: u64 = 0;
    let mut next_frame_time = Instant::now();

    while running.load(Ordering::Relaxed)
        && current_frame_index < total_frames
        && !stop_flag.load(Ordering::Relaxed)
    {
        if Instant::now() >= next_frame_time {
            let ts = start_time.elapsed().as_nanos() as u64;

            // Register the frame with the process thread BEFORE sending UDP,
            // so it is ready to accept shards even if the server responds
            // before the event is drained.
            event_tx
                .send(UdpEvent::FrameReg {
                    frame_index: current_frame_index as u32,
                    client_send_ts: ts,
                })
                .ok();

            let sensor_packet = LatencyTestSensorPacket {
                frame_index: current_frame_index,
                client_send_timestamp_ns: ts,
            };
            let sensor_data = bincode::serialize(&sensor_packet)?;
            udp_socket.send_to(&sensor_data, udp_remote_addr).ok();

            current_frame_index += 1;
            next_frame_time += frame_interval;
        }

        thread::sleep(Duration::from_millis(1));
    }

    // ── Shutdown (channel-driven, no extra AtomicBools needed) ────────
    // 1. Wait for in-flight shards to arrive before stopping the recv thread.
    //    Send periodic keepalive packets to the server during this window so
    //    the Android WiFi chip does not enter Power Save Mode (PSM) and drop
    //    the incoming shard burst for the last frame(s).
    let grace_end = Instant::now() + Duration::from_millis(500);
    while Instant::now() < grace_end {
        // A minimal UDP send keeps the radio in active RX mode.  The server
        // will attempt to deserialize this as a sensor packet; the payload is
        // too short so it will be silently ignored.
        udp_socket.send_to(&[0u8; 1], udp_remote_addr).ok();
        thread::sleep(Duration::from_millis(20));
    }
    recv_running.store(false, Ordering::Relaxed);
    recv_thread.join().ok();

    // 2. Drop main's event_tx clone → channel is now empty and closed
    //    → process_loop exits its blocking recv() and flushes remaining frames.
    drop(event_tx);
    process_thread.join().ok();

    // 3. report_tx was owned by process_thread and is now dropped → report_rx
    //    disconnects → tcp_report_loop drains remaining reports and sends TestComplete.
    report_thread.join().ok();

    info!("Latency test completed");

    Ok(())
}
