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
        Arc,
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

/// Parse header from first shard's data section
fn parse_frame_header(data: &[u8]) -> Option<LatencyTestFrameHeader> {
    if data.len() < LATENCY_TEST_SHARD_PREFIX_SIZE {
        return None;
    }

    let header_data = &data[LATENCY_TEST_SHARD_PREFIX_SIZE..];
    bincode::deserialize(header_data).ok()
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

/// In-progress frame tracking
struct InProgressFrame {
    shards_count: u32,
    received_shards: HashSet<u32>,
    client_send_timestamp_ns: u64,
    rtt_us: u64,
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

    // Setup UDP socket for data plane
    let udp_socket = Socket::new(Domain::IPV4, Type::DGRAM, None)?;
    udp_socket.set_reuse_address(true)?;
    let udp_local_addr: SocketAddr = format!("0.0.0.0:{}", LATENCY_TEST_DATA_PORT).parse()?;
    udp_socket.bind(&udp_local_addr.into())?;
    udp_socket.set_read_timeout(Some(UDP_RECV_TIMEOUT))?;
    let udp_socket: UdpSocket = udp_socket.into();

    let fallback_remote_addr = SocketAddr::new(server_ip, LATENCY_TEST_DATA_PORT);
    info!(
        "UDP data plane ready: local={}, fallback_remote={}",
        udp_local_addr, fallback_remote_addr
    );

    let mut recv_buf = vec![0u8; 65535];

    // UDP address discovery: wait for a warmup packet from the server to learn the
    // real source address (which may differ from TCP peer_addr under NAT/gateway).
    let udp_remote_addr = {
        let discovery_timeout = Duration::from_secs(10);
        let discovery_start = Instant::now();
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

    let start_time = Instant::now();
    let test_start = Instant::now();
    let test_duration = Duration::from_secs(config.duration_secs as u64);

    let mut current_frame_index: u64 = 0;
    let mut next_frame_time = Instant::now();
    let mut in_progress_frames: HashMap<u32, InProgressFrame> = HashMap::new();
    let mut total_frames_reported: u64 = 0;
    let mut udp_remote_addr = udp_remote_addr;

    // Main test loop
    while running.load(Ordering::Relaxed) && test_start.elapsed() < test_duration {
        // Send sensor packet at frame rate
        if Instant::now() >= next_frame_time {
            let sensor_packet = LatencyTestSensorPacket {
                frame_index: current_frame_index,
                client_send_timestamp_ns: start_time.elapsed().as_nanos() as u64,
            };
            let sensor_data = bincode::serialize(&sensor_packet)?;

            if udp_socket.send_to(&sensor_data, udp_remote_addr).is_ok() {
                in_progress_frames.insert(
                    current_frame_index as u32,
                    InProgressFrame {
                        shards_count: expected_shards_count,
                        received_shards: HashSet::new(),
                        client_send_timestamp_ns: sensor_packet.client_send_timestamp_ns,
                        rtt_us: 0,
                    },
                );
            }

            current_frame_index += 1;
            next_frame_time += frame_interval;
        }

        // Receive shards
        match udp_socket.recv_from(&mut recv_buf) {
            Ok((size, source_addr)) => {
                // Update peer address without port filtering — NAT may remap the port
                if udp_remote_addr != source_addr {
                    info!("Latency test UDP peer updated: {}", source_addr);
                    udp_remote_addr = source_addr;
                }

                let recv_time = start_time.elapsed().as_nanos() as u64;

                if let Some((stream_id, packet_idx, shards_count, shard_idx)) =
                    parse_shard_prefix(&recv_buf[..size])
                {
                    if stream_id != LATENCY_TEST_STREAM_ID {
                        continue;
                    }

                    // Report and remove all older frames (they're considered lost)
                    let older_frames: Vec<u32> = in_progress_frames
                        .keys()
                        .filter(|&&idx| idx < packet_idx)
                        .copied()
                        .collect();

                    for old_idx in older_frames {
                        if let Some(old_frame) = in_progress_frames.remove(&old_idx) {
                            let report = LatencyTestFrameReport {
                                frame_index: old_idx as u64,
                                shards_sent: old_frame.shards_count,
                                shards_received: old_frame.received_shards.len() as u32,
                                rtt_us: old_frame.rtt_us,
                            };
                            send_message(stream, &LatencyTestControlMessage::FrameReport(report))?;
                            total_frames_reported += 1;
                        }
                    }

                    // Process current shard
                    if let Some(frame) = in_progress_frames.get_mut(&packet_idx) {
                        frame.shards_count = shards_count;
                        frame.received_shards.insert(shard_idx);

                        // First shard contains timestamp header
                        if shard_idx == 0 {
                            if let Some(header) = parse_frame_header(&recv_buf[..size]) {
                                frame.rtt_us = recv_time
                                    .saturating_sub(header.client_send_timestamp_ns)
                                    / 1000;
                            }
                        }

                        // Check if frame is complete
                        if frame.received_shards.len() as u32 == shards_count {
                            let report = LatencyTestFrameReport {
                                frame_index: packet_idx as u64,
                                shards_sent: shards_count,
                                shards_received: shards_count,
                                rtt_us: frame.rtt_us,
                            };
                            send_message(stream, &LatencyTestControlMessage::FrameReport(report))?;
                            in_progress_frames.remove(&packet_idx);
                            total_frames_reported += 1;
                        }
                    }
                }
            }
            Err(ref e) if e.kind() == std::io::ErrorKind::WouldBlock => {
                // No data, continue
            }
            Err(_) => {}
        }

        // Check for stop command (non-blocking)
        stream.set_nonblocking(true)?;
        if let Ok(msg) = recv_message(stream) {
            if matches!(msg, LatencyTestControlMessage::StopTest) {
                info!("Received stop command");
                break;
            }
        }
        stream.set_nonblocking(false)?;
    }

    // Report any remaining in-progress frames
    for (idx, frame) in in_progress_frames.drain() {
        let report = LatencyTestFrameReport {
            frame_index: idx as u64,
            shards_sent: frame.shards_count,
            shards_received: frame.received_shards.len() as u32,
            rtt_us: frame.rtt_us,
        };
        send_message(stream, &LatencyTestControlMessage::FrameReport(report))?;
        total_frames_reported += 1;
    }

    // Send test complete message
    send_message(stream, &LatencyTestControlMessage::TestComplete)?;

    info!(
        "Latency test completed: {} frames reported",
        total_frames_reported
    );

    Ok(())
}
