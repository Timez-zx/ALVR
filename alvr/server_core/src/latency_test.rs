use crate::FILESYSTEM_LAYOUT;
use alvr_common::{anyhow::Result, error, info, once_cell::sync::Lazy, parking_lot::Mutex, warn};
use alvr_packets::{
    LatencyTestConfig, LatencyTestControlMessage, LatencyTestFrameHeader, LatencyTestSensorPacket,
    LATENCY_TEST_DATA_PORT, LATENCY_TEST_MAX_PACKET_SIZE, LATENCY_TEST_MAX_SHARD_DATA_SIZE,
    LATENCY_TEST_PORT, LATENCY_TEST_SHARD_PREFIX_SIZE, LATENCY_TEST_STREAM_ID,
};
use socket2::{Domain, Socket, Type};
use std::{
    fs::File,
    io::{BufWriter, Read, Write},
    net::{IpAddr, SocketAddr, TcpStream, UdpSocket},
    sync::{
        atomic::{AtomicBool, Ordering},
        Arc,
    },
    thread,
    time::{Duration, Instant},
};

const CONNECT_TIMEOUT: Duration = Duration::from_secs(5);
const READ_TIMEOUT: Duration = Duration::from_secs(1);
const UDP_RECV_TIMEOUT: Duration = Duration::from_millis(10);

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

        // Connect to client (don't bind local port - same as ALVR's tcp::connect_to_client)
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
/// then read in blocking mode.  Avoids the partial-read corruption that
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
    let log_dir = &FILESYSTEM_LAYOUT.get().unwrap().log_dir;
    let timestamp = chrono::Local::now().format("%Y%m%d_%H%M%S");
    let filename = format!(
        "latency_test_{}Hz_{}KB_{}.csv",
        config.frame_rate_hz, config.frame_size_kb, timestamp
    );
    let path = log_dir.join(&filename);

    let file = File::create(&path)?;
    let mut writer = BufWriter::new(file);

    // Write CSV header
    writeln!(writer, "frame_index,shards_sent,shards_received,rtt_us")?;
    writer.flush()?;

    info!("Created latency test CSV file: {}", path.display());
    Ok(writer)
}

/// Build shard with ALVR-compatible binary format
/// Shard prefix (18 bytes, big-endian):
/// - packet_length (4B): total shard length - 4
/// - stream_id (2B)
/// - packet_index (4B): frame index
/// - shards_count (4B)
/// - shard_index (4B)
fn build_shard(
    buffer: &mut Vec<u8>,
    stream_id: u16,
    frame_index: u64,
    shard_index: u32,
    shards_count: u32,
    header: Option<&LatencyTestFrameHeader>,
    data_size: usize,
) {
    // Calculate header size if present
    let header_bytes = header.map(|h| bincode::serialize(h).unwrap_or_default());
    let header_size = header_bytes.as_ref().map(|b| b.len()).unwrap_or(0);

    // Total shard size = prefix + header + data
    let shard_size = LATENCY_TEST_SHARD_PREFIX_SIZE + header_size + data_size;

    buffer.clear();
    buffer.resize(shard_size, 0);

    // Write prefix (18 bytes)
    let packet_length = (shard_size - 4) as u32;
    buffer[0..4].copy_from_slice(&packet_length.to_be_bytes());
    buffer[4..6].copy_from_slice(&stream_id.to_be_bytes());
    buffer[6..10].copy_from_slice(&(frame_index as u32).to_be_bytes());
    buffer[10..14].copy_from_slice(&shards_count.to_be_bytes());
    buffer[14..18].copy_from_slice(&shard_index.to_be_bytes());

    // Write header if present (first shard only)
    if let Some(hdr) = header_bytes {
        buffer[LATENCY_TEST_SHARD_PREFIX_SIZE..LATENCY_TEST_SHARD_PREFIX_SIZE + hdr.len()]
            .copy_from_slice(&hdr);
    }

    // Data portion is already zeroed
}

fn run_latency_test(
    mut stream: TcpStream,
    config: LatencyTestConfig,
    running: Arc<AtomicBool>,
) -> Result<()> {
    // Send start command to client
    send_message(
        &mut stream,
        &LatencyTestControlMessage::StartTest(config.clone()),
    )?;

    // Wait for ack
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

    // Create CSV file for logging
    let mut csv_writer = match create_csv_file(&config) {
        Ok(w) => Some(w),
        Err(e) => {
            error!("Failed to create CSV file: {}", e);
            None
        }
    };

    // Setup UDP socket for data plane
    let client_ip: IpAddr = config.client_ip.parse()?;
    let udp_socket = Socket::new(Domain::IPV4, Type::DGRAM, None)?;
    udp_socket.set_reuse_address(true)?;
    let udp_local_addr: SocketAddr = format!("0.0.0.0:{}", LATENCY_TEST_DATA_PORT).parse()?;
    udp_socket.bind(&udp_local_addr.into())?;
    udp_socket.set_read_timeout(Some(UDP_RECV_TIMEOUT))?;
    let udp_socket: UdpSocket = udp_socket.into();

    let mut udp_remote_addr = SocketAddr::new(client_ip, LATENCY_TEST_DATA_PORT);
    info!(
        "UDP data plane ready: local={}, remote={}",
        udp_local_addr, udp_remote_addr
    );

    let frame_size_bytes = config.frame_size_kb as usize * 1024;
    let test_duration = Duration::from_secs(config.duration_secs as u64);

    // Calculate shards count (matching ALVR's logic)
    let header_size = bincode::serialized_size(&LatencyTestFrameHeader {
        client_send_timestamp_ns: 0,
        server_recv_timestamp_ns: 0,
        server_send_timestamp_ns: 0,
    })? as usize;

    // First shard has header, subsequent shards are pure data
    let first_shard_data = LATENCY_TEST_MAX_SHARD_DATA_SIZE - header_size;
    let shards_count = if frame_size_bytes <= first_shard_data {
        1
    } else {
        let remaining = frame_size_bytes - first_shard_data;
        1 + ((remaining as f64) / (LATENCY_TEST_MAX_SHARD_DATA_SIZE as f64)).ceil() as u32
    };

    info!(
        "Starting latency test: frame_size={}KB ({} shards), rate={}Hz, duration={}s, max_packet={}",
        config.frame_size_kb, shards_count, config.frame_rate_hz, config.duration_secs,
        LATENCY_TEST_MAX_PACKET_SIZE
    );

    let start_time = Instant::now();
    let mut recv_buf = vec![0u8; 65535];
    let mut shard_buf = Vec::with_capacity(LATENCY_TEST_MAX_PACKET_SIZE);
    let mut warmup_buf = Vec::with_capacity(LATENCY_TEST_MAX_PACKET_SIZE);
    let mut last_warmup_send = Instant::now() - Duration::from_secs(1);
    let warmup_interval = Duration::from_millis(200);
    let mut last_tcp_check = Instant::now();
    let tcp_check_interval = Duration::from_millis(50);

    // Main test loop - receive sensor packets, send frame shards
    while running.load(Ordering::Relaxed) && start_time.elapsed() < test_duration {
        if last_warmup_send.elapsed() >= warmup_interval {
            // Warm up the reverse path so client can learn the real source address
            // instead of inferring from TCP peer address under NAT/hairpin.
            build_shard(&mut warmup_buf, 0, 0, 0, 1, None, 0);
            if let Err(e) = udp_socket.send_to(&warmup_buf, udp_remote_addr) {
                warn!("Failed to send UDP warmup packet: {}", e);
            }
            last_warmup_send = Instant::now();
        }

        // Try to receive sensor packet from client
        match udp_socket.recv_from(&mut recv_buf) {
            Ok((size, source_addr)) => {
                if source_addr != udp_remote_addr {
                    info!(
                        "Latency test UDP peer updated: {} -> {}",
                        udp_remote_addr, source_addr
                    );
                    udp_remote_addr = source_addr;
                }

                let server_recv_time = start_time.elapsed().as_nanos() as u64;
                if let Ok(sensor_packet) =
                    bincode::deserialize::<LatencyTestSensorPacket>(&recv_buf[..size])
                {
                    let server_send_time = start_time.elapsed().as_nanos() as u64;

                    // Send frame as multiple shards
                    let mut remaining_data = frame_size_bytes;

                    for shard_idx in 0..shards_count {
                        let (header, data_size) = if shard_idx == 0 {
                            // First shard includes header
                            let hdr = LatencyTestFrameHeader {
                                client_send_timestamp_ns: sensor_packet.client_send_timestamp_ns,
                                server_recv_timestamp_ns: server_recv_time,
                                server_send_timestamp_ns: server_send_time,
                            };
                            let ds = remaining_data.min(first_shard_data);
                            remaining_data = remaining_data.saturating_sub(ds);
                            (Some(hdr), ds)
                        } else {
                            let ds = remaining_data.min(LATENCY_TEST_MAX_SHARD_DATA_SIZE);
                            remaining_data = remaining_data.saturating_sub(ds);
                            (None, ds)
                        };

                        build_shard(
                            &mut shard_buf,
                            LATENCY_TEST_STREAM_ID,
                            sensor_packet.frame_index,
                            shard_idx,
                            shards_count,
                            header.as_ref(),
                            data_size,
                        );

                        if let Err(e) = udp_socket.send_to(&shard_buf, udp_remote_addr) {
                            warn!(
                                "Failed to send shard {}/{}: {}",
                                shard_idx, shards_count, e
                            );
                        }
                    }
                }
            }
            Err(ref e) if e.kind() == std::io::ErrorKind::WouldBlock => {
                // No data available, continue
            }
            Err(e) => {
                warn!("UDP recv error: {}", e);
            }
        }

        // Batch-drain frame reports from client via TCP (rate-limited)
        if last_tcp_check.elapsed() >= tcp_check_interval {
            let mut should_break = false;
            while let Some(msg) = try_recv_message(&mut stream) {
                match msg {
                    LatencyTestControlMessage::FrameReport(report) => {
                        if let Some(ref mut writer) = csv_writer {
                            if let Err(e) = writeln!(
                                writer,
                                "{},{},{},{}",
                                report.frame_index,
                                report.shards_sent,
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

    // Flush and close CSV file
    if let Some(ref mut writer) = csv_writer {
        writer.flush().ok();
    }

    // Send stop command
    send_message(&mut stream, &LatencyTestControlMessage::StopTest)?;

    info!("Latency test completed");

    running.store(false, Ordering::Relaxed);
    Ok(())
}
