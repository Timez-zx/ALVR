use alvr_common::{anyhow::Result, info, warn, error, once_cell::sync::Lazy, parking_lot::Mutex};
use alvr_packets::{LatencyTestConfig, LatencyTestControlMessage, LATENCY_TEST_PORT};
use std::{
    io::{Read, Write},
    net::{TcpStream, SocketAddr, IpAddr},
    sync::{
        atomic::{AtomicBool, Ordering},
        Arc,
    },
    thread,
    time::Duration,
};

const CONNECT_TIMEOUT: Duration = Duration::from_secs(5);
const READ_TIMEOUT: Duration = Duration::from_secs(1);

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
        let addr = SocketAddr::new(client_ip, LATENCY_TEST_PORT);

        info!("Connecting to client at {} for latency test", addr);

        // Try to connect to client
        let stream = TcpStream::connect_timeout(&addr, CONNECT_TIMEOUT)?;
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

fn run_latency_test(
    mut stream: TcpStream,
    config: LatencyTestConfig,
    running: Arc<AtomicBool>,
) -> Result<()> {
    // Send start command to client
    send_message(&mut stream, &LatencyTestControlMessage::StartTest(config.clone()))?;

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

    let frame_interval = Duration::from_secs_f64(1.0 / config.frame_rate_hz as f64);
    let frame_size_bytes = config.frame_size_kb as usize * 1024;
    let test_duration = Duration::from_secs(config.duration_secs as u64);

    info!(
        "Starting latency test: frame_size={}KB, rate={}Hz, duration={}s",
        config.frame_size_kb, config.frame_rate_hz, config.duration_secs
    );

    let start_time = std::time::Instant::now();
    let mut frame_index: u64 = 0;

    // Main test loop
    while running.load(Ordering::Relaxed) && start_time.elapsed() < test_duration {
        let frame_start = std::time::Instant::now();

        // TODO: Send emulated frame data and receive statistics
        // For now, just simulate the timing
        frame_index += 1;

        let elapsed = frame_start.elapsed();
        if elapsed < frame_interval {
            thread::sleep(frame_interval - elapsed);
        }
    }

    // Send stop command
    send_message(&mut stream, &LatencyTestControlMessage::StopTest)?;

    info!("Latency test completed: {} frames sent", frame_index);

    running.store(false, Ordering::Relaxed);
    Ok(())
}
