use alvr_common::{anyhow::Result, error, info, once_cell::sync::Lazy, parking_lot::Mutex};
use alvr_packets::{LatencyTestConfig, LatencyTestControlMessage, LATENCY_TEST_PORT};
use std::{
    io::{Read, Write},
    net::{TcpListener, TcpStream},
    sync::{
        atomic::{AtomicBool, Ordering},
        Arc,
    },
    thread,
    time::Duration,
};

const READ_TIMEOUT: Duration = Duration::from_secs(1);

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

    /// Start listening for latency test connections from server
    pub fn start_listener(&mut self) -> Result<()> {
        info!("Latency test listener: start_listener() called");

        if self.running.load(Ordering::Relaxed) {
            info!("Latency test listener already running");
            return Ok(());
        }

        info!("Latency test listener: attempting to bind to port {}", LATENCY_TEST_PORT);
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

        info!("Latency test listener started on port {}", LATENCY_TEST_PORT);

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
                // No incoming connection, sleep a bit
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

    // Wait for start command
    let config = match recv_message(&mut stream) {
        Ok(LatencyTestControlMessage::StartTest(config)) => {
            info!("Received latency test start command: {:?}", config);
            // Send ack
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

    // Run the test
    run_latency_test(&mut stream, config, running)?;

    Ok(())
}

fn run_latency_test(
    stream: &mut TcpStream,
    config: LatencyTestConfig,
    running: Arc<AtomicBool>,
) -> Result<()> {
    let frame_interval = Duration::from_secs_f64(1.0 / config.frame_rate_hz as f64);
    let _frame_size_bytes = config.frame_size_kb as usize * 1024;

    info!(
        "Running latency test: frame_rate={}Hz, duration={}s",
        config.frame_rate_hz, config.duration_secs
    );

    let start_time = std::time::Instant::now();
    let test_duration = Duration::from_secs(config.duration_secs as u64);
    let mut frame_index: u64 = 0;

    // Main test loop
    while running.load(Ordering::Relaxed) && start_time.elapsed() < test_duration {
        let frame_start = std::time::Instant::now();

        // TODO: Send tracking data and receive emulated frames
        // For now, just simulate the timing
        frame_index += 1;

        // Check for stop command (non-blocking)
        stream.set_read_timeout(Some(Duration::from_millis(1)))?;
        if let Ok(msg) = recv_message(stream) {
            if matches!(msg, LatencyTestControlMessage::StopTest) {
                info!("Received stop command");
                break;
            }
        }
        stream.set_read_timeout(Some(READ_TIMEOUT))?;

        let elapsed = frame_start.elapsed();
        if elapsed < frame_interval {
            thread::sleep(frame_interval - elapsed);
        }
    }

    info!("Latency test completed: {} frames processed", frame_index);

    Ok(())
}
