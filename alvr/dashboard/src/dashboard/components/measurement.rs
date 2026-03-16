use alvr_packets::{LatencyTestConfig, ServerRequest};
use alvr_session::SessionConfig;
use eframe::egui::{Grid, RichText, TextEdit, Ui};
use std::net::IpAddr;

// Stores hostname and its IP address
struct ClientInfo {
    hostname: String,
    ip: Option<IpAddr>,
}

pub struct MeasurementTab {
    available_clients: Vec<ClientInfo>,
    client_ip_input: String,
    frame_size_kb: u32,
    frame_rate_hz: u32,
    duration_secs: u32,
    test_running: bool,
}

impl MeasurementTab {
    pub fn new() -> Self {
        Self {
            available_clients: Vec::new(),
            client_ip_input: String::new(),
            frame_size_kb: 100,
            frame_rate_hz: 90,
            duration_secs: 30,
            test_running: false,
        }
    }

    pub fn update_client_list(&mut self, session: &SessionConfig) {
        self.available_clients = session
            .client_connections
            .iter()
            .filter(|(_, config)| config.trusted)
            .map(|(hostname, config)| ClientInfo {
                hostname: hostname.clone(),
                ip: config.current_ip,
            })
            .collect();

        // Auto-fill IP if we have a connected client and input is empty
        if self.client_ip_input.is_empty() {
            if let Some(client) = self.available_clients.iter().find(|c| c.ip.is_some()) {
                if let Some(ip) = client.ip {
                    self.client_ip_input = ip.to_string();
                }
            }
        }
    }

    pub fn ui(&mut self, ui: &mut Ui) -> Option<ServerRequest> {
        let mut request = None;

        ui.add_space(10.0);
        ui.heading(RichText::new("Latency Test Configuration").size(18.0));
        ui.add_space(10.0);

        Grid::new("measurement_config")
            .num_columns(2)
            .spacing([20.0, 10.0])
            .show(ui, |ui| {
                // Client IP input
                ui.label("Client IP:");
                ui.add(TextEdit::singleline(&mut self.client_ip_input).hint_text("e.g. 192.168.1.100"));
                ui.end_row();

                // Show available clients as hint
                if !self.available_clients.is_empty() {
                    ui.label("Available:");
                    let clients_str: String = self
                        .available_clients
                        .iter()
                        .filter_map(|c| c.ip.map(|ip| ip.to_string()))
                        .collect::<Vec<_>>()
                        .join(", ");
                    ui.label(if clients_str.is_empty() {
                        "No IP available".to_string()
                    } else {
                        clients_str
                    });
                    ui.end_row();
                }

                // Frame size
                ui.label("Frame Size (KB):");
                let mut frame_size = self.frame_size_kb as f32;
                if ui
                    .add(eframe::egui::Slider::new(&mut frame_size, 10.0..=500.0))
                    .changed()
                {
                    self.frame_size_kb = frame_size as u32;
                }
                ui.end_row();

                // Frame rate
                ui.label("Frame Rate (Hz):");
                let mut frame_rate = self.frame_rate_hz as f32;
                if ui
                    .add(eframe::egui::Slider::new(&mut frame_rate, 30.0..=120.0))
                    .changed()
                {
                    self.frame_rate_hz = frame_rate as u32;
                }
                ui.end_row();

                // Duration
                ui.label("Duration (seconds):");
                let mut duration = self.duration_secs as f32;
                if ui
                    .add(eframe::egui::Slider::new(&mut duration, 5.0..=120.0))
                    .changed()
                {
                    self.duration_secs = duration as u32;
                }
                ui.end_row();
            });

        ui.add_space(20.0);

        // Start/Stop buttons
        ui.horizontal(|ui| {
            // Validate IP address format
            let ip_valid = self.client_ip_input.parse::<IpAddr>().is_ok();
            let can_start = ip_valid && !self.test_running;

            if ui
                .add_enabled(can_start, eframe::egui::Button::new("Start Test"))
                .clicked()
            {
                self.test_running = true;
                request = Some(ServerRequest::StartLatencyTest(LatencyTestConfig {
                    client_ip: self.client_ip_input.clone(),
                    frame_size_kb: self.frame_size_kb,
                    frame_rate_hz: self.frame_rate_hz,
                    duration_secs: self.duration_secs,
                }));
            }

            if ui
                .add_enabled(self.test_running, eframe::egui::Button::new("Stop Test"))
                .clicked()
            {
                self.test_running = false;
                request = Some(ServerRequest::StopLatencyTest);
            }

            if !ip_valid && !self.client_ip_input.is_empty() {
                ui.label(RichText::new("Invalid IP format").color(eframe::egui::Color32::RED));
            }
        });

        // Status display
        ui.add_space(10.0);
        if self.test_running {
            ui.label(RichText::new("Test running...").color(eframe::egui::Color32::GREEN));
        }

        request
    }
}
