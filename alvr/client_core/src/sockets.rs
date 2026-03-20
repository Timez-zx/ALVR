use alvr_common::{anyhow::Result, ALVR_NAME};
use alvr_sockets::{CONTROL_PORT, LOCAL_IP};
use std::net::{IpAddr, Ipv4Addr, UdpSocket};

pub struct AnnouncerSocket {
    // One socket per active non-loopback interface so broadcasts reach every
    // connected network (WiFi, ethernet-over-USB, etc.).
    sockets: Vec<UdpSocket>,
    packet: [u8; 56],
}

impl AnnouncerSocket {
    pub fn new(hostname: &str) -> Result<Self> {
        let mut packet = [0; 56];
        packet[0..ALVR_NAME.len()].copy_from_slice(ALVR_NAME.as_bytes());
        packet[16..24].copy_from_slice(&alvr_common::protocol_id_u64().to_le_bytes());
        packet[24..24 + hostname.len()].copy_from_slice(hostname.as_bytes());

        // Create a broadcast socket bound to each local IPv4 address.  Binding
        // to a specific address routes the outgoing UDP packet through the
        // corresponding interface, so the limited broadcast reaches every
        // directly-attached network segment.
        let mut sockets: Vec<UdpSocket> = alvr_system_info::local_ips()
            .into_iter()
            .filter_map(|ip| {
                let IpAddr::V4(ipv4) = ip else { return None };
                let socket = UdpSocket::bind((ipv4, 0)).ok()?;
                socket.set_broadcast(true).ok()?;
                Some(socket)
            })
            .collect();

        // Fallback: if we couldn't enumerate interfaces, use the default route.
        if sockets.is_empty() {
            let socket = UdpSocket::bind((LOCAL_IP, CONTROL_PORT))?;
            socket.set_broadcast(true)?;
            sockets.push(socket);
        }

        Ok(Self { sockets, packet })
    }

    pub fn announce_broadcast(&self) -> Result<()> {
        for socket in &self.sockets {
            // Ignore per-interface errors; at least one should succeed.
            let _ = socket.send_to(&self.packet, (Ipv4Addr::BROADCAST, CONTROL_PORT));
        }

        Ok(())
    }
}
