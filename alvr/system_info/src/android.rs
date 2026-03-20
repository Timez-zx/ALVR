use alvr_common::warn;
use jni::{objects::JObject, sys::jobject, JNIEnv, JavaVM};
use std::net::{IpAddr, Ipv4Addr};

pub const MICROPHONE_PERMISSION: &str = "android.permission.RECORD_AUDIO";

pub fn vm() -> JavaVM {
    unsafe { JavaVM::from_raw(ndk_context::android_context().vm().cast()).unwrap() }
}

pub fn context() -> jobject {
    ndk_context::android_context().context().cast()
}

fn get_api_level() -> i32 {
    let vm = vm();
    let mut env = vm.attach_current_thread().unwrap();

    env.get_static_field("android/os/Build$VERSION", "SDK_INT", "I")
        .unwrap()
        .i()
        .unwrap()
}

pub fn try_get_permission(permission: &str) {
    let vm = vm();
    let mut env = vm.attach_current_thread().unwrap();

    let mic_perm_jstring = env.new_string(permission).unwrap();

    let permission_status = env
        .call_method(
            unsafe { JObject::from_raw(context()) },
            "checkSelfPermission",
            "(Ljava/lang/String;)I",
            &[(&mic_perm_jstring).into()],
        )
        .unwrap()
        .i()
        .unwrap();

    if permission_status != 0 {
        let string_class = env.find_class("java/lang/String").unwrap();
        let perm_array = env
            .new_object_array(1, string_class, mic_perm_jstring)
            .unwrap();

        env.call_method(
            unsafe { JObject::from_raw(context()) },
            "requestPermissions",
            "([Ljava/lang/String;I)V",
            &[(&perm_array).into(), 0.into()],
        )
        .unwrap();

        // todo: handle case where permission is rejected
    }
}

pub fn build_string(ty: &str) -> String {
    let vm = vm();
    let mut env = vm.attach_current_thread().unwrap();

    let jname = env
        .get_static_field("android/os/Build", ty, "Ljava/lang/String;")
        .unwrap()
        .l()
        .unwrap();
    let name_raw = env.get_string((&jname).into()).unwrap();

    name_raw.to_string_lossy().as_ref().to_owned()
}

pub fn device_name() -> String {
    build_string("DEVICE")
}

pub fn model_name() -> String {
    build_string("MODEL")
}

pub fn manufacturer_name() -> String {
    build_string("MANUFACTURER")
}

pub fn product_name() -> String {
    build_string("PRODUCT")
}

fn get_system_service<'a>(env: &mut JNIEnv<'a>, service_name: &str) -> JObject<'a> {
    let service_str = env.new_string(service_name).unwrap();

    env.call_method(
        unsafe { JObject::from_raw(context()) },
        "getSystemService",
        "(Ljava/lang/String;)Ljava/lang/Object;",
        &[(&service_str).into()],
    )
    .unwrap()
    .l()
    .unwrap()
}

// Enumerate all non-loopback IPv4 addresses across all active network interfaces.
// This picks up ethernet-over-USB adapters and other non-WiFi interfaces that
// WifiManager.getConnectionInfo() would miss.
pub fn local_ips() -> Vec<IpAddr> {
    let vm = vm();
    let mut env = vm.attach_current_thread().unwrap();

    let mut ips = Vec::new();

    let Ok(ni_class) = env.find_class("java/net/NetworkInterface") else {
        return ips;
    };
    let Ok(interfaces_jv) =
        env.call_static_method(ni_class, "getNetworkInterfaces", "()Ljava/util/Enumeration;", &[])
    else {
        return ips;
    };
    let Ok(interfaces) = interfaces_jv.l() else {
        return ips;
    };

    loop {
        let has_more = env
            .call_method(&interfaces, "hasMoreElements", "()Z", &[])
            .and_then(|v| v.z())
            .unwrap_or(false);
        if !has_more {
            break;
        }

        let Ok(iface) = env
            .call_method(&interfaces, "nextElement", "()Ljava/lang/Object;", &[])
            .and_then(|v| v.l())
        else {
            continue;
        };

        let is_loopback = env
            .call_method(&iface, "isLoopback", "()Z", &[])
            .and_then(|v| v.z())
            .unwrap_or(true);
        if is_loopback {
            continue;
        }

        let is_up = env
            .call_method(&iface, "isUp", "()Z", &[])
            .and_then(|v| v.z())
            .unwrap_or(false);
        if !is_up {
            continue;
        }

        let Ok(addrs) = env
            .call_method(&iface, "getInetAddresses", "()Ljava/util/Enumeration;", &[])
            .and_then(|v| v.l())
        else {
            continue;
        };

        loop {
            let has_more_addr = env
                .call_method(&addrs, "hasMoreElements", "()Z", &[])
                .and_then(|v| v.z())
                .unwrap_or(false);
            if !has_more_addr {
                break;
            }

            let Ok(addr_obj) = env
                .call_method(&addrs, "nextElement", "()Ljava/lang/Object;", &[])
                .and_then(|v| v.l())
            else {
                continue;
            };

            // Use getHostAddress() (returns a String) to avoid byte-array JNI complexity.
            let Ok(host_jv) = env
                .call_method(&addr_obj, "getHostAddress", "()Ljava/lang/String;", &[])
                .and_then(|v| v.l())
            else {
                continue;
            };
            let host_jstr: jni::objects::JString = host_jv.into();
            let Ok(host_str) = env.get_string(&host_jstr) else {
                continue;
            };
            let host = host_str.to_string_lossy();

            // Only keep plain IPv4 addresses (no scope-id suffix).
            if let Ok(ip @ IpAddr::V4(_)) = host.parse::<IpAddr>() {
                ips.push(ip);
            }
        }
    }

    ips
}

pub fn local_ip() -> IpAddr {
    local_ips()
        .into_iter()
        .next()
        .unwrap_or(IpAddr::V4(Ipv4Addr::UNSPECIFIED))
}

// This is needed to avoid wifi scans that disrupt streaming.
// Code inspired from https://github.com/Meumeu/WiVRn/blob/master/client/application.cpp
pub fn set_wifi_lock(enabled: bool) {
    let vm = vm();
    let mut env = vm.attach_current_thread().unwrap();

    let wifi_manager = get_system_service(&mut env, "wifi");

    fn set_lock<'a>(env: &mut JNIEnv<'a>, lock: &JObject, enabled: bool) {
        env.call_method(lock, "setReferenceCounted", "(Z)V", &[false.into()])
            .unwrap();
        env.call_method(
            &lock,
            if enabled { "acquire" } else { "release" },
            "()V",
            &[],
        )
        .unwrap();

        let lock_is_aquired = env
            .call_method(lock, "isHeld", "()Z", &[])
            .unwrap()
            .z()
            .unwrap();

        if lock_is_aquired != enabled {
            warn!("Failed to set wifi lock: expected {enabled}, got {lock_is_aquired}");
        }
    }

    let wifi_lock_jstring = env.new_string("alvr_wifi_lock").unwrap();
    let wifi_lock = env
        .call_method(
            &wifi_manager,
            "createWifiLock",
            "(ILjava/lang/String;)Landroid/net/wifi/WifiManager$WifiLock;",
            &[
                if get_api_level() >= 29 {
                    // Recommended for virtual reality since it disables WIFI scans
                    4 // WIFI_MODE_FULL_LOW_LATENCY
                } else {
                    3 // WIFI_MODE_FULL_HIGH_PERF
                }
                .into(),
                (&wifi_lock_jstring).into(),
            ],
        )
        .unwrap()
        .l()
        .unwrap();
    set_lock(&mut env, &wifi_lock, enabled);

    // let multicast_lock_jstring = env.new_string("alvr_multicast_lock").unwrap();
    // let multicast_lock = env
    //     .call_method(
    //         wifi_manager,
    //         "createMulticastLock",
    //         "(Ljava/lang/String;)Landroid/net/wifi/WifiManager$MulticastLock;",
    //         &[(&multicast_lock_jstring).into()],
    //     )
    //     .unwrap()
    //     .l()
    //     .unwrap();
    // set_lock(&mut env, &multicast_lock, enabled);
}

pub fn get_battery_status() -> (f32, bool) {
    let vm = vm();
    let mut env = vm.attach_current_thread().unwrap();

    let intent_action_jstring = env
        .new_string("android.intent.action.BATTERY_CHANGED")
        .unwrap();
    let intent_filter = env
        .new_object(
            "android/content/IntentFilter",
            "(Ljava/lang/String;)V",
            &[(&intent_action_jstring).into()],
        )
        .unwrap();
    let battery_intent = env
        .call_method(
            unsafe { JObject::from_raw(context()) },
            "registerReceiver",
            "(Landroid/content/BroadcastReceiver;Landroid/content/IntentFilter;)Landroid/content/Intent;",
            &[(&JObject::null()).into(), (&intent_filter).into()],
        )
        .unwrap()
        .l()
        .unwrap();

    let level_jstring = env.new_string("level").unwrap();
    let level = env
        .call_method(
            &battery_intent,
            "getIntExtra",
            "(Ljava/lang/String;I)I",
            &[(&level_jstring).into(), (-1).into()],
        )
        .unwrap()
        .i()
        .unwrap();
    let scale_jstring = env.new_string("scale").unwrap();
    let scale = env
        .call_method(
            &battery_intent,
            "getIntExtra",
            "(Ljava/lang/String;I)I",
            &[(&scale_jstring).into(), (-1).into()],
        )
        .unwrap()
        .i()
        .unwrap();

    let plugged_jstring = env.new_string("plugged").unwrap();
    let plugged = env
        .call_method(
            &battery_intent,
            "getIntExtra",
            "(Ljava/lang/String;I)I",
            &[(&plugged_jstring).into(), (-1).into()],
        )
        .unwrap()
        .i()
        .unwrap();

    (level as f32 / scale as f32, plugged > 0)
}
