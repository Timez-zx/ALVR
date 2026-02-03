# Android ALVR apk compilation on Ubuntu 24

This guide documents the Android toolchain setup used to build the ALVR APK on Ubuntu 24.

## 1) Install dependency

```bash
sudo apt install android-sdk-platform-tools-common sdkmanager google-android-ndk-r26b-installer
```

## 2) Install Android Command Line Tools (official)

Download the **Linux Command line tools** zip from:

https://developer.android.com/studio#command-tools

Extract to `~/Android/cmdline-tools/latest`:

```bash
mkdir -p ~/Android/cmdline-tools
unzip commandlinetools-linux-*.zip -d ~/Android/cmdline-tools
mv ~/Android/cmdline-tools/cmdline-tools ~/Android/cmdline-tools/latest
```

## 3) Set environment variables

Add these to `~/.bashrc` (or run in the current terminal):

```bash
export JAVA_HOME=/usr/lib/jvm/default-java
export ANDROID_HOME=$HOME/Android
export ANDROID_NDK_HOME=$ANDROID_HOME/ndk/25.1.8937393
export PATH=$ANDROID_HOME/cmdline-tools/latest/bin:$ANDROID_HOME/platform-tools:$PATH
```

Reload the shell:

```bash
source ~/.bashrc
```

## 4) Install SDK/NDK packages

Use the **official** `sdkmanager` from cmdline-tools:

```bash
~/Android/cmdline-tools/latest/bin/sdkmanager --sdk_root="$ANDROID_HOME" \
  "platform-tools" \
  "platforms;android-32" \
  "build-tools;32.0.0" \
  "ndk;25.1.8937393"
```

Accept licenses:

```bash
~/Android/cmdline-tools/latest/bin/sdkmanager --licenses
```

> Note: ALVR currently uses `target_sdk_version = 32` in `alvr/client_openxr/Cargo.toml`,
> so installing `platforms;android-32` is required.

## 5) Verify installation

```bash
echo $ANDROID_HOME
ls $ANDROID_HOME/platforms
ls $ANDROID_HOME/build-tools
```

You should see `android-32` and `32.0.0` in those directories.

## 6) Build the ALVR APK

From the repo root:

```bash
cargo xtask prepare-deps --platform android
cargo xtask build-client --release
```

For cleaning,

```bash
cargo xtask clean
cargo clean
```
