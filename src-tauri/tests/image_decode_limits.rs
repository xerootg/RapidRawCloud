//! Decode-limit regression test for `image_loader::load_image_with_orientation`
//! (security audit: untrusted image header → unbounded allocation).
//!
//! The loader builds an `image::ImageReader`, calls `reader.no_limits()` and
//! then `decode()`. With no limits, `image` 0.25 trusts the dimensions in the
//! file header and allocates the full output buffer (`width * height * bpp`)
//! BEFORE a single pixel is validated. A PNG whose IHDR claims 100000×100000
//! RGBA8 — 1.3 KB on disk — therefore makes the decoder request a 40 GB
//! `Vec`, which ends in `handle_alloc_error` → SIGABRT (or, on a box that
//! lazily overcommits, a multi-GB reservation that is later written into).
//! One hostile file in the library, or one synced from a peer, takes the
//! whole app down. With the crate's default `Limits` (512 MiB `max_alloc`) or
//! any explicit sane limit the same bytes fail in ~30 µs with
//! `ImageError::Limits` and ~11 MB RSS (measured against image 0.25.10 /
//! png 0.18.1; see the test report).
//!
//! Harness. The decode runs in a CHILD PROCESS (this test binary re-executed
//! with `--exact <child entry> --ignored`) under `ulimit -v`, a virtual
//! address-space cap. That makes the proof deterministic and container-safe:
//! an attempted multi-GB allocation fails immediately with an allocation
//! error instead of succeeding lazily, nothing is ever written to that
//! memory, and peak RSS stays in the tens of MB. The parent asserts the child
//! exited normally with `Err(..)`, not that it was killed by a signal.
//!
//! * Today (bug present): the child aborts — `memory allocation of
//!   40000000000 bytes failed` — and these tests FAIL.
//! * After the fix (explicit `Limits` instead of `no_limits()`): the child
//!   prints a clean `Err`, and these tests pass. The positive control proves
//!   the same harness still decodes a real (tiny) PNG under the same cap.
//!
//! Reaches the module-private loader through the `sync::proxy_support` test
//! seam (`pub use crate::image_loader::load_image_with_orientation;`), the
//! same pure-visibility pattern the P3 fidelity suite uses. `ulimit -v`
//! (RLIMIT_AS) is only reliably enforced on Linux, so the file is gated.
#![cfg(all(feature = "sync", target_os = "linux"))]

use std::io::Cursor;
use std::process::{Command, ExitStatus};

use rapidraw_lib::sync::proxy_support::load_image_with_orientation;

/// Environment variable that tells the re-executed test binary which case to
/// decode. Absent in the parent; `oversize`, `six_gib` or `small` in the child.
const CHILD_CASE_ENV: &str = "RAPIDRAW_IMAGE_LIMITS_CHILD_CASE";
/// `ulimit -v` cap for the child, in KiB (~3.8 GiB). Far above what the test
/// binary needs to start and decode a tiny PNG (the positive control proves
/// it), far below the smallest hostile allocation attempted here (6.4 GB).
const CHILD_VMEM_KB: u64 = 4_000_000;
const CHILD_ENTRY: &str = "child_entry_do_not_run_directly";

// ---------------------------------------------------------------------------
// Crafted inputs
// ---------------------------------------------------------------------------

fn crc32(data: &[u8]) -> u32 {
    let mut crc = 0xFFFF_FFFFu32;
    for &b in data {
        crc ^= u32::from(b);
        for _ in 0..8 {
            crc = if crc & 1 != 0 {
                (crc >> 1) ^ 0xEDB8_8320
            } else {
                crc >> 1
            };
        }
    }
    !crc
}

fn png_chunk(out: &mut Vec<u8>, ty: &[u8; 4], data: &[u8]) {
    out.extend_from_slice(&(data.len() as u32).to_be_bytes());
    let mut crc_input = ty.to_vec();
    crc_input.extend_from_slice(data);
    out.extend_from_slice(ty);
    out.extend_from_slice(data);
    out.extend_from_slice(&crc32(&crc_input).to_be_bytes());
}

/// A structurally valid PNG (signature, CRC-correct IHDR/IDAT/IEND) whose
/// header declares `w × h` 8-bit RGBA pixels but whose IDAT holds only a
/// 3-byte truncated zlib stream. ~70 bytes on disk; declares `w*h*4` bytes of
/// pixels. No decoder can produce a pixel from it, so the only way to spend
/// memory on it is to trust the header.
fn hostile_png_header(w: u32, h: u32) -> Vec<u8> {
    let mut out = vec![0x89, b'P', b'N', b'G', 0x0d, 0x0a, 0x1a, 0x0a];
    let mut ihdr = Vec::with_capacity(13);
    ihdr.extend_from_slice(&w.to_be_bytes());
    ihdr.extend_from_slice(&h.to_be_bytes());
    ihdr.extend_from_slice(&[8, 6, 0, 0, 0]); // bit depth 8, colour type 6 (RGBA)
    png_chunk(&mut out, b"IHDR", &ihdr);
    png_chunk(&mut out, b"IDAT", &[0x78, 0x9c, 0x63]); // zlib header + 1 byte
    png_chunk(&mut out, b"IEND", &[]);
    out
}

/// A real 4×4 RGBA PNG, encoded by the `image` crate itself.
fn small_valid_png() -> Vec<u8> {
    let mut img = image::RgbaImage::new(4, 4);
    for (x, y, p) in img.enumerate_pixels_mut() {
        *p = image::Rgba([(x * 60) as u8, (y * 60) as u8, 128, 255]);
    }
    let mut buf = Cursor::new(Vec::new());
    image::DynamicImage::ImageRgba8(img)
        .write_to(&mut buf, image::ImageFormat::Png)
        .expect("encode control PNG");
    buf.into_inner()
}

fn case_bytes(case: &str) -> (Vec<u8>, u64) {
    match case {
        // 100000 × 100000 RGBA8 → 40 GB declared.
        "oversize" => (hostile_png_header(100_000, 100_000), 40_000_000_000),
        // 40000 × 40000 RGBA8 → 6.4 GB declared. Below the Linux heuristic
        // overcommit refusal on large hosts (so it "succeeds" lazily there
        // and the app later faults the pages in); only `ulimit -v` makes it
        // observable deterministically.
        "six_gib" => (hostile_png_header(40_000, 40_000), 6_400_000_000),
        "small" => (small_valid_png(), 64),
        other => panic!("unknown child case {other:?}"),
    }
}

// ---------------------------------------------------------------------------
// Child side
// ---------------------------------------------------------------------------

fn vm_peak_kb() -> Option<String> {
    let status = std::fs::read_to_string("/proc/self/status").ok()?;
    status
        .lines()
        .find(|l| l.starts_with("VmPeak:"))
        .map(|l| l.trim_start_matches("VmPeak:").trim().to_string())
}

/// Child-process entry point. Never asserts; it only reports. Spawned by the
/// tests below with `--exact child_entry_do_not_run_directly --ignored` and
/// `RAPIDRAW_IMAGE_LIMITS_CHILD_CASE` set, after the shell applied `ulimit -v`.
#[test]
#[ignore = "child-process entry point, re-executed by the tests in this file"]
fn child_entry_do_not_run_directly() {
    let Ok(case) = std::env::var(CHILD_CASE_ENV) else {
        println!("CHILD_RESULT=skipped: {CHILD_CASE_ENV} unset");
        return;
    };
    let (bytes, declared) = case_bytes(&case);
    println!(
        "CHILD_INFO case={case} file_bytes={} declared_pixel_bytes={declared} vm_peak_before={}",
        bytes.len(),
        vm_peak_kb().unwrap_or_default()
    );
    // ---- the call under test -------------------------------------------
    let result = load_image_with_orientation(&bytes, None);
    // ---------------------------------------------------------------------
    match result {
        Ok(img) => println!("CHILD_RESULT=ok:{}x{}", img.width(), img.height()),
        Err(e) => println!("CHILD_RESULT=err:{e:#}"),
    }
    println!(
        "CHILD_INFO vm_peak_after={}",
        vm_peak_kb().unwrap_or_default()
    );
}

// ---------------------------------------------------------------------------
// Parent side
// ---------------------------------------------------------------------------

struct ChildRun {
    status: ExitStatus,
    stdout: String,
    stderr: String,
}

fn run_child(case: &str) -> ChildRun {
    let exe = std::env::current_exe().expect("current_exe");
    // `sh -c '<script>' <arg0>`: `$0` is the test binary. `ulimit -v` caps the
    // child's virtual address space so an oversize allocation fails at once
    // (allocation error → abort) instead of being lazily overcommitted.
    let script = format!(
        "ulimit -v {CHILD_VMEM_KB} && exec \"$0\" --exact {CHILD_ENTRY} --ignored --nocapture --test-threads=1"
    );
    let output = Command::new("sh")
        .arg("-c")
        .arg(script)
        .arg(&exe)
        .env(CHILD_CASE_ENV, case)
        .env("RUST_BACKTRACE", "0")
        .output()
        .expect("spawn child test process");
    ChildRun {
        status: output.status,
        stdout: String::from_utf8_lossy(&output.stdout).into_owned(),
        stderr: String::from_utf8_lossy(&output.stderr).into_owned(),
    }
}

fn describe(run: &ChildRun) -> String {
    use std::os::unix::process::ExitStatusExt;
    let how = match (run.status.code(), run.status.signal()) {
        (Some(c), _) => format!("exit code {c}"),
        (None, Some(s)) => format!(
            "killed by signal {s}{}",
            if s == 6 { " (SIGABRT)" } else { "" }
        ),
        _ => "unknown status".to_string(),
    };
    format!(
        "child {how}\n--- child stdout ---\n{}\n--- child stderr ---\n{}",
        run.stdout.trim_end(),
        run.stderr.trim_end()
    )
}

fn child_result_line(run: &ChildRun) -> Option<&str> {
    run.stdout
        .lines()
        .find_map(|l| l.strip_prefix("CHILD_RESULT="))
}

/// The hostile header must come back as a clean `Err` from
/// `load_image_with_orientation`, with the process alive.
fn assert_header_refused(case: &str, w: u32, h: u32, declared: u64) {
    let run = run_child(case);
    let result = child_result_line(&run);

    assert!(
        run.status.success(),
        "`load_image_with_orientation` must REFUSE a PNG whose header declares {w}x{h} RGBA8 \
         ({declared} bytes of pixels, ~70 bytes on disk) with a clean `Err`, but the decoding \
         process died instead.\n\
         Cause: src/image_loader.rs calls `reader.no_limits()`, so the `image` crate trusts the \
         header and allocates the full {declared}-byte output buffer before validating any pixel \
         data (`memory allocation of {declared} bytes failed` → `handle_alloc_error` → SIGABRT \
         under the test's `ulimit -v`; on an unrestricted host this is a multi-GB reservation \
         instead). Replace `no_limits()` with explicit `image::Limits` (max width/height + \
         `max_alloc`) so this fails fast with `ImageError::Limits`.\n{}",
        describe(&run)
    );
    match result {
        Some(r) if r.starts_with("err:") => {
            // Expected post-fix outcome. Anything that is not a decode-stage
            // error would also be acceptable, but it must NOT have reached the
            // pixel stage (which would mean the buffer was allocated).
            assert!(
                !r.contains("CorruptFlateStream") && !r.contains("InsufficientInput"),
                "the decoder got as far as inflating IDAT for a {w}x{h} header, which means the \
                 {declared}-byte output buffer WAS allocated (lazily) before any limit check; \
                 limits must reject the dimensions/allocation up front.\n{}",
                describe(&run)
            );
        }
        Some(r) => panic!(
            "hostile {w}x{h} header unexpectedly decoded: {r}\n{}",
            describe(&run)
        ),
        None => panic!("child produced no CHILD_RESULT line\n{}", describe(&run)),
    }
}

#[test]
fn oversize_png_header_is_refused_without_allocating() {
    assert_header_refused("oversize", 100_000, 100_000, 40_000_000_000);
}

#[test]
fn six_gigabyte_png_header_is_refused_without_allocating() {
    assert_header_refused("six_gib", 40_000, 40_000, 6_400_000_000);
}

/// Positive control: the same harness, same `ulimit -v`, same entry point —
/// a real 4×4 PNG must decode. Proves the cap is not what makes the hostile
/// cases fail, and that the loader still works after limits are added.
#[test]
fn small_png_decodes_under_same_memory_cap() {
    let run = run_child("small");
    assert!(
        run.status.success(),
        "control child did not exit cleanly\n{}",
        describe(&run)
    );
    assert_eq!(
        child_result_line(&run),
        Some("ok:4x4"),
        "control 4x4 PNG must decode through `load_image_with_orientation`\n{}",
        describe(&run)
    );
}
