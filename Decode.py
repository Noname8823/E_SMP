"""
PCM UDP Sender -> ESP32-C3 W5500 -> PCM5102A
=============================================
Decode MP3 -> PCM 16-bit Stereo 44100Hz -> UDP stream

FIX PACKET LOSS:
  - Windows time.sleep() độ phân giải ~15ms -> gửi burst mỗi 15ms thay vì đều ~5.8ms
  - Fix: dùng busy-wait (spin) cho khoảng ngắn < 5ms
  - Fix: tăng chunk_size để interval > 15ms (tránh hoàn toàn vấn đề sleep)
  - Fix: gửi theo batch nhỏ với pacing tốt hơn

Cài đặt:
    pip install pydub
    ffmpeg: https://ffmpeg.org/download.html

Cách dùng:
    python pcm_udp_sender.py --file nhac.mp3 --ip 192.168.1.100
"""

import socket
import time
import argparse
import struct
import sys
import os

try:
    from pydub import AudioSegment
except ImportError:
    print("[LOI] Chua cai pydub: pip install pydub")
    sys.exit(1)

# ──────────────────────────────────────────────
# CẤU HÌNH
# ──────────────────────────────────────────────
DEFAULT_IP   = "192.168.1.100"
DEFAULT_PORT = 5000
SAMPLE_RATE  = 44100
CHANNELS     = 2
SAMPLE_WIDTH = 2                     # 16-bit
MAGIC        = b'\xAB\xCD'

# KEY FIX: chunk_size phải đủ lớn để interval > 15ms (giới hạn sleep Windows)
# bytes_per_sec = 44100 * 2 * 2 = 176400
# interval = chunk_size / 176400
# Để interval >= 16ms: chunk_size >= 176400 * 0.016 = 2822 bytes -> dùng 2820 (chia hết 4)
#
# Nếu muốn chunk nhỏ hơn (latency thấp hơn), dùng busy-wait spin loop
DEFAULT_CHUNK_SIZE = 2820   # ~16ms/packet, an toàn với Windows sleep

# ──────────────────────────────────────────────
# HIGH-RESOLUTION SLEEP
# Dùng busy-wait (spin) khi cần sleep < SPIN_THRESHOLD
# Tránh vấn đề Windows timer resolution 15ms
# ──────────────────────────────────────────────
SPIN_THRESHOLD = 0.002  # 2ms: dưới ngưỡng này dùng spin thay vì sleep

def precise_sleep(seconds: float):
    """Sleep chính xác cao, kết hợp sleep + spin."""
    if seconds <= 0:
        return
    if seconds < SPIN_THRESHOLD:
        # Busy-wait hoàn toàn cho khoảng rất ngắn
        deadline = time.perf_counter() + seconds
        while time.perf_counter() < deadline:
            pass
        return
    # Sleep phần lớn, spin phần nhỏ cuối
    sleep_time = seconds - SPIN_THRESHOLD
    if sleep_time > 0:
        time.sleep(sleep_time)
    # Spin nốt phần còn lại
    deadline = time.perf_counter() + SPIN_THRESHOLD
    while time.perf_counter() < deadline:
        pass

# ──────────────────────────────────────────────
# Trên Windows: tăng timer resolution lên 1ms
# ──────────────────────────────────────────────
def set_windows_timer_resolution():
    if os.name == 'nt':
        try:
            import ctypes
            # timeBeginPeriod(1) -> yêu cầu Windows timer resolution 1ms
            ctypes.windll.winmm.timeBeginPeriod(1)
            print("[SYS] Windows timer resolution: 1ms")
            return True
        except Exception:
            pass
    return False

def restore_windows_timer_resolution():
    if os.name == 'nt':
        try:
            import ctypes
            ctypes.windll.winmm.timeEndPeriod(1)
        except Exception:
            pass

# ──────────────────────────────────────────────
# PACKET BUILDERS
# ──────────────────────────────────────────────
def build_packet(seq: int, data: bytes) -> bytes:
    return MAGIC + struct.pack(">IH", seq, len(data)) + data

def build_end_packet(seq: int) -> bytes:
    return MAGIC + struct.pack(">IH", seq, 0xFFFF)

# ──────────────────────────────────────────────
# DECODE MP3
# ──────────────────────────────────────────────
def decode_mp3_to_pcm(filepath: str) -> bytes:
    print(f"[DECODE] Doc va decode: {filepath}")
    try:
        audio = AudioSegment.from_file(filepath)
    except Exception as e:
        print(f"[LOI] Khong decode duoc: {e}")
        sys.exit(1)

    audio = audio.set_frame_rate(SAMPLE_RATE)
    audio = audio.set_channels(CHANNELS)
    audio = audio.set_sample_width(SAMPLE_WIDTH)

    pcm_data = audio.raw_data
    duration = len(audio) / 1000.0
    print(f"[DECODE] {duration:.1f}s | {len(pcm_data)/1024/1024:.2f} MB PCM")
    return pcm_data

# ──────────────────────────────────────────────
# STREAM
# ──────────────────────────────────────────────
def stream_pcm_udp(filepath, esp32_ip, udp_port, chunk_size, loop=False):
    chunk_size = (chunk_size // 4) * 4
    if chunk_size <= 0:
        chunk_size = DEFAULT_CHUNK_SIZE

    bytes_per_sec = SAMPLE_RATE * CHANNELS * SAMPLE_WIDTH  # 176400
    interval      = chunk_size / bytes_per_sec             # giây/packet

    # Cảnh báo nếu interval quá ngắn cho Windows
    if os.name == 'nt' and interval < 0.015:
        print(f"[WARN] interval={interval*1000:.1f}ms < 15ms (gioi han sleep Windows)")
        print(f"[WARN] Se dung busy-wait. CPU usage cao hon nhung chinh xac hon.")
        print(f"[HINT] Dung --chunk {int(bytes_per_sec * 0.016 // 4 * 4)} de tranh van de nay")

    print("=" * 55)
    print(f"  File     : {filepath}")
    print(f"  Dest     : {esp32_ip}:{udp_port}")
    print(f"  Chunk    : {chunk_size} bytes ({chunk_size//4} samples)")
    print(f"  Interval : {interval*1000:.2f} ms/packet")
    print(f"  Rate     : {bytes_per_sec/1000:.0f} KB/s PCM raw")
    print(f"  OS       : {'Windows' if os.name == 'nt' else 'Linux/Mac'}")
    print("=" * 55)

    # Tăng timer resolution Windows
    timer_set = set_windows_timer_resolution()

    pcm_data   = decode_mp3_to_pcm(filepath)
    total_size = len(pcm_data)

    # Socket
    sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    sock.setsockopt(socket.SOL_SOCKET, socket.SO_SNDBUF, 131072)  # 128KB OS send buffer
    dest = (esp32_ip, udp_port)

    # Đo jitter thực tế của sleep (debug)
    _test_sleep_jitter()

    try:
        run = 0
        while True:
            run += 1
            n_packets  = (total_size + chunk_size - 1) // chunk_size
            print(f"\n[STREAM] Lan {run} | {n_packets} packets x {chunk_size}B ...")

            seq            = 0
            sent_bytes     = 0
            dropped_warn   = 0
            t_start        = time.perf_counter()
            next_send_time = t_start

            for i in range(0, total_size, chunk_size):
                chunk = pcm_data[i : i + chunk_size]

                # ── Gửi packet ──
                sock.sendto(build_packet(seq, chunk), dest)
                seq        += 1
                sent_bytes += len(chunk)

                # ── Tính thời điểm gửi packet tiếp theo ──
                next_send_time += interval

                # ── Sleep/spin đến đúng thời điểm ──
                now  = time.perf_counter()
                wait = next_send_time - now

                if wait > 0:
                    precise_sleep(wait)
                elif wait < -0.010:
                    # Trễ hơn 10ms: đang bị overrun, resync
                    dropped_warn += 1
                    next_send_time = time.perf_counter()  # reset baseline

                # ── Log mỗi 2 giây ──
                if seq % max(1, int(2.0 / interval)) == 0:
                    elapsed  = time.perf_counter() - t_start
                    progress = sent_bytes / total_size * 100
                    remain   = (total_size - sent_bytes) / bytes_per_sec
                    actual_rate = sent_bytes / elapsed / 1000 if elapsed > 0 else 0
                    print(f"  [{progress:5.1f}%] {elapsed:.0f}s elapsed | "
                          f"~{remain:.0f}s remain | "
                          f"{actual_rate:.0f} KB/s | "
                          f"overrun={dropped_warn}")

            # END packet
            sock.sendto(build_end_packet(seq), dest)
            elapsed = time.perf_counter() - t_start
            print(f"\n[DONE] {elapsed:.2f}s | overrun warnings: {dropped_warn}")

            if not loop:
                break
            print("[LOOP] Lap lai sau 1 giay...")
            time.sleep(1.0)

    except KeyboardInterrupt:
        print("\n[STOP] Dung stream.")
    finally:
        sock.close()
        if timer_set:
            restore_windows_timer_resolution()

# ──────────────────────────────────────────────
# DEBUG: đo jitter sleep thực tế
# ──────────────────────────────────────────────
def _test_sleep_jitter():
    print("[JITTER TEST] Do do chinh xac sleep...")
    errors = []
    for target_ms in [1, 2, 5, 8, 16]:
        t0 = time.perf_counter()
        precise_sleep(target_ms / 1000.0)
        actual_ms = (time.perf_counter() - t0) * 1000
        error_ms  = actual_ms - target_ms
        errors.append(error_ms)
        print(f"  target={target_ms:2d}ms  actual={actual_ms:.2f}ms  error={error_ms:+.2f}ms")
    avg_err = sum(abs(e) for e in errors) / len(errors)
    print(f"  Trung binh sai so: {avg_err:.2f}ms")
    if avg_err > 5:
        print("  [WARN] Sai so lon! Nen dung chunk_size lon hon.")
    else:
        print("  [OK] Sleep chinh xac du dung.")

# ──────────────────────────────────────────────
# MAIN
# ──────────────────────────────────────────────
def main():
    parser = argparse.ArgumentParser(description="Stream PCM audio qua UDP den ESP32")
    parser.add_argument("--file",  required=True,                    help="File MP3/WAV/FLAC")
    parser.add_argument("--ip",    default=DEFAULT_IP,               help=f"IP ESP32 (default: {DEFAULT_IP})")
    parser.add_argument("--port",  type=int, default=DEFAULT_PORT,   help=f"UDP port (default: {DEFAULT_PORT})")
    parser.add_argument("--chunk", type=int, default=DEFAULT_CHUNK_SIZE,
                        help=f"Bytes/packet (default: {DEFAULT_CHUNK_SIZE} = ~16ms)")
    parser.add_argument("--loop",  action="store_true",              help="Lap lai khi het file")
    args = parser.parse_args()

    stream_pcm_udp(
        filepath   = args.file,
        esp32_ip   = args.ip,
        udp_port   = args.port,
        chunk_size = args.chunk,
        loop       = args.loop,
    )

if __name__ == "__main__":
    main()