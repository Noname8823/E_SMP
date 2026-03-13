"""
PCM UDP Sender - REAL-TIME -> ESP32-C3 W5500 -> PCM5102A
=========================================================
Real-time: decode MP3 từng chunk nhỏ + gửi UDP đồng thời
-> Nghe tiếng ngay trong ~0.5 giây, không cần chờ decode xong toàn bộ

Cài đặt:
    pip install pydub
    ffmpeg: winget install ffmpeg  (Windows)
            sudo apt install ffmpeg (Linux)

Cách dùng:
    python pcm_udp_sender.py --file nhac.mp3 --ip 192.168.1.100
    python pcm_udp_sender.py --file nhac.mp3 --ip 192.168.1.100 --loop
"""

import socket
import time
import argparse
import struct
import sys
import os
import threading
import queue

try:
    from pydub import AudioSegment
except ImportError:
    print("[LOI] Chua cai pydub: pip install pydub")
    sys.exit(1)

# ──────────────────────────────────────────────
# CẤU HÌNH
# ──────────────────────────────────────────────
DEFAULT_IP         = "192.168.1.100"
DEFAULT_PORT       = 5000
SAMPLE_RATE        = 44100
CHANNELS           = 2
SAMPLE_WIDTH       = 2                                          # 16-bit
BYTES_PER_SEC      = SAMPLE_RATE * CHANNELS * SAMPLE_WIDTH     # 176400

# UDP chunk: 2820 bytes = ~16ms/packet (an toàn với Windows timer 15ms)
DEFAULT_CHUNK_SIZE = 2820

# Decode mỗi lần 200ms PCM -> đẩy vào queue
DECODE_CHUNK_MS    = 200
DECODE_CHUNK_BYTES = int(BYTES_PER_SEC * DECODE_CHUNK_MS / 1000 / 4) * 4

# Queue tối đa 2 giây audio (tránh dùng quá nhiều RAM khi decode nhanh hơn phát)
QUEUE_MAX_SECONDS  = 2
QUEUE_MAXSIZE      = int(QUEUE_MAX_SECONDS * 1000 / DECODE_CHUNK_MS) + 4

# Prebuffer trước khi gửi UDP (tránh underrun ESP32 ngay đầu)
PREBUFFER_SECONDS  = 0.5

MAGIC = b'\xAB\xCD'

# ──────────────────────────────────────────────
# WINDOWS TIMER RESOLUTION FIX
# Nâng độ phân giải timer Windows từ ~15ms lên 1ms
# ──────────────────────────────────────────────
def _set_win_timer():
    if os.name == 'nt':
        try:
            import ctypes
            ctypes.windll.winmm.timeBeginPeriod(1)
            print("[SYS] Windows timer: 1ms resolution")
            return True
        except Exception:
            pass
    return False

def _restore_win_timer():
    if os.name == 'nt':
        try:
            import ctypes
            ctypes.windll.winmm.timeEndPeriod(1)
        except Exception:
            pass

# ──────────────────────────────────────────────
# PRECISE SLEEP: sleep lớn + spin nhỏ cuối
# Tránh overshoot của time.sleep() trên Windows
# ──────────────────────────────────────────────
SPIN_THRESHOLD = 0.002  # spin 2ms cuối để chính xác

def precise_sleep(seconds: float):
    if seconds <= 0:
        return
    if seconds <= SPIN_THRESHOLD:
        deadline = time.perf_counter() + seconds
        while time.perf_counter() < deadline:
            pass
        return
    time.sleep(seconds - SPIN_THRESHOLD)
    deadline = time.perf_counter() + SPIN_THRESHOLD
    while time.perf_counter() < deadline:
        pass

# ──────────────────────────────────────────────
# PACKET BUILDERS
# ──────────────────────────────────────────────
def build_packet(seq: int, data: bytes) -> bytes:
    return MAGIC + struct.pack(">IH", seq, len(data)) + data

def build_end_packet(seq: int) -> bytes:
    return MAGIC + struct.pack(">IH", seq, 0xFFFF)

# ──────────────────────────────────────────────
# DECODER THREAD
# Chạy song song: load MP3 -> convert -> đẩy PCM vào queue từng DECODE_CHUNK_MS
# ──────────────────────────────────────────────
def decoder_thread(filepath: str, pcm_queue: queue.Queue, stop_event: threading.Event):
    try:
        print(f"[DECODE] Doc file: {filepath}")
        t0    = time.perf_counter()
        audio = AudioSegment.from_file(filepath)
        audio = audio.set_frame_rate(SAMPLE_RATE)
        audio = audio.set_channels(CHANNELS)
        audio = audio.set_sample_width(SAMPLE_WIDTH)

        dur_ms = len(audio)
        print(f"[DECODE] Xong trong {time.perf_counter()-t0:.1f}s | "
              f"Duration={dur_ms/1000:.1f}s | "
              f"PCM={len(audio.raw_data)/1024/1024:.1f}MB")

        # Chia nhỏ và đẩy vào queue
        ms = 0
        while ms < dur_ms and not stop_event.is_set():
            end_ms  = min(ms + DECODE_CHUNK_MS, dur_ms)
            chunk   = audio[ms:end_ms].raw_data
            if chunk:
                pcm_queue.put(chunk, block=True, timeout=10.0)
            ms = end_ms

        pcm_queue.put(None)  # sentinel báo hết

    except Exception as e:
        print(f"[DECODE ERR] {e}")
        pcm_queue.put(None)

# ──────────────────────────────────────────────
# STREAM REAL-TIME
# ──────────────────────────────────────────────
def stream_realtime(filepath, esp32_ip, udp_port, chunk_size, loop=False):
    chunk_size = (chunk_size // 4) * 4 or DEFAULT_CHUNK_SIZE
    interval   = chunk_size / BYTES_PER_SEC   # giây/packet

    print("=" * 55)
    print("  PCM UDP REAL-TIME STREAMER")
    print(f"  File     : {filepath}")
    print(f"  Dest     : {esp32_ip}:{udp_port}")
    print(f"  UDP chunk: {chunk_size}B = {interval*1000:.1f}ms/packet")
    print(f"  Prebuffer: {PREBUFFER_SECONDS*1000:.0f}ms")
    print(f"  OS       : {'Windows' if os.name=='nt' else 'Linux/Mac'}")
    print("=" * 55)

    timer_set = _set_win_timer()

    sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    sock.setsockopt(socket.SOL_SOCKET, socket.SO_SNDBUF, 131072)
    dest = (esp32_ip, udp_port)

    run = 0
    try:
        while True:
            run += 1
            print(f"\n[STREAM] === Lan {run} ===")

            pcm_queue  = queue.Queue(maxsize=QUEUE_MAXSIZE)
            stop_event = threading.Event()

            # Khởi động decoder song song
            dec = threading.Thread(
                target=decoder_thread,
                args=(filepath, pcm_queue, stop_event),
                daemon=True, name="Decoder"
            )
            dec.start()

            # ── Prebuffer: chờ đủ dữ liệu trước khi gửi ──
            PREBUFFER_BYTES = int(BYTES_PER_SEC * PREBUFFER_SECONDS)
            send_buf        = bytearray()
            stream_done     = False

            print(f"[STREAM] Prebuffer {PREBUFFER_BYTES//1024}KB...")
            while len(send_buf) < PREBUFFER_BYTES:
                try:
                    pc = pcm_queue.get(timeout=10.0)
                except queue.Empty:
                    print("[WARN] Timeout khi prebuffer")
                    stream_done = True
                    break
                if pc is None:
                    stream_done = True
                    break
                send_buf.extend(pc)

            print(f"[STREAM] Bat dau gui UDP ({len(send_buf)} bytes trong buffer)...")

            # ── Sender loop ──
            seq            = 0
            sent_bytes     = 0
            overruns       = 0
            t_start        = time.perf_counter()
            next_send_time = t_start
            last_log_time  = t_start

            def send_from_buf():
                """Gửi hết những gì có trong send_buf theo chunk_size, có pacing."""
                nonlocal seq, sent_bytes, next_send_time, overruns
                while len(send_buf) >= chunk_size:
                    pkt = bytes(send_buf[:chunk_size])
                    del send_buf[:chunk_size]

                    sock.sendto(build_packet(seq, pkt), dest)
                    seq        += 1
                    sent_bytes += chunk_size

                    # Pacing chính xác
                    next_send_time += interval
                    wait = next_send_time - time.perf_counter()
                    if wait > 0:
                        precise_sleep(wait)
                    elif wait < -0.020:
                        overruns      += 1
                        next_send_time = time.perf_counter()  # resync

            # Gửi phần đã prebuffer
            send_from_buf()

            # Tiếp tục nhận từ queue và gửi
            while not stream_done:
                try:
                    pc = pcm_queue.get(timeout=5.0)
                except queue.Empty:
                    print("[WARN] Queue timeout")
                    break

                if pc is None:
                    stream_done = True
                else:
                    send_buf.extend(pc)

                send_from_buf()

                # Log mỗi 3 giây
                now = time.perf_counter()
                if now - last_log_time >= 3.0:
                    last_log_time = now
                    played = sent_bytes / BYTES_PER_SEC
                    print(f"  [{played:.0f}s played] "
                          f"buf={len(send_buf)//1024}KB "
                          f"queue={pcm_queue.qsize()} "
                          f"overrun={overruns}")

            # Flush phần lẻ cuối (< chunk_size)
            remainder = (len(send_buf) // 4) * 4
            if remainder > 0:
                sock.sendto(build_packet(seq, bytes(send_buf[:remainder])), dest)
                seq += 1

            # END packet
            sock.sendto(build_end_packet(seq), dest)
            stop_event.set()
            dec.join(timeout=3.0)

            elapsed = time.perf_counter() - t_start
            print(f"\n[DONE] {elapsed:.1f}s | {seq} packets | overrun={overruns}")

            if not loop:
                break
            print("[LOOP] Lap lai sau 1 giay...")
            time.sleep(1.0)

    except KeyboardInterrupt:
        print("\n[STOP] Dung stream.")
        stop_event.set()
    finally:
        sock.close()
        if timer_set:
            _restore_win_timer()

# ──────────────────────────────────────────────
# MAIN
# ──────────────────────────────────────────────
def main():
    parser = argparse.ArgumentParser(
        description="Stream MP3 real-time qua UDP den ESP32 (PCM5102A)"
    )
    parser.add_argument("--file",  required=True,
                        help="File MP3/WAV/FLAC")
    parser.add_argument("--ip",    default=DEFAULT_IP,
                        help=f"IP ESP32 (default: {DEFAULT_IP})")
    parser.add_argument("--port",  type=int, default=DEFAULT_PORT,
                        help=f"UDP port (default: {DEFAULT_PORT})")
    parser.add_argument("--chunk", type=int, default=DEFAULT_CHUNK_SIZE,
                        help=f"Bytes/UDP packet (default: {DEFAULT_CHUNK_SIZE} = ~16ms)")
    parser.add_argument("--loop",  action="store_true",
                        help="Lap lai khi het file")
    args = parser.parse_args()

    if not os.path.isfile(args.file):
        print(f"[LOI] Khong tim thay file: {args.file}")
        sys.exit(1)

    stream_realtime(
        filepath   = args.file,
        esp32_ip   = args.ip,
        udp_port   = args.port,
        chunk_size = args.chunk,
        loop       = args.loop,
    )

if __name__ == "__main__":
    main()