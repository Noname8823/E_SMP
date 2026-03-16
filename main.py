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
# CONFIG
# ──────────────────────────────────────────────
DEFAULT_IP   = "192.168.1.100"
DEFAULT_PORT = 5000

MAGIC = b"\xAB\xCD"

PKT_MUSIC_PCM  = 1
PKT_CALL_G711A = 2
PKT_CALL_G711U = 3

# G711 sender config
G711_SAMPLE_RATE  = 8000
G711_CHANNELS     = 1
G711_SAMPLE_WIDTH = 2   # PCM16 trước khi encode
G711_BYTES_PER_SEC = 8000   # 8000 samples/s * 1 byte/sample

DEFAULT_CODEC = "alaw"       # alaw | ulaw
DEFAULT_CHUNK = 160          # 160 bytes = 20 ms ở 8kHz
DECODE_CHUNK_MS = 200
PREBUFFER_SECONDS = 0.20
QUEUE_MAXSIZE = 16

# ──────────────────────────────────────────────
# TIMER FIX CHO WINDOWS
# ──────────────────────────────────────────────
def _set_win_timer():
    if os.name == "nt":
        try:
            import ctypes
            ctypes.windll.winmm.timeBeginPeriod(1)
            print("[SYS] Windows timer: 1ms resolution")
            return True
        except Exception:
            pass
    return False

def _restore_win_timer():
    if os.name == "nt":
        try:
            import ctypes
            ctypes.windll.winmm.timeEndPeriod(1)
        except Exception:
            pass

SPIN_THRESHOLD = 0.002

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
# Header đúng với ESP:
# [0:2]  magic
# [2]    type
# [3:7]  seq
# [7:9]  dataLen
# [9..]  payload
# ──────────────────────────────────────────────
def build_packet(pkt_type: int, seq: int, data: bytes) -> bytes:
    return MAGIC + struct.pack(">BIH", pkt_type, seq, len(data)) + data

def build_end_packet(pkt_type: int, seq: int) -> bytes:
    return MAGIC + struct.pack(">BIH", pkt_type, seq, 0xFFFF)

# ──────────────────────────────────────────────
# G711 ENCODER (PURE PYTHON)
# ──────────────────────────────────────────────
SEG_AEND = (0x1F, 0x3F, 0x7F, 0xFF, 0x1FF, 0x3FF, 0x7FF, 0xFFF)
SEG_UEND = (0x3F, 0x7F, 0xFF, 0x1FF, 0x3FF, 0x7FF, 0xFFF, 0x1FFF)
ULAW_BIAS = 0x84
ULAW_CLIP = 8159

def _search_segment(val: int, table) -> int:
    for i, end in enumerate(table):
        if val <= end:
            return i
    return len(table)

def linear2alaw_sample(pcm_val: int) -> int:
    if pcm_val >= 0:
        mask = 0xD5
    else:
        mask = 0x55
        pcm_val = -pcm_val - 8
        if pcm_val < 0:
            pcm_val = 0

    seg = _search_segment(pcm_val, SEG_AEND)
    if seg >= 8:
        return 0x7F ^ mask

    aval = seg << 4
    if seg < 2:
        aval |= (pcm_val >> 4) & 0x0F
    else:
        aval |= (pcm_val >> (seg + 3)) & 0x0F

    return aval ^ mask

def linear2ulaw_sample(pcm_val: int) -> int:
    pcm_val = pcm_val >> 2

    if pcm_val < 0:
        pcm_val = -pcm_val
        mask = 0x7F
    else:
        mask = 0xFF

    if pcm_val > ULAW_CLIP:
        pcm_val = ULAW_CLIP

    pcm_val += (ULAW_BIAS >> 2)

    seg = _search_segment(pcm_val, SEG_UEND)
    if seg >= 8:
        return 0x7F ^ mask

    uval = (seg << 4) | ((pcm_val >> (seg + 1)) & 0x0F)
    return uval ^ mask

def pcm16le_to_alaw(pcm_bytes: bytes) -> bytes:
    out = bytearray(len(pcm_bytes) // 2)
    j = 0
    for i in range(0, len(pcm_bytes), 2):
        sample = struct.unpack_from("<h", pcm_bytes, i)[0]
        out[j] = linear2alaw_sample(sample)
        j += 1
    return bytes(out)

def pcm16le_to_ulaw(pcm_bytes: bytes) -> bytes:
    out = bytearray(len(pcm_bytes) // 2)
    j = 0
    for i in range(0, len(pcm_bytes), 2):
        sample = struct.unpack_from("<h", pcm_bytes, i)[0]
        out[j] = linear2ulaw_sample(sample)
        j += 1
    return bytes(out)

# ──────────────────────────────────────────────
# DECODER THREAD
# file -> PCM16 mono 8k -> G711
# ──────────────────────────────────────────────
def decoder_thread_g711(filepath: str, out_queue: queue.Queue,
                        stop_event: threading.Event, codec: str):
    try:
        print(f"[DECODE] Doc file: {filepath}")
        t0 = time.perf_counter()

        audio = AudioSegment.from_file(filepath)
        audio = audio.set_frame_rate(G711_SAMPLE_RATE)
        audio = audio.set_channels(G711_CHANNELS)
        audio = audio.set_sample_width(G711_SAMPLE_WIDTH)

        dur_ms = len(audio)
        print(f"[DECODE] Xong trong {time.perf_counter()-t0:.1f}s | "
              f"Duration={dur_ms/1000:.1f}s")

        ms = 0
        while ms < dur_ms and not stop_event.is_set():
            end_ms = min(ms + DECODE_CHUNK_MS, dur_ms)
            pcm_chunk = audio[ms:end_ms].raw_data

            if pcm_chunk:
                if codec == "alaw":
                    g711_chunk = pcm16le_to_alaw(pcm_chunk)
                else:
                    g711_chunk = pcm16le_to_ulaw(pcm_chunk)

                out_queue.put(g711_chunk, block=True, timeout=10.0)

            ms = end_ms

        out_queue.put(None)

    except Exception as e:
        print(f"[DECODE ERR] {e}")
        out_queue.put(None)

# ──────────────────────────────────────────────
# STREAM G711 REAL-TIME
# ──────────────────────────────────────────────
def stream_g711_realtime(filepath, esp32_ip, udp_port, chunk_size, codec, loop=False):
    chunk_size = max(1, int(chunk_size))
    interval = chunk_size / G711_BYTES_PER_SEC

    if codec == "alaw":
        pkt_type = PKT_CALL_G711A
    elif codec == "ulaw":
        pkt_type = PKT_CALL_G711U
    else:
        raise ValueError("codec phai la 'alaw' hoac 'ulaw'")

    print("=" * 58)
    print("  G711 UDP REAL-TIME STREAMER")
    print(f"  File     : {filepath}")
    print(f"  Codec    : {codec}")
    print(f"  Dest     : {esp32_ip}:{udp_port}")
    print(f"  Chunk    : {chunk_size}B = {interval*1000:.1f}ms/packet")
    print(f"  Prebuffer: {PREBUFFER_SECONDS*1000:.0f}ms")
    print("=" * 58)

    timer_set = _set_win_timer()

    sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    sock.setsockopt(socket.SOL_SOCKET, socket.SO_SNDBUF, 131072)
    dest = (esp32_ip, udp_port)

    run = 0
    try:
        while True:
            run += 1
            print(f"\n[STREAM] === Lan {run} ===")

            data_queue = queue.Queue(maxsize=QUEUE_MAXSIZE)
            stop_event = threading.Event()

            dec = threading.Thread(
                target=decoder_thread_g711,
                args=(filepath, data_queue, stop_event, codec),
                daemon=True,
                name="DecoderG711"
            )
            dec.start()

            prebuffer_bytes = int(G711_BYTES_PER_SEC * PREBUFFER_SECONDS)
            send_buf = bytearray()
            stream_done = False

            print(f"[STREAM] Prebuffer {prebuffer_bytes} bytes...")
            while len(send_buf) < prebuffer_bytes:
                try:
                    block = data_queue.get(timeout=10.0)
                except queue.Empty:
                    print("[WARN] Timeout khi prebuffer")
                    stream_done = True
                    break

                if block is None:
                    stream_done = True
                    break

                send_buf.extend(block)

            print(f"[STREAM] Bat dau gui UDP ({len(send_buf)} bytes trong buffer)...")

            seq = 0
            sent_bytes = 0
            overruns = 0
            t_start = time.perf_counter()
            next_send_time = t_start
            last_log_time = t_start

            def send_from_buf():
                nonlocal seq, sent_bytes, next_send_time, overruns
                while len(send_buf) >= chunk_size:
                    pkt = bytes(send_buf[:chunk_size])
                    del send_buf[:chunk_size]

                    sock.sendto(build_packet(pkt_type, seq, pkt), dest)
                    seq += 1
                    sent_bytes += len(pkt)

                    next_send_time += interval
                    wait = next_send_time - time.perf_counter()
                    if wait > 0:
                        precise_sleep(wait)
                    elif wait < -0.020:
                        overruns += 1
                        next_send_time = time.perf_counter()

            send_from_buf()

            while not stream_done:
                try:
                    block = data_queue.get(timeout=5.0)
                except queue.Empty:
                    print("[WARN] Queue timeout")
                    break

                if block is None:
                    stream_done = True
                else:
                    send_buf.extend(block)

                send_from_buf()

                now = time.perf_counter()
                if now - last_log_time >= 3.0:
                    last_log_time = now
                    played = sent_bytes / G711_BYTES_PER_SEC
                    print(f"  [{played:.1f}s played] "
                          f"buf={len(send_buf)}B "
                          f"queue={data_queue.qsize()} "
                          f"overrun={overruns}")

            if len(send_buf) > 0:
                sock.sendto(build_packet(pkt_type, seq, bytes(send_buf)), dest)
                seq += 1

            sock.sendto(build_end_packet(pkt_type, seq), dest)

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
    finally:
        try:
            stop_event.set()
        except Exception:
            pass
        sock.close()
        if timer_set:
            _restore_win_timer()

# ──────────────────────────────────────────────
# MAIN
# ──────────────────────────────────────────────
def main():
    parser = argparse.ArgumentParser(
        description="Stream G711 qua UDP den ESP32"
    )
    parser.add_argument("--file", required=True, help="File MP3/WAV/FLAC")
    parser.add_argument("--ip", default=DEFAULT_IP, help=f"IP ESP32 (default: {DEFAULT_IP})")
    parser.add_argument("--port", type=int, default=DEFAULT_PORT, help=f"UDP port (default: {DEFAULT_PORT})")
    parser.add_argument("--codec", choices=["alaw", "ulaw"], default=DEFAULT_CODEC,
                        help="Loai G711: alaw hoac ulaw")
    parser.add_argument("--chunk", type=int, default=DEFAULT_CHUNK,
                        help="Bytes moi UDP packet, vd 160 = 20ms")
    parser.add_argument("--loop", action="store_true", help="Lap lai khi het file")
    args = parser.parse_args()

    if not os.path.isfile(args.file):
        print(f"[LOI] Khong tim thay file: {args.file}")
        sys.exit(1)

    stream_g711_realtime(
        filepath=args.file,
        esp32_ip=args.ip,
        udp_port=args.port,
        chunk_size=args.chunk,
        codec=args.codec,
        loop=args.loop,
    )

if __name__ == "__main__":
    main()