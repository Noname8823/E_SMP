import socket
import time
import argparse
import struct
import sys
import os
import threading
import queue
import wave
import array

# =====================================================
# PACKET / PROTOCOL
# =====================================================
PKT_PCM = 1
MAGIC = b"\xAB\xCD"

# =====================================================
# DEFAULT NETWORK
# =====================================================
DEFAULT_IP = "192.168.1.100"
DEFAULT_PORT = 5000

# =====================================================
# PCM CONFIG - PHAI KHOP ESP32
# WAV PCM 44.1kHz stereo 16-bit
# =====================================================
PCM_SAMPLE_RATE = 44100
PCM_CHANNELS = 2
PCM_SAMPLE_WIDTH = 2  # bytes = 16-bit
PCM_BYTES_PER_SEC = PCM_SAMPLE_RATE * PCM_CHANNELS * PCM_SAMPLE_WIDTH

# =====================================================
# STREAM TUNING
# =====================================================
DECODE_CHUNK_MS = 40
QUEUE_MAX_SECONDS = 2
QUEUE_MAXSIZE = int(QUEUE_MAX_SECONDS * 1000 / DECODE_CHUNK_MS) + 8

PCM_PREBUFFER_SECONDS = 1.0

# Giam de tranh dồn packet luc bat dau
STARTUP_BURST_PACKETS_PCM = 2

# Gioi han send buffer noi bo
SEND_BUF_MAX_BYTES_PCM = 128 * 1024

# Toc do gui. 1.0 = dung toc do ly thuyet
PCM_TX_SPEEDUP = 1.0

# Sleep tinh hơn
SPIN_THRESHOLD = 0.002


# =====================================================
# TIMER HELPERS
# =====================================================
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


# =====================================================
# PACKET BUILDERS
# MAGIC(2) + SEQ(4) + TYPE(1) + LEN(2) + PAYLOAD
# =====================================================
def build_packet(seq: int, pkt_type: int, data: bytes) -> bytes:
    return MAGIC + struct.pack(">IBH", seq, pkt_type, len(data)) + data


def build_end_packet(seq: int, pkt_type: int) -> bytes:
    return MAGIC + struct.pack(">IBH", seq, pkt_type, 0xFFFF)


# =====================================================
# PCM HELPERS
# =====================================================
def scale_pcm_16le_stereo(pcm_chunk: bytes, volume: float) -> bytes:
    """
    Giam/tang bien do PCM 16-bit little-endian.
    volume = 1.0 -> giu nguyen
    volume = 0.5 -> giam 50%
    """
    if volume >= 0.9999:
        return pcm_chunk

    samples = array.array("h")
    samples.frombytes(pcm_chunk)

    # Nếu máy big-endian thì cần byteswap; đa số PC là little-endian
    if sys.byteorder != "little":
        samples.byteswap()

    for i in range(len(samples)):
        v = int(samples[i] * volume)
        if v > 32767:
            v = 32767
        elif v < -32768:
            v = -32768
        samples[i] = v

    if sys.byteorder != "little":
        samples.byteswap()

    return samples.tobytes()


def encode_pcm(pcm_chunk: bytes) -> bytes:
    """
    PCM thô: không đổi byte, không đổi endian, không đổi channel.
    """
    return pcm_chunk


# =====================================================
# DECODER THREAD - DOC THANG WAV PCM BANG wave
# =====================================================
def wav_pcm_reader_thread(filepath, payload_queue, stop_event, volume: float):
    try:
        with wave.open(filepath, "rb") as wf:
            channels = wf.getnchannels()
            sample_width = wf.getsampwidth()
            sample_rate = wf.getframerate()
            comptype = wf.getcomptype()

            print(
                f"[WAV] channels={channels}, sample_width={sample_width}, "
                f"sample_rate={sample_rate}, comptype={comptype}"
            )

            if channels != PCM_CHANNELS:
                raise ValueError(
                    f"WAV channels phai = {PCM_CHANNELS}, hien tai = {channels}"
                )
            if sample_width != PCM_SAMPLE_WIDTH:
                raise ValueError(
                    f"WAV sample_width phai = {PCM_SAMPLE_WIDTH}, hien tai = {sample_width}"
                )
            if sample_rate != PCM_SAMPLE_RATE:
                raise ValueError(
                    f"WAV sample_rate phai = {PCM_SAMPLE_RATE}, hien tai = {sample_rate}"
                )
            if comptype != "NONE":
                raise ValueError("WAV phai la PCM khong nen (comptype=NONE)")

            frames_per_chunk = int(sample_rate * DECODE_CHUNK_MS / 1000)
            total_frames = wf.getnframes()
            duration_sec = total_frames / sample_rate
            print(f"[WAV] total_frames={total_frames}, duration={duration_sec:.2f}s")
            print(f"[WAV] frames_per_chunk={frames_per_chunk}")

            chunk_idx = 0
            while not stop_event.is_set():
                pcm_chunk = wf.readframes(frames_per_chunk)
                if not pcm_chunk:
                    break

                # Test clipping: giam volume neu can
                pcm_chunk = scale_pcm_16le_stereo(pcm_chunk, volume)

                payload = encode_pcm(pcm_chunk)
                payload_queue.put(payload, block=True, timeout=10.0)

                chunk_idx += 1
                if chunk_idx <= 3:
                    print(
                        f"[PCM CHUNK {chunk_idx}] payload={len(payload)}B "
                        f"(volume={volume:.2f})"
                    )

        payload_queue.put(None)

    except Exception as e:
        print(f"[WAV PCM ERR] {e}")
        payload_queue.put(None)


# =====================================================
# CORE STREAM
# =====================================================
def run_stream(filepath, esp32_ip, udp_port, chunk_size, volume):
    align = 4  # stereo 16-bit = 4 bytes/frame

    if chunk_size <= 0:
        chunk_size = 256

    chunk_size = (chunk_size // align) * align
    if chunk_size <= 0:
        chunk_size = 256

    effective_tx_bps = PCM_BYTES_PER_SEC * PCM_TX_SPEEDUP
    interval = chunk_size / effective_tx_bps

    print("=" * 60)
    print("UDP PCM WAV STREAMER")
    print(f"File       : {filepath}")
    print(f"Dest       : {esp32_ip}:{udp_port}")
    print(f"Chunk      : {chunk_size} B")
    print(f"Interval   : {interval*1000:.2f} ms")
    print(f"Prebuffer  : {PCM_PREBUFFER_SECONDS:.2f} s")
    print(f"Volume     : {volume:.2f}")
    print(f"PCM Bps    : {PCM_BYTES_PER_SEC}")
    print("=" * 60)

    timer_set = _set_win_timer()

    sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    sock.setsockopt(socket.SOL_SOCKET, socket.SO_SNDBUF, 65536)
    dest = (esp32_ip, udp_port)

    payload_queue = queue.Queue(maxsize=QUEUE_MAXSIZE)
    stop_event = threading.Event()

    t = threading.Thread(
        target=wav_pcm_reader_thread,
        args=(filepath, payload_queue, stop_event, volume),
        daemon=True,
    )
    t.start()

    prebuffer_bytes = int(PCM_BYTES_PER_SEC * PCM_PREBUFFER_SECONDS)
    send_buf = bytearray()
    decoder_finished = False

    print(f"[STREAM] Prebuffer {prebuffer_bytes} bytes...")
    while len(send_buf) < prebuffer_bytes and not decoder_finished:
        try:
            pc = payload_queue.get(timeout=10.0)
        except queue.Empty:
            print("[WARN] Timeout khi prebuffer")
            break

        if pc is None:
            decoder_finished = True
            break

        send_buf.extend(pc)

    print(f"[STREAM] Start send ({len(send_buf)} bytes buffered)")

    seq = 0
    sent_bytes = 0
    overruns = 0
    next_send_time = time.perf_counter()
    last_log_time = next_send_time

    # Startup burst: bom it thoi, tranh don qua manh
    while len(send_buf) >= chunk_size and seq < STARTUP_BURST_PACKETS_PCM:
        pkt_payload = bytes(send_buf[:chunk_size])
        del send_buf[:chunk_size]

        sock.sendto(build_packet(seq, PKT_PCM, pkt_payload), dest)
        seq += 1
        sent_bytes += len(pkt_payload)

    next_send_time = time.perf_counter()

    try:
        while True:
            while len(send_buf) < SEND_BUF_MAX_BYTES_PCM:
                try:
                    pc = payload_queue.get_nowait()
                except queue.Empty:
                    break

                if pc is None:
                    decoder_finished = True
                    break

                send_buf.extend(pc)

            now = time.perf_counter()

            if len(send_buf) >= chunk_size and now >= next_send_time:
                pkt_payload = bytes(send_buf[:chunk_size])
                del send_buf[:chunk_size]

                sock.sendto(build_packet(seq, PKT_PCM, pkt_payload), dest)
                seq += 1
                sent_bytes += len(pkt_payload)

                next_send_time += interval

                # Neu tre nhieu qua thi resync nhe
                lag = time.perf_counter() - next_send_time
                if lag > 0.020:
                    overruns += 1
                    next_send_time = time.perf_counter() + interval

                continue

            if decoder_finished and len(send_buf) < chunk_size:
                break

            wait = next_send_time - time.perf_counter()
            if wait > 0.001:
                precise_sleep(min(wait, 0.002))
            else:
                time.sleep(0)

            now = time.perf_counter()
            if now - last_log_time >= 3.0:
                last_log_time = now
                played = sent_bytes / effective_tx_bps
                print(
                    f"[{played:.0f}s] "
                    f"buf={len(send_buf)}B "
                    f"queue={payload_queue.qsize()} "
                    f"overrun={overruns} "
                    f"last_seq={seq-1}"
                )

        remainder = (len(send_buf) // align) * align
        if remainder > 0:
            tail = bytes(send_buf[:remainder])
            sock.sendto(build_packet(seq, PKT_PCM, tail), dest)
            seq += 1
            sent_bytes += len(tail)

        sock.sendto(build_end_packet(seq, PKT_PCM), dest)
        print(f"[DONE] stream finished | packets={seq} | overruns={overruns}")

    finally:
        stop_event.set()
        sock.close()
        if timer_set:
            _restore_win_timer()


# =====================================================
# MAIN
# =====================================================
def main():
    parser = argparse.ArgumentParser(
        description="UDP streamer cho WAV PCM 44.1kHz stereo 16-bit"
    )
    parser.add_argument("--file", required=True, help="File WAV PCM 44.1kHz stereo 16-bit")
    parser.add_argument("--ip", default=DEFAULT_IP, help=f"IP ESP32 (default: {DEFAULT_IP})")
    parser.add_argument("--port", type=int, default=DEFAULT_PORT, help=f"UDP port (default: {DEFAULT_PORT})")
    parser.add_argument("--chunk", type=int, default=256, help="Bytes moi UDP packet")
    parser.add_argument(
        "--volume",
        type=float,
        default=1.0,
        help="He so am luong PCM truoc khi gui. Vi du 1.0, 0.8, 0.5",
    )
    args = parser.parse_args()

    if not os.path.isfile(args.file):
        print(f"[LOI] Khong tim thay file: {args.file}")
        sys.exit(1)

    if args.volume <= 0:
        print("[LOI] --volume phai > 0")
        sys.exit(1)

    run_stream(args.file, args.ip, args.port, args.chunk, args.volume)


if __name__ == "__main__":
    main()