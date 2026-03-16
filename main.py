# main.py

import socket
import time
import argparse
import struct
import sys
import os
import threading
import queue

from pydub import AudioSegment

from pcm_codec import PKT_PCM, encode_pcm, get_pcm_config
from g711_codec import (
    PKT_CALL_G711U,
    PKT_CALL_G711A,
    encode_ulaw,
    encode_alaw,
    get_g711_config,
)

# ──────────────────────────────────────────────
# CẤU HÌNH CHUNG
# ──────────────────────────────────────────────
DEFAULT_IP = "192.168.1.100"
DEFAULT_PORT = 5000
DEFAULT_PCM_CHUNK_SIZE = 1000
DECODE_CHUNK_MS = 200
QUEUE_MAX_SECONDS = 2
QUEUE_MAXSIZE = int(QUEUE_MAX_SECONDS * 1000 / DECODE_CHUNK_MS) + 4
PREBUFFER_SECONDS = 0.5

MAGIC = b"\xAB\xCD"

# ──────────────────────────────────────────────
# DEBUG PYTHON LOG
# ──────────────────────────────────────────────
DBG_SHOW_CODEC_INFO = True
DBG_SHOW_PACKET_HEADER = True
DBG_PACKET_HEADER_COUNT = 10
DBG_SHOW_END_PACKET = True

# ──────────────────────────────────────────────
# WINDOWS TIMER RESOLUTION FIX
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


# ──────────────────────────────────────────────
# PRECISE SLEEP
# ──────────────────────────────────────────────
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
# DEBUG HELPERS
# ──────────────────────────────────────────────
def hex_bytes(data: bytes, max_len: int = None) -> str:
    if max_len is not None:
        data = data[:max_len]
    return " ".join(f"{b:02X}" for b in data)


def pkt_type_name(pkt_type: int) -> str:
    if pkt_type == PKT_PCM:
        return "PCM"
    if pkt_type == PKT_CALL_G711U:
        return "G711U"
    if pkt_type == PKT_CALL_G711A:
        return "G711A"
    return f"UNKNOWN({pkt_type})"


def print_packet_debug(tag: str, packet: bytes, seq: int, pkt_type: int, payload_len: int):
    header = packet[:9]
    print(
        f"[PY {tag}] seq={seq} type={pkt_type} ({pkt_type_name(pkt_type)}) "
        f"len={payload_len} | hdr={hex_bytes(header)}"
    )


# ──────────────────────────────────────────────
# PACKET BUILDERS
# FORMAT: MAGIC(2) + SEQ(4) + TYPE(1) + LEN(2) + PAYLOAD
# ──────────────────────────────────────────────
def build_packet(seq: int, pkt_type: int, data: bytes) -> bytes:
    return MAGIC + struct.pack(">IBH", seq, pkt_type, len(data)) + data


def build_end_packet(seq: int, pkt_type: int) -> bytes:
    return MAGIC + struct.pack(">IBH", seq, pkt_type, 0xFFFF)


# ──────────────────────────────────────────────
# CODEC DISPATCH
# ──────────────────────────────────────────────
def get_codec_config(codec: str) -> dict:
    codec = codec.lower()

    if codec == "pcm":
        return get_pcm_config()

    if codec in ("ulaw", "alaw"):
        return get_g711_config(codec)

    raise ValueError("codec phải là pcm, ulaw hoặc alaw")


def encode_audio_chunk(codec: str, pcm_chunk: bytes) -> bytes:
    codec = codec.lower()

    if codec == "pcm":
        return encode_pcm(pcm_chunk)
    if codec == "ulaw":
        return encode_ulaw(pcm_chunk)
    if codec == "alaw":
        return encode_alaw(pcm_chunk)

    raise ValueError(f"codec không hợp lệ: {codec}")


# ──────────────────────────────────────────────
# DECODER THREAD
# ──────────────────────────────────────────────
def decoder_thread(
    filepath: str,
    payload_queue: queue.Queue,
    stop_event: threading.Event,
    codec: str,
    sample_rate: int,
    channels: int,
    sample_width: int,
):
    try:
        print(f"[DECODE] Doc file: {filepath}")
        t0 = time.perf_counter()

        audio = AudioSegment.from_file(filepath)
        audio = audio.set_frame_rate(sample_rate)
        audio = audio.set_channels(channels)
        audio = audio.set_sample_width(sample_width)

        dur_ms = len(audio)
        print(f"[DECODE] Xong trong {time.perf_counter() - t0:.1f}s | Duration={dur_ms/1000:.1f}s")

        ms = 0
        chunk_idx = 0
        while ms < dur_ms and not stop_event.is_set():
            end_ms = min(ms + DECODE_CHUNK_MS, dur_ms)
            pcm_chunk = audio[ms:end_ms].raw_data

            if pcm_chunk:
                payload = encode_audio_chunk(codec, pcm_chunk)
                payload_queue.put(payload, block=True, timeout=10.0)

                chunk_idx += 1
                if chunk_idx <= 3:
                    print(
                        f"[DECODE CHUNK {chunk_idx}] "
                        f"src_ms={ms}-{end_ms} "
                        f"pcm_in={len(pcm_chunk)}B "
                        f"encoded_out={len(payload)}B"
                    )

            ms = end_ms

        payload_queue.put(None)

    except Exception as e:
        print(f"[DECODE ERR] {e}")
        payload_queue.put(None)


# ──────────────────────────────────────────────
# STREAM REAL-TIME
# ──────────────────────────────────────────────
def stream_realtime(filepath, esp32_ip, udp_port, chunk_size, codec="pcm", loop=False):
    cfg = get_codec_config(codec)

    sample_rate = cfg["sample_rate"]
    channels = cfg["channels"]
    sample_width = cfg["sample_width"]
    tx_bytes_per_sec = cfg["tx_bytes_per_sec"]
    pkt_type = cfg["pkt_type"]
    default_chunk = cfg["default_chunk"]
    align = cfg["align"]
    label = cfg["label"]

    if chunk_size is None or chunk_size <= 0:
        chunk_size = default_chunk

    chunk_size = (chunk_size // align) * align
    if chunk_size <= 0:
        chunk_size = default_chunk

    interval = chunk_size / tx_bytes_per_sec

    print("=" * 60)
    print("  UDP REAL-TIME STREAMER")
    print(f"  RUN FILE : {os.path.abspath(__file__)}")
    print(f"  File     : {filepath}")
    print(f"  Dest     : {esp32_ip}:{udp_port}")
    print(f"  Codec    : {label}")
    print(f"  UDP chunk: {chunk_size}B = {interval*1000:.1f}ms/packet")
    print(f"  Prebuffer: {PREBUFFER_SECONDS*1000:.0f}ms")
    print(f"  OS       : {'Windows' if os.name == 'nt' else 'Linux/Mac'}")
    print("=" * 60)

    if DBG_SHOW_CODEC_INFO:
        print("[CFG]")
        print(f"  pkt_type        = {pkt_type} ({pkt_type_name(pkt_type)})")
        print(f"  sample_rate     = {sample_rate}")
        print(f"  channels        = {channels}")
        print(f"  sample_width    = {sample_width}")
        print(f"  tx_bytes_per_sec= {tx_bytes_per_sec}")
        print(f"  default_chunk   = {default_chunk}")
        print(f"  align           = {align}")
        print("  packet_format   = MAGIC(2) + SEQ(4) + TYPE(1) + LEN(2) + PAYLOAD")

    timer_set = _set_win_timer()

    sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    sock.setsockopt(socket.SOL_SOCKET, socket.SO_SNDBUF, 131072)
    dest = (esp32_ip, udp_port)

    run = 0
    stop_event = None

    try:
        while True:
            run += 1
            print(f"\n[STREAM] === Lan {run} ===")

            payload_queue = queue.Queue(maxsize=QUEUE_MAXSIZE)
            stop_event = threading.Event()

            dec = threading.Thread(
                target=decoder_thread,
                args=(
                    filepath,
                    payload_queue,
                    stop_event,
                    codec,
                    sample_rate,
                    channels,
                    sample_width,
                ),
                daemon=True,
                name="Decoder",
            )
            dec.start()

            prebuffer_bytes = int(tx_bytes_per_sec * PREBUFFER_SECONDS)
            send_buf = bytearray()
            stream_done = False

            print(f"[STREAM] Prebuffer {prebuffer_bytes} bytes...")
            while len(send_buf) < prebuffer_bytes:
                try:
                    pc = payload_queue.get(timeout=10.0)
                except queue.Empty:
                    print("[WARN] Timeout khi prebuffer")
                    stream_done = True
                    break

                if pc is None:
                    stream_done = True
                    break

                send_buf.extend(pc)

            print(f"[STREAM] Bat dau gui UDP ({len(send_buf)} bytes trong buffer)...")

            seq = 0
            sent_bytes = 0
            overruns = 0
            dbg_sent_count = 0

            t_start = time.perf_counter()
            next_send_time = t_start
            last_log_time = t_start

            def send_from_buf():
                nonlocal seq, sent_bytes, next_send_time, overruns, dbg_sent_count

                while len(send_buf) >= chunk_size:
                    pkt_payload = bytes(send_buf[:chunk_size])
                    del send_buf[:chunk_size]

                    packet = build_packet(seq, pkt_type, pkt_payload)

                    if DBG_SHOW_PACKET_HEADER and dbg_sent_count < DBG_PACKET_HEADER_COUNT:
                        print_packet_debug(
                            tag=f"TX {dbg_sent_count + 1}",
                            packet=packet,
                            seq=seq,
                            pkt_type=pkt_type,
                            payload_len=len(pkt_payload),
                        )
                        dbg_sent_count += 1

                    sock.sendto(packet, dest)
                    seq += 1
                    sent_bytes += len(pkt_payload)

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
                    pc = payload_queue.get(timeout=5.0)
                except queue.Empty:
                    print("[WARN] Queue timeout")
                    break

                if pc is None:
                    stream_done = True
                else:
                    send_buf.extend(pc)

                send_from_buf()

                now = time.perf_counter()
                if now - last_log_time >= 3.0:
                    last_log_time = now
                    played = sent_bytes / tx_bytes_per_sec
                    print(
                        f"  [{played:.0f}s played] "
                        f"buf={len(send_buf)}B "
                        f"queue={payload_queue.qsize()} "
                        f"overrun={overruns} "
                        f"last_seq={seq - 1}"
                    )

            remainder = (len(send_buf) // align) * align
            if remainder > 0:
                tail = bytes(send_buf[:remainder])
                packet = build_packet(seq, pkt_type, tail)

                if DBG_SHOW_PACKET_HEADER and dbg_sent_count < DBG_PACKET_HEADER_COUNT:
                    print_packet_debug(
                        tag=f"TX {dbg_sent_count + 1}",
                        packet=packet,
                        seq=seq,
                        pkt_type=pkt_type,
                        payload_len=len(tail),
                    )
                    dbg_sent_count += 1

                sock.sendto(packet, dest)
                seq += 1
                sent_bytes += len(tail)

            end_packet = build_end_packet(seq, pkt_type)
            if DBG_SHOW_END_PACKET:
                print_packet_debug(
                    tag="END",
                    packet=end_packet,
                    seq=seq,
                    pkt_type=pkt_type,
                    payload_len=0xFFFF,
                )

            sock.sendto(end_packet, dest)

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
        if stop_event is not None:
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
        description="Stream audio qua UDP den ESP32 (PCM / G.711)"
    )
    parser.add_argument("--file", required=True, help="File MP3/WAV/FLAC")
    parser.add_argument("--ip", default=DEFAULT_IP, help=f"IP ESP32 (default: {DEFAULT_IP})")
    parser.add_argument("--port", type=int, default=DEFAULT_PORT, help=f"UDP port (default: {DEFAULT_PORT})")
    parser.add_argument("--chunk", type=int, default=0, help="Bytes/UDP packet. 0 = auto theo codec")
    parser.add_argument("--codec", choices=["pcm", "ulaw", "alaw"], default="pcm", help="Loai stream gui di")
    parser.add_argument("--loop", action="store_true", help="Lap lai khi het file")
    args = parser.parse_args()

    if not os.path.isfile(args.file):
        print(f"[LOI] Khong tim thay file: {args.file}")
        sys.exit(1)

    print(f"[MAIN] codec arg = {args.codec}")

    stream_realtime(
        filepath=args.file,
        esp32_ip=args.ip,
        udp_port=args.port,
        chunk_size=args.chunk,
        codec=args.codec,
        loop=args.loop,
    )


if __name__ == "__main__":
    main()