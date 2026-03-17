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

DECODE_CHUNK_MS = 40
QUEUE_MAX_SECONDS = 2
QUEUE_MAXSIZE = int(QUEUE_MAX_SECONDS * 1000 / DECODE_CHUNK_MS) + 8

PCM_PREBUFFER_SECONDS = 1.0
G711_PREBUFFER_SECONDS = 0.5

# Gửi burst lúc đầu để ESP32 tích buffer
STARTUP_BURST_PACKETS_PCM = 80
STARTUP_BURST_PACKETS_G711 = 16

# Giới hạn send_buf để không ăn quá nhiều RAM
SEND_BUF_MAX_BYTES_PCM = 512 * 1024
SEND_BUF_MAX_BYTES_G711 = 128 * 1024

MAGIC = b"\xAB\xCD"

PCM_TX_SPEEDUP = 1.01
G711_TX_SPEEDUP = 1.00

# ──────────────────────────────────────────────
# DEBUG
# ──────────────────────────────────────────────
DBG_SHOW_CODEC_INFO = True
DBG_SHOW_PACKET_HEADER = False
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
# DECODER THREADS
# ──────────────────────────────────────────────
def decoder_pcm_thread(
    filepath: str,
    payload_queue: queue.Queue,
    stop_event: threading.Event,
):
    try:
        cfg = get_pcm_config()
        sample_rate = cfg["sample_rate"]
        channels = cfg["channels"]
        sample_width = cfg["sample_width"]

        print(f"[DECODE PCM] Doc file: {filepath}")
        t0 = time.perf_counter()

        audio = AudioSegment.from_file(filepath)
        audio = audio.set_frame_rate(sample_rate)
        audio = audio.set_channels(channels)
        audio = audio.set_sample_width(sample_width)

        dur_ms = len(audio)
        print(f"[DECODE PCM] Xong trong {time.perf_counter() - t0:.1f}s | Duration={dur_ms/1000:.1f}s")

        ms = 0
        chunk_idx = 0
        while ms < dur_ms and not stop_event.is_set():
            end_ms = min(ms + DECODE_CHUNK_MS, dur_ms)
            pcm_chunk = audio[ms:end_ms].raw_data

            if pcm_chunk:
                payload = encode_pcm(pcm_chunk)
                payload_queue.put(payload, block=True, timeout=10.0)

                chunk_idx += 1
                if chunk_idx <= 3:
                    print(
                        f"[PCM CHUNK {chunk_idx}] "
                        f"src_ms={ms}-{end_ms} "
                        f"pcm_in={len(pcm_chunk)}B "
                        f"payload={len(payload)}B"
                    )

            ms = end_ms

        payload_queue.put(None)

    except Exception as e:
        print(f"[DECODE PCM ERR] {e}")
        payload_queue.put(None)


def decoder_g711_thread(
    filepath: str,
    payload_queue: queue.Queue,
    stop_event: threading.Event,
    law: str,
):
    try:
        cfg = get_g711_config(law)
        sample_rate = cfg["sample_rate"]
        channels = cfg["channels"]
        sample_width = cfg["sample_width"]

        print(f"[DECODE {law.upper()}] Doc file: {filepath}")
        t0 = time.perf_counter()

        audio = AudioSegment.from_file(filepath)
        audio = audio.set_frame_rate(sample_rate)
        audio = audio.set_channels(channels)
        audio = audio.set_sample_width(sample_width)

        dur_ms = len(audio)
        print(f"[DECODE {law.upper()}] Xong trong {time.perf_counter() - t0:.1f}s | Duration={dur_ms/1000:.1f}s")

        ms = 0
        chunk_idx = 0
        while ms < dur_ms and not stop_event.is_set():
            end_ms = min(ms + DECODE_CHUNK_MS, dur_ms)
            pcm_chunk = audio[ms:end_ms].raw_data

            if pcm_chunk:
                if law == "ulaw":
                    payload = encode_ulaw(pcm_chunk)
                else:
                    payload = encode_alaw(pcm_chunk)

                payload_queue.put(payload, block=True, timeout=10.0)

                chunk_idx += 1
                if chunk_idx <= 3:
                    print(
                        f"[{law.upper()} CHUNK {chunk_idx}] "
                        f"src_ms={ms}-{end_ms} "
                        f"pcm_in={len(pcm_chunk)}B "
                        f"payload={len(payload)}B"
                    )

            ms = end_ms

        payload_queue.put(None)

    except Exception as e:
        print(f"[DECODE {law.upper()} ERR] {e}")
        payload_queue.put(None)


# ──────────────────────────────────────────────
# CORE SENDER
# ──────────────────────────────────────────────
def run_stream_loop(
    filepath: str,
    esp32_ip: str,
    udp_port: int,
    chunk_size: int,
    pkt_type: int,
    label: str,
    tx_bytes_per_sec: int,
    default_chunk: int,
    align: int,
    prebuffer_seconds: float,
    decoder_target,
    decoder_args: tuple,
    loop: bool = False,
):
    if chunk_size is None or chunk_size <= 0:
        chunk_size = default_chunk

    chunk_size = (chunk_size // align) * align
    if chunk_size <= 0:
        chunk_size = default_chunk

    if pkt_type == PKT_PCM:
        effective_tx_bps = tx_bytes_per_sec * PCM_TX_SPEEDUP
    else:
        effective_tx_bps = tx_bytes_per_sec * G711_TX_SPEEDUP

    interval = chunk_size / effective_tx_bps

    print("=" * 60)
    print("  UDP REAL-TIME STREAMER")
    print(f"  RUN FILE : {os.path.abspath(__file__)}")
    print(f"  File     : {filepath}")
    print(f"  Dest     : {esp32_ip}:{udp_port}")
    print(f"  Codec    : {label}")
    print(f"  UDP chunk: {chunk_size}B = {interval*1000:.2f}ms/packet")
    print(f"  Prebuffer: {prebuffer_seconds*1000:.0f}ms")
    print(f"  OS       : {'Windows' if os.name == 'nt' else 'Linux/Mac'}")
    print("=" * 60)

    if DBG_SHOW_CODEC_INFO:
        print("[CFG]")
        print(f"  pkt_type        = {pkt_type} ({pkt_type_name(pkt_type)})")
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

    startup_burst_packets = (
        STARTUP_BURST_PACKETS_PCM if pkt_type == PKT_PCM else STARTUP_BURST_PACKETS_G711
    )

    send_buf_max_bytes = (
        SEND_BUF_MAX_BYTES_PCM if pkt_type == PKT_PCM else SEND_BUF_MAX_BYTES_G711
    )

    try:
        while True:
            run += 1
            print(f"\n[STREAM] === Lan {run} ===")

            payload_queue = queue.Queue(maxsize=QUEUE_MAXSIZE)
            stop_event = threading.Event()

            dec = threading.Thread(
                target=decoder_target,
                args=decoder_args + (payload_queue, stop_event),
                daemon=True,
                name="Decoder",
            )
            dec.start()

            prebuffer_bytes = int(tx_bytes_per_sec * prebuffer_seconds)
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

            print(f"[STREAM] Bat dau gui UDP ({len(send_buf)} bytes trong buffer)...")

            seq = 0
            sent_bytes = 0
            overruns = 0
            dbg_sent_count = 0

            t_start = time.perf_counter()
            next_send_time = t_start
            last_log_time = t_start

            # Startup burst: bơm nhanh lúc đầu cho ESP tích buffer
            while len(send_buf) >= chunk_size and seq < startup_burst_packets:
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

            next_send_time = time.perf_counter()

            while True:
                # Chỉ hút thêm dữ liệu nếu send_buf chưa quá lớn
                while len(send_buf) < send_buf_max_bytes:
                    try:
                        pc = payload_queue.get_nowait()
                    except queue.Empty:
                        break

                    if pc is None:
                        decoder_finished = True
                        break

                    send_buf.extend(pc)

                now = time.perf_counter()

                # Gửi đúng nhịp theo clock
                if len(send_buf) >= chunk_size and now >= next_send_time:
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

                    # Nếu trễ quá nhiều thì resync clock
                    lag = time.perf_counter() - next_send_time
                    if lag > 0.020:
                        overruns += 1
                        next_send_time = time.perf_counter() + interval

                    continue

                # Hết decoder và buffer không đủ 1 packet nữa thì thoát
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
# PCM STREAMER
# ──────────────────────────────────────────────
def stream_pcm(filepath, esp32_ip, udp_port, chunk_size, loop=False):
    cfg = get_pcm_config()

    run_stream_loop(
        filepath=filepath,
        esp32_ip=esp32_ip,
        udp_port=udp_port,
        chunk_size=chunk_size,
        pkt_type=cfg["pkt_type"],
        label=cfg["label"],
        tx_bytes_per_sec=cfg["tx_bytes_per_sec"],
        default_chunk=cfg["default_chunk"],
        align=cfg["align"],
        prebuffer_seconds=PCM_PREBUFFER_SECONDS,
        decoder_target=lambda filepath, payload_queue, stop_event: decoder_pcm_thread(
            filepath, payload_queue, stop_event
        ),
        decoder_args=(filepath,),
        loop=loop,
    )


# ──────────────────────────────────────────────
# G711 STREAMER
# ──────────────────────────────────────────────
def stream_g711(filepath, esp32_ip, udp_port, chunk_size, law, loop=False):
    cfg = get_g711_config(law)

    run_stream_loop(
        filepath=filepath,
        esp32_ip=esp32_ip,
        udp_port=udp_port,
        chunk_size=chunk_size,
        pkt_type=cfg["pkt_type"],
        label=cfg["label"],
        tx_bytes_per_sec=cfg["tx_bytes_per_sec"],
        default_chunk=cfg["default_chunk"],
        align=cfg["align"],
        prebuffer_seconds=G711_PREBUFFER_SECONDS,
        decoder_target=lambda filepath, payload_queue, stop_event: decoder_g711_thread(
            filepath, payload_queue, stop_event, law
        ),
        decoder_args=(filepath,),
        loop=loop,
    )


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

    if args.codec == "pcm":
        stream_pcm(
            filepath=args.file,
            esp32_ip=args.ip,
            udp_port=args.port,
            chunk_size=args.chunk,
            loop=args.loop,
        )
    elif args.codec == "ulaw":
        stream_g711(
            filepath=args.file,
            esp32_ip=args.ip,
            udp_port=args.port,
            chunk_size=args.chunk,
            law="ulaw",
            loop=args.loop,
        )
    elif args.codec == "alaw":
        stream_g711(
            filepath=args.file,
            esp32_ip=args.ip,
            udp_port=args.port,
            chunk_size=args.chunk,
            law="alaw",
            loop=args.loop,
        )
    else:
        print(f"[LOI] codec khong hop le: {args.codec}")
        sys.exit(1)


if __name__ == "__main__":
    main()