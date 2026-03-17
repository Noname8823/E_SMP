import socket
import time
import argparse
import struct
import sys
import os
import threading
import queue
import wave

PKT_PCM = 1
MAGIC = b"\xAB\xCD"

DEFAULT_IP = "192.168.1.100"
DEFAULT_PORT = 5000

PCM_SAMPLE_RATE = 44100
PCM_CHANNELS = 2
PCM_SAMPLE_WIDTH = 2
PCM_BYTES_PER_SEC = PCM_SAMPLE_RATE * PCM_CHANNELS * PCM_SAMPLE_WIDTH

DECODE_CHUNK_MS = 40
QUEUE_MAX_SECONDS = 2
QUEUE_MAXSIZE = int(QUEUE_MAX_SECONDS * 1000 / DECODE_CHUNK_MS) + 8

PCM_PREBUFFER_SECONDS = 1.0
STARTUP_BURST_PACKETS_PCM = 48
SEND_BUF_MAX_BYTES_PCM = 512 * 1024
PCM_TX_SPEEDUP = 1.0

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


def build_packet(seq: int, pkt_type: int, data: bytes) -> bytes:
    return MAGIC + struct.pack(">IBH", seq, pkt_type, len(data)) + data


def build_end_packet(seq: int, pkt_type: int) -> bytes:
    return MAGIC + struct.pack(">IBH", seq, pkt_type, 0xFFFF)


def wav_pcm_reader_thread(filepath, payload_queue, stop_event):
    try:
        with wave.open(filepath, "rb") as wf:
            channels = wf.getnchannels()
            sample_width = wf.getsampwidth()
            sample_rate = wf.getframerate()
            comptype = wf.getcomptype()

            print(f"[WAV] channels={channels}, sample_width={sample_width}, sample_rate={sample_rate}, comptype={comptype}")

            if channels != PCM_CHANNELS:
                raise ValueError(f"WAV channels phai = {PCM_CHANNELS}, hien tai = {channels}")
            if sample_width != PCM_SAMPLE_WIDTH:
                raise ValueError(f"WAV sample_width phai = {PCM_SAMPLE_WIDTH}, hien tai = {sample_width}")
            if sample_rate != PCM_SAMPLE_RATE:
                raise ValueError(f"WAV sample_rate phai = {PCM_SAMPLE_RATE}, hien tai = {sample_rate}")
            if comptype != "NONE":
                raise ValueError("WAV phai la PCM khong nen (comptype=NONE)")

            frames_per_chunk = int(sample_rate * DECODE_CHUNK_MS / 1000)

            chunk_idx = 0
            while not stop_event.is_set():
                pcm_chunk = wf.readframes(frames_per_chunk)
                if not pcm_chunk:
                    break

                payload_queue.put(pcm_chunk, block=True, timeout=10.0)

                chunk_idx += 1
                if chunk_idx <= 3:
                    print(f"[PCM CHUNK {chunk_idx}] payload={len(pcm_chunk)}B")

        payload_queue.put(None)

    except Exception as e:
        print(f"[WAV PCM ERR] {e}")
        payload_queue.put(None)


def run_stream(filepath, esp32_ip, udp_port, chunk_size):
    align = 4
    if chunk_size <= 0:
        chunk_size = 1460

    chunk_size = (chunk_size // align) * align
    effective_tx_bps = PCM_BYTES_PER_SEC * PCM_TX_SPEEDUP
    interval = chunk_size / effective_tx_bps

    print("=" * 60)
    print("UDP PCM WAV STREAMER")
    print(f"File     : {filepath}")
    print(f"Dest     : {esp32_ip}:{udp_port}")
    print(f"Chunk    : {chunk_size} B")
    print(f"Interval : {interval*1000:.2f} ms")
    print("=" * 60)

    sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    sock.setsockopt(socket.SOL_SOCKET, socket.SO_SNDBUF, 131072)
    dest = (esp32_ip, udp_port)

    payload_queue = queue.Queue(maxsize=QUEUE_MAXSIZE)
    stop_event = threading.Event()

    t = threading.Thread(
        target=wav_pcm_reader_thread,
        args=(filepath, payload_queue, stop_event),
        daemon=True,
    )
    t.start()

    prebuffer_bytes = int(PCM_BYTES_PER_SEC * PCM_PREBUFFER_SECONDS)
    send_buf = bytearray()
    decoder_finished = False

    print(f"[STREAM] Prebuffer {prebuffer_bytes} bytes...")
    while len(send_buf) < prebuffer_bytes and not decoder_finished:
        pc = payload_queue.get(timeout=10.0)
        if pc is None:
            decoder_finished = True
            break
        send_buf.extend(pc)

    print(f"[STREAM] Start send ({len(send_buf)} bytes buffered)")

    seq = 0
    sent_bytes = 0
    next_send_time = time.perf_counter()
    last_log_time = next_send_time

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
                print(f"[{played:.0f}s] buf={len(send_buf)}B queue={payload_queue.qsize()} last_seq={seq-1}")

        remainder = (len(send_buf) // align) * align
        if remainder > 0:
            tail = bytes(send_buf[:remainder])
            sock.sendto(build_packet(seq, PKT_PCM, tail), dest)
            seq += 1

        sock.sendto(build_end_packet(seq, PKT_PCM), dest)
        print("[DONE] stream finished")

    finally:
        stop_event.set()
        sock.close()


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--file", required=True, help="WAV PCM 44.1kHz stereo 16-bit")
    parser.add_argument("--ip", default=DEFAULT_IP)
    parser.add_argument("--port", type=int, default=DEFAULT_PORT)
    parser.add_argument("--chunk", type=int, default=1460)
    args = parser.parse_args()

    if not os.path.isfile(args.file):
        print("[LOI] Khong tim thay file")
        sys.exit(1)

    run_stream(args.file, args.ip, args.port, args.chunk)


if __name__ == "__main__":
    main()