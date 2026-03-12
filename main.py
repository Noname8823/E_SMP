import socket
import time
import argparse
import os
import struct
import sys

DEFAULT_ESP32_IP   = "192.168.1.100"
DEFAULT_UDP_PORT   = 5000
DEFAULT_CHUNK_SIZE = 1024
DEFAULT_INTERVAL   = 0.01

MAGIC = b'\xAB\xCD'

def build_packet(seq_num: int, data: bytes) -> bytes:
    header = MAGIC + struct.pack(">IH", seq_num, len(data))
    return header + data

def build_end_packet(seq_num: int) -> bytes:
    return MAGIC + struct.pack(">IH", seq_num, 0)

def stream_mp3_udp(filepath, esp32_ip, udp_port, chunk_size, interval, loop=False):
    if not os.path.isfile(filepath):
        print(f"[LOI] Khong tim thay file: {filepath}")
        sys.exit(1)

    if chunk_size <= 0 or chunk_size > 1400:
        print("[LOI] chunk_size nen trong khoang 1..1400")
        sys.exit(1)

    file_size = os.path.getsize(filepath)
    total_chunks = (file_size + chunk_size - 1) // chunk_size

    print(f"File: {filepath}")
    print(f"Size: {file_size} bytes")
    print(f"Dest: {esp32_ip}:{udp_port}")
    print(f"Chunk: {chunk_size}")
    print(f"Interval: {interval}s")
    print(f"Total chunks: {total_chunks}")

    sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    dest = (esp32_ip, udp_port)

    try:
        while True:
            seq = 0
            with open(filepath, "rb") as f:
                while True:
                    chunk = f.read(chunk_size)
                    if not chunk:
                        break

                    sock.sendto(build_packet(seq, chunk), dest)
                    seq += 1
                    time.sleep(interval)

            sock.sendto(build_end_packet(seq), dest)
            print("Gui xong")

            if not loop:
                break

            time.sleep(1.0)

    except KeyboardInterrupt:
        print("Dung theo yeu cau")
    finally:
        sock.close()

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--file", required=True)
    parser.add_argument("--ip", default=DEFAULT_ESP32_IP)
    parser.add_argument("--port", type=int, default=DEFAULT_UDP_PORT)
    parser.add_argument("--chunk", type=int, default=DEFAULT_CHUNK_SIZE)
    parser.add_argument("--interval", type=float, default=DEFAULT_INTERVAL)
    parser.add_argument("--loop", action="store_true")
    args = parser.parse_args()

    stream_mp3_udp(
        filepath=args.file,
        esp32_ip=args.ip,
        udp_port=args.port,
        chunk_size=args.chunk,
        interval=args.interval,
        loop=args.loop,
    )

if __name__ == "__main__":
    main()