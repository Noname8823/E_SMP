import socket
import time
import argparse
import struct
import sys

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
DEFAULT_CHUNK_SIZE = 1024          # bytes PCM mỗi packet (phải chẵn, chia hết 4)
SAMPLE_RATE        = 44100         # Hz - phải khớp với ESP32 I2S config
CHANNELS           = 2             # Stereo
SAMPLE_WIDTH       = 2             # 16-bit = 2 bytes/sample
MAGIC              = b'\xAB\xCD'

# ──────────────────────────────────────────────
# PACKET STRUCTURE (8 bytes header)
# [MAGIC 2B][SEQ 4B][LEN 2B][PCM DATA...]
# ──────────────────────────────────────────────
def build_packet(seq: int, data: bytes) -> bytes:
    return MAGIC + struct.pack(">IH", seq, len(data)) + data

def build_end_packet(seq: int) -> bytes:
    return MAGIC + struct.pack(">IH", seq, 0xFFFF)  # 0xFFFF = END signal


def decode_mp3_to_pcm(filepath: str) -> bytes:
    print(f"[DECODE] Dang doc va decode: {filepath}")

    try:
        audio = AudioSegment.from_mp3(filepath)
    except Exception as e:
        print(f"[LOI] Khong decode duoc MP3: {e}")
        sys.exit(1)

    audio = audio.set_frame_rate(SAMPLE_RATE)
    audio = audio.set_channels(CHANNELS)
    audio = audio.set_sample_width(SAMPLE_WIDTH)

    pcm_data = audio.raw_data
    duration = len(audio) / 1000.0

    print(f"[DECODE] Xong!")
    print(f"  Sample rate : {SAMPLE_RATE} Hz")
    print(f"  Channels    : {CHANNELS} (Stereo)")
    print(f"  Bit depth   : {SAMPLE_WIDTH * 8} bit")
    print(f"  Duration    : {duration:.2f}s")
    print(f"  PCM size    : {len(pcm_data):,} bytes ({len(pcm_data)/1024/1024:.2f} MB)")
    return pcm_data


def parse_packet(pkt: bytes):
    if len(pkt) < 8:
        raise ValueError("Packet qua ngan, khong du 8 byte header")

    magic = pkt[0:2]
    seq, data_len = struct.unpack(">IH", pkt[2:8])
    payload = pkt[8:]

    return {
        "magic": magic,
        "seq": seq,
        "len": data_len,
        "payload": payload,
        "payload_len": len(payload),
    }


def verify_packet(seq: int, original_data: bytes, pkt: bytes, verbose=True):
    info = parse_packet(pkt)

    if verbose:
        print(f"[VERIFY] seq mong doi     : {seq}")
        print(f"[VERIFY] seq trong packet : {info['seq']}")
        print(f"[VERIFY] len mong doi     : {len(original_data)}")
        print(f"[VERIFY] len trong packet : {info['len']}")
        print(f"[VERIFY] payload thuc te  : {info['payload_len']}")
        print(f"[VERIFY] header hex       : {pkt[:8].hex(' ')}")

    if info["magic"] != MAGIC:
        if verbose: print("[LOI] MAGIC sai")
        return False

    if info["seq"] != seq:
        if verbose: print("[LOI] SEQ sai")
        return False

    if info["len"] != len(original_data):
        if verbose: print("[LOI] LEN trong header sai")
        return False

    if info["payload"] != original_data:
        if verbose: print("[LOI] Payload bi sai")
        return False

    if verbose:
        print("[OK] Packet dong goi dung")
    return True


def verify_end_packet(seq: int, pkt: bytes, verbose=True):
    info = parse_packet(pkt)

    if verbose:
        print(f"[VERIFY END] seq mong doi     : {seq}")
        print(f"[VERIFY END] seq trong packet : {info['seq']}")
        print(f"[VERIFY END] len trong packet : {info['len']}")
        print(f"[VERIFY END] payload thuc te  : {info['payload_len']}")
        print(f"[VERIFY END] header hex       : {pkt[:8].hex(' ')}")

    if info["magic"] != MAGIC:
        if verbose: print("[LOI] MAGIC sai")
        return False

    if info["seq"] != seq:
        if verbose: print("[LOI] SEQ sai")
        return False

    if info["len"] != 0xFFFF:
        if verbose: print("[LOI] END packet phai co LEN = 0xFFFF")
        return False

    if info["payload_len"] != 0:
        if verbose: print("[LOI] END packet khong duoc co payload")
        return False

    if verbose:
        print("[OK] END packet dung")
    return True


def self_test_basic():
    print("\n================ SELF TEST CO BAN ================\n")

    test_data = bytes([1, 2, 3, 4, 5, 6, 7, 8])
    pkt = build_packet(5, test_data)

    print("[TEST 1] Packet thuong")
    ok1 = verify_packet(5, test_data, pkt)

    print("\nNoi dung full packet hex:")
    print(pkt.hex(' '))

    end_pkt = build_end_packet(99)

    print("\n[TEST 2] END packet")
    ok2 = verify_end_packet(99, end_pkt)

    print("\nNoi dung END packet hex:")
    print(end_pkt.hex(' '))

    if ok1 and ok2:
        print("\n[SELF TEST PASS] Tat ca packet co ban deu dung")
        return True
    else:
        print("\n[SELF TEST FAIL] Co loi trong qua trinh dong goi")
        return False


def verify_pcm_packetization(filepath, chunk_size, max_packets_to_check=10):
    print("\n================ KIEM TRA PACKET HOA TU FILE MP3 ================\n")

    chunk_size = (chunk_size // 4) * 4
    if chunk_size <= 0:
        chunk_size = 1024

    pcm_data = decode_mp3_to_pcm(filepath)
    total_size = len(pcm_data)
    n_chunks = (total_size + chunk_size - 1) // chunk_size

    print(f"[CHECK] Tong so packet du kien: {n_chunks}")
    print(f"[CHECK] Se kiem tra toi da {min(max_packets_to_check, n_chunks)} packet dau\n")

    checked = 0
    all_ok = True
    seq = 0

    for i in range(0, total_size, chunk_size):
        chunk = pcm_data[i:i + chunk_size]
        pkt = build_packet(seq, chunk)

        verbose = checked < 3  # chi in chi tiet 3 packet dau
        ok = verify_packet(seq, chunk, pkt, verbose=verbose)
        if not ok:
            print(f"[FAIL] Packet seq={seq} dong goi sai")
            all_ok = False
            break

        if verbose:
            print()

        checked += 1
        seq += 1

        if checked >= max_packets_to_check:
            break

    if all_ok:
        end_pkt = build_end_packet(seq)
        print("[CHECK] Kiem tra END packet...")
        if not verify_end_packet(seq, end_pkt, verbose=True):
            all_ok = False

    print()
    if all_ok:
        print(f"[PASS] Da kiem tra {checked} packet dau + 1 END packet, tat ca deu dung")
    else:
        print("[FAIL] Packet hoa bi loi")

    return all_ok, pcm_data


def stream_pcm_data_udp(pcm_data, esp32_ip, udp_port, chunk_size, loop=False):
    chunk_size = (chunk_size // 4) * 4
    if chunk_size <= 0:
        chunk_size = 1024

    bytes_per_sec = SAMPLE_RATE * CHANNELS * SAMPLE_WIDTH
    interval = chunk_size / bytes_per_sec

    total_size = len(pcm_data)
    n_chunks = (total_size + chunk_size - 1) // chunk_size

    print("=" * 55)
    print("  PCM UDP STREAMER -> ESP32-C3 W5500 -> PCM5102A")
    print("=" * 55)
    print(f"  Dest     : {esp32_ip}:{udp_port}")
    print(f"  Chunk    : {chunk_size} bytes = {chunk_size//4} samples")
    print(f"  Interval : {interval*1000:.2f} ms/packet")
    print(f"  Bitrate  : {bytes_per_sec/1000:.1f} KB/s PCM")
    print("=" * 55)

    sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    sock.setsockopt(socket.SOL_SOCKET, socket.SO_SNDBUF, 131072)
    dest = (esp32_ip, udp_port)

    try:
        run = 0
        while True:
            run += 1
            print(f"\n[STREAM] Bat dau phat lan {run} ({n_chunks} packets)...")
            seq = 0
            sent_bytes = 0
            t_start = time.perf_counter()

            for i in range(0, total_size, chunk_size):
                chunk = pcm_data[i:i + chunk_size]
                pkt = build_packet(seq, chunk)
                sock.sendto(pkt, dest)

                seq += 1
                sent_bytes += len(chunk)

                expected_time = t_start + (sent_bytes / bytes_per_sec)
                now = time.perf_counter()
                if expected_time > now:
                    time.sleep(expected_time - now)

                if seq % max(1, int(2.0 / interval)) == 0:
                    elapsed = time.perf_counter() - t_start
                    progress = (sent_bytes / total_size) * 100
                    remain = (total_size - sent_bytes) / bytes_per_sec
                    print(f"  [{progress:5.1f}%] {elapsed:.1f}s elapsed | con lai ~{remain:.1f}s")

            sock.sendto(build_end_packet(seq), dest)
            elapsed = time.perf_counter() - t_start
            print(f"\n[DONE] Phat xong trong {elapsed:.2f}s")

            if not loop:
                break
            print("[LOOP] Lap lai sau 1 giay...")
            time.sleep(1.0)

    except KeyboardInterrupt:
        print("\n[STOP] Nguoi dung dung stream.")
    finally:
        sock.close()


def main():
    parser = argparse.ArgumentParser(description="Stream PCM audio qua UDP den ESP32")
    parser.add_argument("--file", required=True, help="File MP3 dau vao")
    parser.add_argument("--ip", default=DEFAULT_IP, help=f"IP ESP32 (mac dinh: {DEFAULT_IP})")
    parser.add_argument("--port", type=int, default=DEFAULT_PORT, help=f"UDP port (mac dinh: {DEFAULT_PORT})")
    parser.add_argument("--chunk", type=int, default=DEFAULT_CHUNK_SIZE, help="Bytes/packet (mac dinh: 1024)")
    parser.add_argument("--loop", action="store_true", help="Lap lai khi het file")
    parser.add_argument("--check-only", action="store_true", help="Chi kiem tra dong goi packet, khong gui UDP")
    parser.add_argument("--check-packets", type=int, default=10, help="So packet dau tien can kiem tra")
    args = parser.parse_args()

    print("=== CHAY SELF TEST CO BAN ===")
    if not self_test_basic():
        sys.exit(1)

    print("\n=== KIEM TRA PACKET HOA TU FILE THAT ===")
    ok, pcm_data = verify_pcm_packetization(
        filepath=args.file,
        chunk_size=args.chunk,
        max_packets_to_check=args.check_packets
    )

    if not ok:
        print("[DUNG] Packet hoa loi, khong gui UDP")
        sys.exit(1)

    if args.check_only:
        print("[XONG] Da kiem tra xong, khong gui UDP vi dang o che do --check-only")
        sys.exit(0)

    print("\n=== BAT DAU STREAM ===")
    stream_pcm_data_udp(
        pcm_data=pcm_data,
        esp32_ip=args.ip,
        udp_port=args.port,
        chunk_size=args.chunk,
        loop=args.loop,
    )

if __name__ == "__main__":
    main()