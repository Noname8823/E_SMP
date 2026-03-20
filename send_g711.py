import socket
import time
import struct
import wave
import audioop
import argparse

MAGIC = b"\xAB\xCD"
PKT_G711_ULAW = 2

DEFAULT_IP = "192.168.1.100"
DEFAULT_PORT = 5000

SAMPLE_RATE = 8000
CHANNELS = 1
SAMPLE_WIDTH = 2  # 16-bit PCM
FRAME_MS = 20     # 20 ms/frame
PCM_SAMPLES_PER_FRAME = SAMPLE_RATE * FRAME_MS // 1000   # 160 samples
PCM_BYTES_PER_FRAME = PCM_SAMPLES_PER_FRAME * SAMPLE_WIDTH * CHANNELS  # 320 bytes
G711_BYTES_PER_FRAME = PCM_SAMPLES_PER_FRAME  # 160 bytes, vì 1 sample -> 1 byte G711


def build_packet(seq: int, payload: bytes) -> bytes:
    # Header:
    # 2B magic + 1B type + 2B seq + 2B payload_len
    return MAGIC + struct.pack("!BHH", PKT_G711_ULAW, seq & 0xFFFF, len(payload)) + payload


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--ip", default=DEFAULT_IP, help="ESP32 IP")
    parser.add_argument("--port", type=int, default=DEFAULT_PORT, help="UDP port")
    parser.add_argument("--wav", required=True, help="WAV file PCM 16-bit mono 8kHz")
    args = parser.parse_args()

    sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)

    with wave.open(args.wav, "rb") as wf:
        nch = wf.getnchannels()
        sw = wf.getsampwidth()
        fr = wf.getframerate()

        print(f"WAV info: channels={nch}, sample_width={sw}, rate={fr}")

        if nch != 1:
            raise ValueError("Chỉ hỗ trợ WAV mono.")
        if sw != 2:
            raise ValueError("Chỉ hỗ trợ WAV 16-bit PCM.")
        if fr != 8000:
            raise ValueError("Chỉ hỗ trợ WAV 8kHz.")

        seq = 0
        frame_time = FRAME_MS / 1000.0
        next_send = time.perf_counter()

        while True:
            pcm = wf.readframes(PCM_SAMPLES_PER_FRAME)
            if not pcm:
                print("Gửi xong file.")
                break

            if len(pcm) < PCM_BYTES_PER_FRAME:
                pcm += b"\x00" * (PCM_BYTES_PER_FRAME - len(pcm))

            # PCM16 -> G711 μ-law
            g711 = audioop.lin2ulaw(pcm, 2)

            pkt = build_packet(seq, g711)
            sock.sendto(pkt, (args.ip, args.port))

            print(f"Sent seq={seq}, payload={len(g711)} bytes")
            seq += 1

            next_send += frame_time
            now = time.perf_counter()
            sleep_time = next_send - now
            if sleep_time > 0:
                time.sleep(sleep_time)
            else:
                next_send = now


if __name__ == "__main__":
    main()