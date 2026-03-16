# pcm_codec.py

PKT_PCM = 1

def encode_pcm(pcm_chunk: bytes) -> bytes:
    # PCM gửi nguyên dữ liệu
    return pcm_chunk

def get_pcm_config() -> dict:
    sample_rate = 44100
    channels = 2
    sample_width = 2   # 16-bit

    tx_bytes_per_sec = sample_rate * channels * sample_width

    return {
        "sample_rate": sample_rate,
        "channels": channels,
        "sample_width": sample_width,
        "tx_bytes_per_sec": tx_bytes_per_sec,
        "pkt_type": PKT_PCM,
        "default_chunk": 1000,
        "align": 4,   # stereo 16-bit = 4 byte / frame
        "label": "PCM 44.1kHz stereo 16-bit",
    }