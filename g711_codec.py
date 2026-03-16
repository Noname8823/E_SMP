# g711_codec.py

import audioop

PKT_CALL_G711U = 2
PKT_CALL_G711A = 3

def encode_ulaw(pcm_chunk: bytes) -> bytes:
    # pcm_chunk là PCM 16-bit little-endian
    return audioop.lin2ulaw(pcm_chunk, 2)

def encode_alaw(pcm_chunk: bytes) -> bytes:
    return audioop.lin2alaw(pcm_chunk, 2)

def get_g711_config(codec: str) -> dict:
    codec = codec.lower()

    if codec == "ulaw":
        pkt_type = PKT_CALL_G711U
        label = "G711 u-law 8kHz mono"
    elif codec == "alaw":
        pkt_type = PKT_CALL_G711A
        label = "G711 a-law 8kHz mono"
    else:
        raise ValueError("codec phải là ulaw hoặc alaw")

    sample_rate = 8000
    channels = 1
    sample_width = 2   # PCM nguồn 16-bit trước khi encode

    # Sau khi encode G711: 1 byte / sample, mono
    tx_bytes_per_sec = sample_rate * 1

    return {
        "sample_rate": sample_rate,
        "channels": channels,
        "sample_width": sample_width,
        "tx_bytes_per_sec": tx_bytes_per_sec,
        "pkt_type": pkt_type,
        "default_chunk": 160,   # 20ms ở 8kHz
        "align": 1,
        "label": label,
    }