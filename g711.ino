#include <Arduino.h>
#include <SPI.h>
#include <Ethernet.h>
#include <EthernetUdp.h>
#include <driver/i2s.h>

// =====================================================
// HW PINS: ESP32-C3 + W5500 + PCM5102A
// =====================================================
#define ETH_MOSI_PIN   2
#define ETH_MISO_PIN   1
#define ETH_SCK_PIN    3
#define ETH_CS_PIN     10
#define ETH_RST_PIN    4

#define I2S_BCK_PIN    5
#define I2S_WS_PIN     6
#define I2S_DATA_PIN   7

// =====================================================
// DEBUG CONFIG
// =====================================================
#define DBG_ENABLE_RAW_UDP     0
#define DBG_RAW_UDP_COUNT      5

#define DBG_ENABLE_HDR         0
#define DBG_HDR_COUNT          5

#define DBG_ENABLE_PARSE       0
#define DBG_PARSE_COUNT        10

#define DBG_ENABLE_BAD_MAGIC   0
#define DBG_BAD_MAGIC_COUNT    3

#define DBG_ENABLE_BAD_TYPE    0
#define DBG_BAD_TYPE_COUNT     5

#define DBG_ENABLE_SEQ         0
#define DBG_ENABLE_MODE        1
#define DBG_ENABLE_I2S         0
#define DBG_ENABLE_ETH         1
#define DBG_ENABLE_DROP        0
#define DBG_ENABLE_LOWBUF      1

// =====================================================
// NETWORK
// =====================================================
static byte MAC_ADDR[] = {0x02, 0x00, 0x00, 0x00, 0x00, 0x10};
IPAddress LOCAL_IP(192, 168, 1, 100);
IPAddress DNS_IP(8, 8, 8, 8);
IPAddress GATEWAY(192, 168, 1, 1);
IPAddress SUBNET(255, 255, 255, 0);

EthernetUDP udp;
const uint16_t UDP_PORT = 5000;

// =====================================================
// PACKET FORMAT
// MAGIC(2) + SEQ(4) + TYPE(1) + LEN(2) + PAYLOAD
// =====================================================
const uint8_t  MAGIC_0 = 0xAB;
const uint8_t  MAGIC_1 = 0xCD;
const size_t   HEADER_SIZE = 9;
const uint16_t END_SIGNAL = 0xFFFF;

const uint8_t PKT_PCM        = 1;
const uint8_t PKT_CALL_G711U = 2;
const uint8_t PKT_CALL_G711A = 3;

// =====================================================
// AUDIO MODES
// =====================================================
enum AudioMode : uint8_t {
  MODE_NONE  = 0,
  MODE_PCM   = PKT_PCM,
  MODE_G711U = PKT_CALL_G711U,
  MODE_G711A = PKT_CALL_G711A
};

volatile AudioMode currentMode = MODE_NONE;
volatile bool streamEnded = false;
volatile bool playing = false;

// =====================================================
// AUDIO / I2S CONFIG
// =====================================================
const i2s_port_t I2S_PORT = I2S_NUM_0;

const uint32_t PCM_SAMPLE_RATE  = 44100;
const uint32_t CALL_SAMPLE_RATE = 8000;

// Ring buffer PCM stereo 16-bit
// Nếu báo thiếu RAM khi compile, giảm xuống 128*1024
const size_t AUDIO_BUF_SIZE = 160 * 1024;
uint8_t audioBuf[AUDIO_BUF_SIZE];

volatile size_t bufHead = 0;
volatile size_t bufTail = 0;
volatile size_t bufUsed = 0;

portMUX_TYPE bufMux = portMUX_INITIALIZER_UNLOCKED;

// Packet buffer
const size_t MAX_PACKET_SIZE = 2048;
uint8_t packetBuf[MAX_PACKET_SIZE];

// Mỗi byte G711 -> 4 byte PCM stereo 16-bit
uint8_t decodeBuf[MAX_PACKET_SIZE * 4];

// =====================================================
// STATUS / COUNTERS
// =====================================================
uint32_t lastStatusMs = 0;
uint32_t lastSeq = 0;
bool seqValid = false;

uint32_t droppedBytes = 0;
uint32_t badPackets = 0;
uint32_t seqGaps = 0;
uint32_t totalPackets = 0;
uint32_t validPackets = 0;
uint32_t endPackets = 0;
uint32_t badMagicPackets = 0;
uint32_t badLenPackets = 0;
uint32_t badTypePackets = 0;
uint32_t tooBigPackets = 0;

uint32_t dbgRawUdpCount = 0;
uint32_t dbgHdrCount = 0;
uint32_t dbgParseCount = 0;
uint32_t dbgBadMagicCount = 0;
uint32_t dbgBadTypeCount = 0;

// =====================================================
// FORWARD DECLARE
// =====================================================
void reset_stream_counters();
void switch_mode(AudioMode newMode);
void handle_udp();
void audio_task(void* arg);
void udp_rx_task(void* arg);
void print_status();

// =====================================================
// HELPERS
// =====================================================
const char* mode_name(AudioMode mode) {
  switch (mode) {
    case MODE_PCM:   return "PCM";
    case MODE_G711U: return "G711U";
    case MODE_G711A: return "G711A";
    default:         return "NONE";
  }
}

const char* hw_status_name(EthernetHardwareStatus s) {
  switch (s) {
    case EthernetNoHardware: return "NoHardware";
    case EthernetW5100:      return "W5100";
    case EthernetW5200:      return "W5200";
    case EthernetW5500:      return "W5500";
    default:                 return "UnknownHW";
  }
}

const char* link_status_name(EthernetLinkStatus s) {
  switch (s) {
    case Unknown: return "Unknown";
    case LinkON:  return "LinkON";
    case LinkOFF: return "LinkOFF";
    default:      return "Link?";
  }
}

// =====================================================
// RING BUFFER
// =====================================================
size_t buf_used() {
  size_t v;
  portENTER_CRITICAL(&bufMux);
  v = bufUsed;
  portEXIT_CRITICAL(&bufMux);
  return v;
}

size_t buf_free() {
  size_t v;
  portENTER_CRITICAL(&bufMux);
  v = AUDIO_BUF_SIZE - bufUsed;
  portEXIT_CRITICAL(&bufMux);
  return v;
}

void buf_clear() {
  portENTER_CRITICAL(&bufMux);
  bufHead = 0;
  bufTail = 0;
  bufUsed = 0;
  portEXIT_CRITICAL(&bufMux);
}

size_t buf_write(const uint8_t* data, size_t len) {
  size_t written = 0;

  portENTER_CRITICAL(&bufMux);
  while (written < len && bufUsed < AUDIO_BUF_SIZE) {
    audioBuf[bufHead] = data[written];
    bufHead++;
    if (bufHead >= AUDIO_BUF_SIZE) bufHead = 0;
    bufUsed++;
    written++;
  }
  portEXIT_CRITICAL(&bufMux);

  return written;
}

size_t buf_read(uint8_t* out, size_t len) {
  size_t readn = 0;

  portENTER_CRITICAL(&bufMux);
  while (readn < len && bufUsed > 0) {
    out[readn] = audioBuf[bufTail];
    bufTail++;
    if (bufTail >= AUDIO_BUF_SIZE) bufTail = 0;
    bufUsed--;
    readn++;
  }
  portEXIT_CRITICAL(&bufMux);

  return readn;
}

// =====================================================
// G711 DECODER
// =====================================================
int16_t ulaw2linear(uint8_t u_val) {
  u_val = ~u_val;
  int t = ((u_val & 0x0F) << 3) + 0x84;
  t <<= ((unsigned)u_val & 0x70) >> 4;
  return (u_val & 0x80) ? (0x84 - t) : (t - 0x84);
}

int16_t alaw2linear(uint8_t a_val) {
  a_val ^= 0x55;

  int t = (a_val & 0x0F) << 4;
  int seg = ((unsigned)a_val & 0x70) >> 4;

  switch (seg) {
    case 0:
      t += 8;
      break;
    case 1:
      t += 0x108;
      break;
    default:
      t += 0x108;
      t <<= (seg - 1);
      break;
  }

  return (a_val & 0x80) ? t : -t;
}

// G711 mono 8k -> PCM stereo 16-bit
size_t decode_g711_to_stereo_pcm(const uint8_t* in, size_t inLen, uint8_t* out, bool isUlaw) {
  size_t o = 0;

  for (size_t i = 0; i < inLen; i++) {
    int16_t s = isUlaw ? ulaw2linear(in[i]) : alaw2linear(in[i]);

    // little-endian, mono -> stereo
    out[o++] = (uint8_t)(s & 0xFF);
    out[o++] = (uint8_t)((s >> 8) & 0xFF);
    out[o++] = (uint8_t)(s & 0xFF);
    out[o++] = (uint8_t)((s >> 8) & 0xFF);
  }

  return o;
}

// =====================================================
// I2S
// =====================================================
void i2s_setup_base() {
  i2s_config_t cfg = {};
  cfg.mode = (i2s_mode_t)(I2S_MODE_MASTER | I2S_MODE_TX);
  cfg.sample_rate = PCM_SAMPLE_RATE;
  cfg.bits_per_sample = I2S_BITS_PER_SAMPLE_16BIT;
  cfg.channel_format = I2S_CHANNEL_FMT_RIGHT_LEFT;
  cfg.communication_format = I2S_COMM_FORMAT_STAND_I2S;
  cfg.intr_alloc_flags = ESP_INTR_FLAG_LEVEL1;
  cfg.dma_buf_count = 12;
  cfg.dma_buf_len = 256;
  cfg.use_apll = false;
  cfg.tx_desc_auto_clear = true;
  cfg.fixed_mclk = 0;

  i2s_pin_config_t pins = {};
  pins.bck_io_num = I2S_BCK_PIN;
  pins.ws_io_num = I2S_WS_PIN;
  pins.data_out_num = I2S_DATA_PIN;
  pins.data_in_num = I2S_PIN_NO_CHANGE;

  i2s_driver_install(I2S_PORT, &cfg, 0, nullptr);
  i2s_set_pin(I2S_PORT, &pins);
  i2s_zero_dma_buffer(I2S_PORT);
  i2s_set_clk(I2S_PORT, PCM_SAMPLE_RATE, I2S_BITS_PER_SAMPLE_16BIT, I2S_CHANNEL_STEREO);

#if DBG_ENABLE_I2S
  Serial.println("[I2S] Init OK");
#endif
}

void set_i2s_rate_for_mode(AudioMode mode) {
  if (mode == MODE_PCM) {
    i2s_set_clk(I2S_PORT, PCM_SAMPLE_RATE, I2S_BITS_PER_SAMPLE_16BIT, I2S_CHANNEL_STEREO);
#if DBG_ENABLE_I2S
    Serial.println("[I2S] Mode PCM 44.1kHz stereo");
#endif
  } else if (mode == MODE_G711U || mode == MODE_G711A) {
    i2s_set_clk(I2S_PORT, CALL_SAMPLE_RATE, I2S_BITS_PER_SAMPLE_16BIT, I2S_CHANNEL_STEREO);
#if DBG_ENABLE_I2S
    Serial.println("[I2S] Mode CALL 8kHz stereo(out)");
#endif
  }
}

size_t start_threshold() {
  if (currentMode == MODE_PCM) return 80 * 1024;
  if (currentMode == MODE_G711U || currentMode == MODE_G711A) return 24 * 1024;
  return 4 * 1024;
}

size_t low_threshold() {
  if (currentMode == MODE_PCM) return 24 * 1024;
  if (currentMode == MODE_G711U || currentMode == MODE_G711A) return 8 * 1024;
  return 2 * 1024;
}

void switch_mode(AudioMode newMode) {
  if (newMode == MODE_NONE || newMode == currentMode) return;

  reset_stream_counters();
  playing = false;
  streamEnded = false;
  seqValid = false;
  buf_clear();
  i2s_zero_dma_buffer(I2S_PORT);
  currentMode = newMode;
  set_i2s_rate_for_mode(newMode);

#if DBG_ENABLE_MODE
  Serial.printf("[MODE] Switch -> %s\n", mode_name(newMode));
#endif
}

// =====================================================
// AUDIO TASK
// =====================================================
void audio_task(void* arg) {
  // 1280 B = 40 ms audio ở 8k stereo 16-bit
  // nhưng sẽ ghi ra I2S theo từng cục 640 B = ~20 ms
  static uint8_t outChunk[1280];
  uint32_t lastLowBufLogMs = 0;

  while (true) {
    size_t used = buf_used();

    if (playing && !streamEnded && used < low_threshold()) {
      playing = false;
      i2s_zero_dma_buffer(I2S_PORT);

#if DBG_ENABLE_LOWBUF
      uint32_t now = millis();
      if (now - lastLowBufLogMs > 500) {
        Serial.printf("[AUDIO] Rebuffer | mode=%s | buf=%uB\n",
                      mode_name((AudioMode)currentMode),
                      (unsigned)used);
        lastLowBufLogMs = now;
      }
#endif
    }

    if (!playing) {
      if (used >= start_threshold() || (streamEnded && used > 0)) {
        playing = true;
        Serial.printf("[AUDIO] Start play | mode=%s | buf=%uB | threshold=%uB\n",
                      mode_name((AudioMode)currentMode),
                      (unsigned)used,
                      (unsigned)start_threshold());
      } else {
        vTaskDelay(pdMS_TO_TICKS(1));
        continue;
      }
    }

    if (used == 0) {
      if (streamEnded) {
        playing = false;
        streamEnded = false;
        i2s_zero_dma_buffer(I2S_PORT);
        Serial.println("[AUDIO] Stream finished");
      }
      vTaskDelay(pdMS_TO_TICKS(1));
      continue;
    }

    size_t toRead = used;
    if (toRead > sizeof(outChunk)) toRead = sizeof(outChunk);

    toRead = (toRead / 4) * 4;   // stereo 16-bit = 4 byte/frame
    if (toRead == 0) {
      vTaskDelay(pdMS_TO_TICKS(1));
      continue;
    }

    size_t got = buf_read(outChunk, toRead);
    if (got > 0) {
      size_t offset = 0;

      while (offset < got) {
        size_t once = got - offset;
        if (once > 640) once = 640;   // ~20 ms ở 8k stereo 16-bit

        size_t written = 0;
        i2s_write(I2S_PORT, outChunk + offset, once, &written, portMAX_DELAY);

        if (written == 0) {
          break;
        }

        offset += written;
        taskYIELD();
      }
    } else {
      vTaskDelay(pdMS_TO_TICKS(1));
    }
  }
}

// =====================================================
// W5500
// =====================================================
void reset_w5500() {
  pinMode(ETH_RST_PIN, OUTPUT);
  digitalWrite(ETH_RST_PIN, LOW);
  delay(50);
  digitalWrite(ETH_RST_PIN, HIGH);
  delay(200);
}

void ethernet_setup() {
  SPI.begin(ETH_SCK_PIN, ETH_MISO_PIN, ETH_MOSI_PIN, ETH_CS_PIN);
  Ethernet.init(ETH_CS_PIN);

  reset_w5500();

  Ethernet.begin(MAC_ADDR, LOCAL_IP, DNS_IP, GATEWAY, SUBNET);
  delay(200);

#if DBG_ENABLE_ETH
  Serial.printf("[ETH] HW   : %s\n", hw_status_name(Ethernet.hardwareStatus()));
  Serial.printf("[ETH] Link : %s\n", link_status_name(Ethernet.linkStatus()));
#endif

  Serial.print("[ETH] IP   : ");
  Serial.println(Ethernet.localIP());

  udp.begin(UDP_PORT);
  Serial.print("[UDP] Port : ");
  Serial.println(UDP_PORT);
}

// =====================================================
// UDP RX
// =====================================================
void handle_udp() {
  while (true) {
    int pktSize = udp.parsePacket();
    if (pktSize <= 0) break;

    totalPackets++;

#if DBG_ENABLE_RAW_UDP
    if (dbgRawUdpCount < DBG_RAW_UDP_COUNT) {
      Serial.printf("[RAW UDP] size=%d from %s:%d\n",
                    pktSize,
                    udp.remoteIP().toString().c_str(),
                    udp.remotePort());
      dbgRawUdpCount++;
    }
#endif

    if (pktSize > (int)sizeof(packetBuf)) {
      udp.read(packetBuf, sizeof(packetBuf));
      badPackets++;
      tooBigPackets++;
      continue;
    }

    int received = udp.read(packetBuf, pktSize);
    if (received < (int)HEADER_SIZE) {
      badPackets++;
      badLenPackets++;
      continue;
    }

    if (packetBuf[0] != MAGIC_0 || packetBuf[1] != MAGIC_1) {
      badPackets++;
      badMagicPackets++;
#if DBG_ENABLE_BAD_MAGIC
      if (dbgBadMagicCount < DBG_BAD_MAGIC_COUNT) {
        Serial.printf("[BAD MAGIC] %02X %02X %02X %02X %02X %02X %02X %02X %02X\n",
                      packetBuf[0], packetBuf[1], packetBuf[2], packetBuf[3], packetBuf[4],
                      packetBuf[5], packetBuf[6], packetBuf[7], packetBuf[8]);
        dbgBadMagicCount++;
      }
#endif
      continue;
    }

#if DBG_ENABLE_HDR
    if (dbgHdrCount < DBG_HDR_COUNT) {
      Serial.printf("[HDR] %02X %02X %02X %02X %02X %02X %02X %02X %02X\n",
                    packetBuf[0], packetBuf[1], packetBuf[2], packetBuf[3], packetBuf[4],
                    packetBuf[5], packetBuf[6], packetBuf[7], packetBuf[8]);
      dbgHdrCount++;
    }
#endif

    uint32_t seq =
      ((uint32_t)packetBuf[2] << 24) |
      ((uint32_t)packetBuf[3] << 16) |
      ((uint32_t)packetBuf[4] << 8)  |
       (uint32_t)packetBuf[5];

    uint8_t type = packetBuf[6];

    uint16_t dataLen =
      ((uint16_t)packetBuf[7] << 8) |
       (uint16_t)packetBuf[8];

#if DBG_ENABLE_PARSE
    if (dbgParseCount < DBG_PARSE_COUNT) {
      Serial.printf("[PARSE] seq=%lu type=%u len=%u\n",
                    (unsigned long)seq,
                    (unsigned)type,
                    (unsigned)dataLen);
      dbgParseCount++;
    }
#endif

    if (dataLen == END_SIGNAL) {
      streamEnded = true;
      seqValid = false;
      endPackets++;
      dbgHdrCount = 0;
      dbgRawUdpCount = 0;
      dbgParseCount = 0;
      dbgBadMagicCount = 0;
      dbgBadTypeCount = 0;
      Serial.printf("[STREAM] END | seq=%lu | type=%u\n",
                    (unsigned long)seq,
                    (unsigned)type);
      continue;
    }

    if ((HEADER_SIZE + dataLen) != (size_t)received) {
      badPackets++;
      badLenPackets++;
      Serial.printf("[BAD LEN] received=%d expect=%u\n",
                    received,
                    (unsigned)(HEADER_SIZE + dataLen));
      continue;
    }

    AudioMode pktMode = MODE_NONE;
    if (type == PKT_PCM) pktMode = MODE_PCM;
    else if (type == PKT_CALL_G711U) pktMode = MODE_G711U;
    else if (type == PKT_CALL_G711A) pktMode = MODE_G711A;
    else {
      badPackets++;
      badTypePackets++;
#if DBG_ENABLE_BAD_TYPE
      if (dbgBadTypeCount < DBG_BAD_TYPE_COUNT) {
        Serial.printf("[BAD TYPE] type=%u seq=%lu len=%u\n",
                      (unsigned)type,
                      (unsigned long)seq,
                      (unsigned)dataLen);
        dbgBadTypeCount++;
      }
#endif
      continue;
    }

    if (seqValid && seq != (lastSeq + 1)) {
      seqGaps++;
#if DBG_ENABLE_SEQ
      Serial.printf("[SEQ] expected=%lu got=%lu type=%u len=%u\n",
                    (unsigned long)(lastSeq + 1),
                    (unsigned long)seq,
                    (unsigned)type,
                    (unsigned)dataLen);
#endif
    }

    lastSeq = seq;
    seqValid = true;
    validPackets++;

    if (pktMode != currentMode) {
      switch_mode(pktMode);
    }

    const uint8_t* payload = packetBuf + HEADER_SIZE;
    size_t payloadBytes = dataLen;

    if (pktMode == MODE_PCM) {
      size_t written = buf_write(payload, payloadBytes);
      if (written < payloadBytes) {
        droppedBytes += (payloadBytes - written);
#if DBG_ENABLE_DROP
        Serial.printf("[DROP PCM] write=%u need=%u free=%u\n",
                      (unsigned)written,
                      (unsigned)payloadBytes,
                      (unsigned)buf_free());
#endif
      }
    } else {
      bool isUlaw = (pktMode == MODE_G711U);
      size_t pcmBytes = decode_g711_to_stereo_pcm(payload, payloadBytes, decodeBuf, isUlaw);
      size_t written = buf_write(decodeBuf, pcmBytes);
      if (written < pcmBytes) {
        droppedBytes += (pcmBytes - written);
#if DBG_ENABLE_DROP
        Serial.printf("[DROP G711] write=%u need=%u free=%u\n",
                      (unsigned)written,
                      (unsigned)pcmBytes,
                      (unsigned)buf_free());
#endif
      }
    }
  }
}

void udp_rx_task(void* arg) {
  while (true) {
    handle_udp();
    vTaskDelay(pdMS_TO_TICKS(1));
  }
}

// =====================================================
// RESET COUNTERS
// =====================================================
void reset_stream_counters() {
  droppedBytes = 0;
  badPackets = 0;
  seqGaps = 0;
  totalPackets = 0;
  validPackets = 0;
  endPackets = 0;
  badMagicPackets = 0;
  badLenPackets = 0;
  badTypePackets = 0;
  tooBigPackets = 0;
  lastSeq = 0;
  seqValid = false;
}

// =====================================================
// STATUS
// =====================================================
void print_status() {
  uint32_t now = millis();
  if (now - lastStatusMs < 10000) return;
  lastStatusMs = now;

  size_t used = buf_used();
  float percent = 100.0f * (float)used / (float)AUDIO_BUF_SIZE;

  Serial.printf(
    "[STATUS] mode=%s | buf=%u/%u (%.1f%%) | playing=%d | total=%lu valid=%lu end=%lu | bad=%lu {magic=%lu len=%lu type=%lu big=%lu} | dropB=%lu | seqGap=%lu\n",
    mode_name((AudioMode)currentMode),
    (unsigned)used,
    (unsigned)AUDIO_BUF_SIZE,
    percent,
    playing ? 1 : 0,
    (unsigned long)totalPackets,
    (unsigned long)validPackets,
    (unsigned long)endPackets,
    (unsigned long)badPackets,
    (unsigned long)badMagicPackets,
    (unsigned long)badLenPackets,
    (unsigned long)badTypePackets,
    (unsigned long)tooBigPackets,
    (unsigned long)droppedBytes,
    (unsigned long)seqGaps
  );
}

// =====================================================
// SETUP / LOOP
// =====================================================
void setup() {
  Serial.begin(921600);
  delay(1000);
  Serial.println();
  Serial.println("ESP32 UDP AUDIO RX - BUILD V7");
  Serial.println("PCM + G711(u/a-law) -> I2S -> PCM5102A");

  i2s_setup_base();
  ethernet_setup();

  xTaskCreate(
    udp_rx_task,
    "udp_rx_task",
    4096,
    nullptr,
    3,
    nullptr
  );

  xTaskCreate(
    audio_task,
    "audio_task",
    6144,
    nullptr,
    2,
    nullptr
  );

  Serial.printf("[BUF] Ring buffer: %u KB\n", (unsigned)(AUDIO_BUF_SIZE / 1024));
  Serial.println("[READY] Waiting for UDP stream...");
}

void loop() {
  print_status();
  delay(10);
}