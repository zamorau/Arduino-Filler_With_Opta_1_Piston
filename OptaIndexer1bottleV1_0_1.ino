/*
  Arduino OPTA - Version 1.0.1
  Reduced I/O + WiFi + lightweight JSON API for external HMI

  OUTPUTS
    D0 = IN piston relay
    D1 = OUT piston relay
    D2 = FILL relay
    D3 = CONVEYOR relay

  INPUTS
    A0 = bottle sensor
    A2 = start/stop pushbutton

  Pushbutton behavior
    Short press = Start / Stop toggle
    Long press  = Reset alarms

  API
    GET /api/status
    GET /api/run?set=1
    GET /api/run?set=0
    GET /api/alarm/reset
    GET /api/set?delay=..&fill=..&indelay=..&outmin=..&filter=..

  Notes
    - No local HTML HMI
    - External HMI/server polls and commands this controller
    - Non-blocking logic
    - Parameters persist in internal flash
*/

#include <Arduino.h>
#include <WiFi.h>
#include <WiFiUdp.h>
#include <cstddef>
#include <cmath>
#include "mbed.h"

using namespace mbed;

// =====================================================
// MACHINE ID
// =====================================================
static const char* MACHINE_NAME = "DRLZ-FILLER-INDEXER-02";
static const char* FIRMWARE_VERSION = "1.0.1";

// =====================================================
// UDP TELEMETRY
// =====================================================
static WiFiUDP udp;

// CHANGE THIS TO YOUR WINDOWS TELEMETRY SERVER IP
static const uint8_t UDP_SERVER_IP[4] = {10, 1, 10, 58};
static const uint16_t UDP_SERVER_PORT = 5001;

static const uint32_t UDP_HEARTBEAT_MS = 500;

static uint32_t lastUdpSendMs = 0;
static int lastSentState = -1;
static uint32_t lastSentAlarms = 0xFFFFFFFF;
static uint32_t lastSentTotal = 0xFFFFFFFF;
static uint32_t lastSentCycles = 0xFFFFFFFF;
static bool lastSentRun = false;

static float lastSentDelay = -999.0f;
static float lastSentFill = -999.0f;
static float lastSentInDelay = -999.0f;
static float lastSentOutMin = -999.0f;
static float lastSentFilter = -999.0f;
// =====================================================
// TYPES
// =====================================================
enum CycleState : uint8_t {
  ST_WAIT_BOTTLE = 0,
  //ST_WAIT_2ND_BOTTLE,
  ST_DELAY_BEFORE_FILL,
  ST_FILL_PULSE,
  ST_WAIT_FILL_DONE,
  ST_OUT_OPENING,
  ST_IN_REOPEN_DELAY,
  ST_ALARM
};

enum AlarmBits : uint32_t {
  ALM_NONE                  = 0,
  ALM_SENSOR_STUCK_ON       = 1u << 0,
  ALM_EXIT_TIMEOUT          = 1u << 1,
  ALM_CYCLE_TIMEOUT         = 1u << 2
};

// =====================================================
// WIFI
// =====================================================
static uint32_t mWL_CONNECTED = 3;

struct WifiConfig {
  char ssid[32];
  char pass[64];
};

static WifiConfig wifiCfg = {
  "VPharma",
  "445w26th"
};

// =====================================================
// PIN MAP
// =====================================================
static const uint8_t PIN_SENSOR_1      = A0;
// static const uint8_t PIN_SENSOR_2      = A1;
static const uint8_t PIN_STARTSTOP_PB  = A2;

static const uint8_t PIN_IN_OPEN       = D0;
static const uint8_t PIN_OUT_OPEN      = D1;
static const uint8_t PIN_FILL_RELAY    = D2;
static const uint8_t PIN_CONVEYOR      = D3;

// OPTA LEDs
static const uint8_t LED_IN            = LED_D0;
static const uint8_t LED_OUT           = LED_D1;
static const uint8_t LED_FILL          = LED_D2;
static const uint8_t LED_SENSOR        = LED_D3;

// =====================================================
// HARDWARE POLARITY
// =====================================================
static const bool SENSOR1_ACTIVE_HIGH = true;
// static const bool SENSOR2_ACTIVE_HIGH = true;
static const bool PB_ACTIVE_HIGH = true;

// Keep this aligned with your actual wiring.
// If relay HIGH means conveyor RUN, keep true.
// If relay HIGH means conveyor STOP, change to false.
static const bool CONVEYOR_HIGH_MEANS_RUN = false;

enum RunMode : uint8_t { MODE_AUTO = 0, MODE_MANUAL = 1 };
static RunMode runMode = MODE_AUTO;

// manual command bits
static bool manInOpen = false;
static bool manOutOpen = false;
static bool manConveyorRun = false;
static bool manFillPulseReq = false;
static uint32_t manFillPulseStartMs = 0;
// =====================================================
// NETWORK CONFIG
// =====================================================
enum NetMode : uint8_t { NET_DHCP = 1, NET_STATIC = 2 };

struct NetConfig {
  uint8_t mode;
  uint8_t ip[4];
  uint8_t gw[4];
  uint8_t sn[4];
};

static NetConfig netCfg = {
  NET_STATIC,
  {10, 1, 10, 218},
  {10, 1, 10, 1},
  {255, 255, 255, 0}
};

static WiFiServer httpServer(80);

// =====================================================
// PARAMETERS
// =====================================================
struct Params {
  float tDelayFill;
  float tFill;
  float tInDelay;
  float tOutMin;
  float tFilter;
};

static Params P = {
  1.00f,
  4.80f,
  1.35f,
  2.25f,
  0.05f
};

// =====================================================
// PERSISTENCE
// =====================================================
static const uint32_t PERSIST_MAGIC   = 0x4F505431; // OPT1
static const uint32_t PERSIST_VERSION = 1;

struct Persist {
  uint32_t magic;
  uint32_t version;
  uint32_t crc;
  Params   p;
};

static Persist S;

static FlashIAP flash;
static uint32_t flash_base_addr = 0;
static uint32_t flash_erase_total = 0;
static uint32_t flash_prog_size = 0;

static inline uint32_t align_up(uint32_t v, uint32_t a) {
  return (a == 0) ? v : ((v + a - 1) / a) * a;
}

static uint32_t crc32_update(uint32_t crc, uint8_t data) {
  crc ^= data;
  for (int i = 0; i < 8; i++) {
    uint32_t mask = -(crc & 1u);
    crc = (crc >> 1) ^ (0xEDB88320u & mask);
  }
  return crc;
}

static uint32_t crc32_calc(const uint8_t* data, size_t len) {
  uint32_t crc = 0xFFFFFFFFu;
  for (size_t i = 0; i < len; i++) crc = crc32_update(crc, data[i]);
  return ~crc;
}

static uint32_t persist_crc_calc(const Persist& x) {
  const uint8_t* payload = (const uint8_t*)&x.p;
  const size_t len = sizeof(Persist) - offsetof(Persist, p);
  return crc32_calc(payload, len);
}

static bool flash_prepare_region(uint32_t needed_bytes) {
  if (flash.init() != 0) return false;

  const uint32_t start = flash.get_flash_start();
  const uint32_t size  = flash.get_flash_size();
  const uint32_t end   = start + size;

  flash_prog_size = flash.get_page_size();
  if (flash_prog_size == 0) flash_prog_size = 8;

  const uint32_t needed = align_up(needed_bytes, flash_prog_size);

  uint32_t addr = end;
  uint32_t total = 0;

  while (total < needed) {
    uint32_t probe = addr - 1;
    uint32_t sector = flash.get_sector_size(probe);
    addr -= sector;
    total += sector;

    if (addr < start + 0x10000) {
      flash.deinit();
      return false;
    }
  }

  flash_base_addr = addr;
  flash_erase_total = total;

  flash.deinit();
  return true;
}

static bool persist_read() {
  static bool region_ok = false;
  if (!region_ok) {
    if (!flash_prepare_region(sizeof(Persist))) return false;
    region_ok = true;
  }

  Persist tmp;
  memcpy(&tmp, (const void*)flash_base_addr, sizeof(Persist));

  if (tmp.magic != PERSIST_MAGIC || tmp.version != PERSIST_VERSION) return false;

  const uint32_t calc = persist_crc_calc(tmp);
  if (tmp.crc != calc) return false;

  if (isnan(tmp.p.tDelayFill) || isnan(tmp.p.tFill) || isnan(tmp.p.tInDelay) ||
      isnan(tmp.p.tOutMin) || isnan(tmp.p.tFilter)) return false;

  S = tmp;
  P = S.p;
  return true;
}

static bool persist_write() {
  static bool region_ok = false;
  if (!region_ok) {
    if (!flash_prepare_region(sizeof(Persist))) return false;
    region_ok = true;
  }

  S.magic = PERSIST_MAGIC;
  S.version = PERSIST_VERSION;
  S.p = P;
  S.crc = persist_crc_calc(S);

  if (flash.init() != 0) return false;

  uint32_t addr = flash_base_addr;
  uint32_t remaining = flash_erase_total;
  while (remaining > 0) {
    const uint32_t sector = flash.get_sector_size(addr);
    if (flash.erase(addr, sector) != 0) {
      flash.deinit();
      return false;
    }
    addr += sector;
    remaining -= sector;
  }

  const uint32_t prog_len = align_up((uint32_t)sizeof(Persist), flash_prog_size);
  uint8_t buf[256];
  if (prog_len > sizeof(buf)) {
    flash.deinit();
    return false;
  }

  memset(buf, 0xFF, sizeof(buf));
  memcpy(buf, &S, sizeof(Persist));

  const int rc = flash.program(buf, flash_base_addr, prog_len);
  flash.deinit();
  return rc == 0;
}

// =====================================================
// HELPERS
// =====================================================
static inline uint32_t s_to_ms(float s) {
  if (s <= 0.0f) return 0UL;
  return (uint32_t)lroundf(s * 1000.0f);
}

static inline float clampf(float v, float lo, float hi) {
  return (v < lo) ? lo : (v > hi) ? hi : v;
}

static inline bool validOctet(int v) {
  return v >= 0 && v <= 255;
}

static String ipToString(IPAddress ip) {
  return String(ip[0]) + "." + String(ip[1]) + "." + String(ip[2]) + "." + String(ip[3]);
}

static IPAddress ipFrom4(const uint8_t b[4]) {
  return IPAddress(b[0], b[1], b[2], b[3]);
}

static bool parseIpStr(const String& s, uint8_t out[4]) {
  int a, b, c, d;
  if (sscanf(s.c_str(), "%d.%d.%d.%d", &a, &b, &c, &d) != 4) return false;
  if (!validOctet(a) || !validOctet(b) || !validOctet(c) || !validOctet(d)) return false;
  out[0] = (uint8_t)a;
  out[1] = (uint8_t)b;
  out[2] = (uint8_t)c;
  out[3] = (uint8_t)d;
  return true;
}

// =====================================================
// FILTER
// =====================================================
class StableFilter {
public:
  void setSec(float sec) {
    stableMs = s_to_ms(sec);
    if (stableMs < 5UL) stableMs = 5UL;
  }

  bool update(bool raw, uint32_t now) {
    if (raw != lastRaw) {
      lastRaw = raw;
      lastChangeMs = now;
    }

    if (stable != lastRaw && (now - lastChangeMs) >= stableMs) {
      stable = lastRaw;
    }
    return stable;
  }

private:
  uint32_t stableMs = 50;
  bool lastRaw = false;
  bool stable = false;
  uint32_t lastChangeMs = 0;
};

static StableFilter sensor1Filter;
// static StableFilter sensor2Filter;

// =====================================================
// RUNTIME
// =====================================================
static const float FILL_PULSE_S = 0.20f;
static const uint32_t FILL_FAILSAFE_MS             = 1000UL;
static const uint32_t T_SENSOR_STUCK_ON_MS         = 8000UL;
static const uint32_t T_EXIT_TIMEOUT_MS            = 3000UL;
static const uint32_t T_CYCLE_TIMEOUT_MS           = 15000UL;
static const uint32_t T_LONG_PRESS_MS              = 2500UL;
// static const uint32_t T_SECOND_BOTTLE_TIMEOUT_MS   = 4000UL;

static CycleState state = ST_WAIT_BOTTLE;

static bool machineRun = false;
static bool sensor1 = false;
// static bool sensor2 = false;
static bool sensor = false;   // compatibility / station occupied

static bool armedForNewBottle = true;
static bool armedFor2ndBottle = true;

static uint32_t tStateMs = 0;
static uint32_t cycleStartMs = 0;
static uint32_t fillPulseStartMs = 0;
static uint32_t outOpenStartMs = 0;
static uint32_t sensorOnStartMs = 0;

// production
static uint32_t bottlesTotal = 0;
static uint32_t cyclesTotal = 0;
static uint32_t count10s = 0;
static uint16_t bpm = 0;
static float avgBpm = 0.0f;
static uint32_t bpmSamples = 0;
static uint32_t windowStartMs = 0;

// alarms
static uint32_t alarms = 0;
static bool alarmLatched = false;

// pushbutton
static bool pbStable = false;
static bool pbLastStable = false;
static bool pbPressed = false;
static bool pbLongHandled = false;
static uint32_t pbPressStartMs = 0;

// outputs
static bool outInOpen = true;
static bool outOutOpen = false;
static bool outFillRelay = false;
static bool outConveyorRun = true;

// =====================================================
// INPUTS
// =====================================================
static bool readSensor1Raw() {
  bool v = (digitalRead(PIN_SENSOR_1) == LOW);
  return SENSOR1_ACTIVE_HIGH ? v : !v;
}

// static bool readSensor2Raw() {
//   bool v = (digitalRead(PIN_SENSOR_2) == LOW);
//   return SENSOR2_ACTIVE_HIGH ? v : !v;
// }

static bool readPbRaw() {
  bool v = (digitalRead(PIN_STARTSTOP_PB) == HIGH);
  return PB_ACTIVE_HIGH ? v : !v;
}

// =====================================================
// OUTPUTS
// =====================================================
static void writeConveyor(bool runCmd) {
  bool level = CONVEYOR_HIGH_MEANS_RUN ? runCmd : !runCmd;
  digitalWrite(PIN_CONVEYOR, level ? HIGH : LOW);
}

static void applyOutputs() {
  digitalWrite(PIN_IN_OPEN,    outInOpen ? HIGH : LOW);
  digitalWrite(PIN_OUT_OPEN,   outOutOpen ? HIGH : LOW);
  digitalWrite(PIN_FILL_RELAY, outFillRelay ? HIGH : LOW);
  writeConveyor(outConveyorRun);

  digitalWrite(LED_IN,     outInOpen ? HIGH : LOW);
  digitalWrite(LED_OUT,    outOutOpen ? HIGH : LOW);
  digitalWrite(LED_FILL,   outFillRelay ? HIGH : LOW);
  digitalWrite(LED_SENSOR, sensor ? HIGH : LOW);
}

// =====================================================
// MACHINE HELPERS
// =====================================================
static void enterState(CycleState s, uint32_t now) {
  state = s;
  tStateMs = now;
  if (s == ST_OUT_OPENING) outOpenStartMs = now;
  if (s == ST_DELAY_BEFORE_FILL) cycleStartMs = now;
}

static void clearAlarms(uint32_t now) {
  alarms = 0;
  alarmLatched = false;
  cycleStartMs = 0;
  outInOpen = true;
  outOutOpen = false;
  outFillRelay = false;
  outConveyorRun = true;

  applyOutputs();
  enterState(ST_WAIT_BOTTLE, now);
}

static void raiseAlarm(uint32_t bit, uint32_t now) {
  alarms |= bit;
  alarmLatched = true;  
  outConveyorRun = true;
  applyOutputs();
  enterState(ST_ALARM, now);
}

static const char* stateName(CycleState s) {
  switch (s) {
    case ST_WAIT_BOTTLE:       return "WAIT_BOTTLE";
    //case ST_WAIT_2ND_BOTTLE:   return "WAIT_2ND_BOTTLE";
    case ST_DELAY_BEFORE_FILL: return "DELAY_BEFORE_FILL";
    case ST_FILL_PULSE:        return "FILL_PULSE";
    case ST_WAIT_FILL_DONE:    return "WAIT_FILL_DONE";
    case ST_OUT_OPENING:       return "OUT_OPENING";
    case ST_IN_REOPEN_DELAY:   return "IN_REOPEN_DELAY";
    case ST_ALARM:             return "ALARM";
    default:                   return "UNKNOWN";
  }
}

static String alarmTextFromBits(uint32_t a) {
  if (a == 0) return "NONE";

  String s = "";

  if (a & ALM_SENSOR_STUCK_ON) {
    if (s.length()) s += " | ";
    s += "SENSOR_STUCK_ON";
  }
  if (a & ALM_EXIT_TIMEOUT) {
    if (s.length()) s += " | ";
    s += "EXIT_TIMEOUT";
  }
  if (a & ALM_CYCLE_TIMEOUT) {
    if (s.length()) s += " | ";
    s += "CYCLE_TIMEOUT";
  }
 
  return s;
}

static const char* firstAlarmText(uint32_t a) {
  if (a & ALM_SENSOR_STUCK_ON)       return "SENSOR_STUCK_ON";
  if (a & ALM_EXIT_TIMEOUT)          return "EXIT_TIMEOUT";
  if (a & ALM_CYCLE_TIMEOUT)         return "CYCLE_TIMEOUT";
  return "NONE";
}

// =====================================================
// HTTP HELPERS
// =====================================================
static String urlDecode(const String& s) {
  String out;
  out.reserve(s.length());

  for (int i = 0; i < (int)s.length(); i++) {
    char c = s[i];
    if (c == '+') {
      out += ' ';
    } else if (c == '%' && i + 2 < (int)s.length()) {
      auto hex = [](char h) -> int {
        if (h >= '0' && h <= '9') return h - '0';
        if (h >= 'A' && h <= 'F') return 10 + (h - 'A');
        if (h >= 'a' && h <= 'f') return 10 + (h - 'a');
        return 0;
      };
      out += char((hex(s[i + 1]) << 4) | hex(s[i + 2]));
      i += 2;
    } else {
      out += c;
    }
  }
  return out;
}

static String getQueryParam(const String& path, const char* key) {
  int q = path.indexOf('?');
  if (q < 0) return "";

  String qs = path.substring(q + 1);
  String k = String(key) + "=";
  int pos = 0;

  while (pos < (int)qs.length()) {
    int amp = qs.indexOf('&', pos);
    if (amp < 0) amp = qs.length();
    String part = qs.substring(pos, amp);
    if (part.startsWith(k)) return urlDecode(part.substring(k.length()));
    pos = amp + 1;
  }

  return "";
}

static void httpSend(WiFiClient& c, const char* status, const char* contentType, const String& body) {
  c.print("HTTP/1.1 "); c.println(status);
  c.print("Content-Type: "); c.println(contentType);
  c.println("Cache-Control: no-store, no-cache, must-revalidate, max-age=0");
  c.println("Pragma: no-cache");
  c.println("Expires: 0");
  c.println("Connection: close");
  c.print("Content-Length: "); c.println(body.length());
  c.println();
  c.print(body);
}

static void httpSendText(WiFiClient& c, const char* status, const String& body) {
  httpSend(c, status, "text/plain; charset=utf-8", body);
}

static void httpSendJson(WiFiClient& c, const String& body) {
  httpSend(c, "200 OK", "application/json; charset=utf-8", body);
}

static void sendUdpStatusNow(uint32_t now) {
  if (WiFi.status() != mWL_CONNECTED) return;

  IPAddress serverIp(
    UDP_SERVER_IP[0],
    UDP_SERVER_IP[1],
    UDP_SERVER_IP[2],
    UDP_SERVER_IP[3]
  );

  String payload = buildUdpJson(now);

  udp.beginPacket(serverIp, UDP_SERVER_PORT);
  udp.write((const uint8_t*)payload.c_str(), payload.length());
  udp.endPacket();
}
// =====================================================
// JSON STATUS
// =====================================================
static String buildJsonStatus() {
  IPAddress cur = WiFi.localIP();

  String json = "{";
  json += "\"ok\":true,";
  json += "\"machine_name\":\"" + String(MACHINE_NAME) + "\",";
  json += "\"firmware_version\":\"" + String(FIRMWARE_VERSION) + "\",";
  json += "\"machineRun\":" + String(machineRun ? "true" : "false") + ",";
  json += "\"run_mode\":\"" + String(runMode == MODE_AUTO ? "auto" : "manual") + "\",";
  json += "\"state\":" + String((int)state) + ",";
  json += "\"stateName\":\"" + String(stateName(state)) + "\",";
  json += "\"sensor\":" + String(sensor ? "true" : "false") + ",";
  // json += "\"sensor_1\":" + String(sensor1 ? "true" : "false") + ",";
  // json += "\"sensor_2\":" + String(sensor2 ? "true" : "false") + ",";
  json += "\"in_open\":" + String(outInOpen ? "true" : "false") + ",";
  json += "\"out_open\":" + String(outOutOpen ? "true" : "false") + ",";
  json += "\"fill_active\":" + String(outFillRelay ? "true" : "false") + ",";
  json += "\"conveyor_run\":" + String(outConveyorRun ? "true" : "false") + ",";
  json += "\"alarm_latched\":" + String(alarmLatched ? "true" : "false") + ",";
  json += "\"alarms\":" + String(alarms) + ",";
  json += "\"alarm_text\":\"" + String(firstAlarmText(alarms)) + "\",";
  json += "\"alarm_list\":\"" + alarmTextFromBits(alarms) + "\",";
  json += "\"total\":" + String(bottlesTotal) + ",";
  json += "\"cycles\":" + String(cyclesTotal) + ",";
  json += "\"bpm\":" + String(bpm) + ",";
  json += "\"avg_bpm\":" + String(avgBpm, 2) + ",";
  json += "\"params\":{";
  json += "\"delay\":" + String(P.tDelayFill, 3) + ",";
  json += "\"fill\":" + String(P.tFill, 3) + ",";
  json += "\"indelay\":" + String(P.tInDelay, 3) + ",";
  json += "\"outmin\":" + String(P.tOutMin, 3) + ",";
  json += "\"filter\":" + String(P.tFilter, 3);
  json += "},";
  json += "\"net\":{";
  json += "\"mode\":\"" + String(netCfg.mode == NET_DHCP ? "dhcp" : "static") + "\",";
  json += "\"ip\":\"" + ipToString(cur) + "\",";
  json += "\"rssi\":" + String(WiFi.RSSI());
  json += "},";
  json += "\"raw\":{";
  json += "\"pin_sensor_1_high\":" + String(digitalRead(PIN_SENSOR_1) == HIGH ? "true" : "false") + ",";
  // json += "\"pin_sensor_2_high\":" + String(digitalRead(PIN_SENSOR_2) == HIGH ? "true" : "false") + ",";
  json += "\"sensor_active\":" + String(sensor ? "true" : "false");
  json += "},";
  json += "\"debug\":{";
  json += "\"tStateMs\":" + String(tStateMs) + ",";
  json += "\"cycleStartMs\":" + String(cycleStartMs) + ",";
  json += "\"fillPulseStartMs\":" + String(fillPulseStartMs) + ",";
  json += "\"outOpenStartMs\":" + String(outOpenStartMs) + ",";
  json += "\"sensorOnStartMs\":" + String(sensorOnStartMs) + ",";
  json += "}";
  json += "}";
  return json;
}
static String buildUdpJson(uint32_t now) {
  IPAddress cur = WiFi.localIP();

  String json = "{";
  json += "\"machine_name\":\"" + String(MACHINE_NAME) + "\",";
  json += "\"firmware_version\":\"" + String(FIRMWARE_VERSION) + "\",";
  json += "\"ip\":\"" + ipToString(cur) + "\",";
  json += "\"millis\":" + String(now) + ",";
  json += "\"machineRun\":" + String(machineRun ? "true" : "false") + ",";
  json += "\"state\":" + String((int)state) + ",";
  json += "\"stateName\":\"" + String(stateName(state)) + "\",";
  json += "\"run_mode\":\"" + String(runMode == MODE_AUTO ? "auto" : "manual") + "\",";
  json += "\"sensor\":" + String(sensor ? "true" : "false") + ",";
  // json += "\"sensor_1\":" + String(sensor1 ? "true" : "false") + ",";
  // json += "\"sensor_2\":" + String(sensor2 ? "true" : "false") + ",";
  json += "\"in_open\":" + String(outInOpen ? "true" : "false") + ",";
  json += "\"out_open\":" + String(outOutOpen ? "true" : "false") + ",";
  json += "\"fill_active\":" + String(outFillRelay ? "true" : "false") + ",";
  json += "\"conveyor_run\":" + String(outConveyorRun ? "true" : "false") + ",";
  json += "\"alarm_latched\":" + String(alarmLatched ? "true" : "false") + ",";
  json += "\"alarms\":" + String(alarms) + ",";
  json += "\"alarm_text\":\"" + String(firstAlarmText(alarms)) + "\",";
  json += "\"alarm_list\":\"" + alarmTextFromBits(alarms) + "\",";
  json += "\"total\":" + String(bottlesTotal) + ",";
  json += "\"cycles\":" + String(cyclesTotal) + ",";
  json += "\"bpm\":" + String(bpm) + ",";
  json += "\"avg_bpm\":" + String(avgBpm, 2) + ",";
  json += "\"params\":{";
  json += "\"delay\":" + String(P.tDelayFill, 3) + ",";
  json += "\"fill\":" + String(P.tFill, 3) + ",";
  json += "\"indelay\":" + String(P.tInDelay, 3) + ",";
  json += "\"outmin\":" + String(P.tOutMin, 3) + ",";
  json += "\"filter\":" + String(P.tFilter, 3);
  json += "}";
  json += "}";

  return json;
}
// =====================================================
// HTTP API
// =====================================================
static void handleHttpOnce(uint32_t now) {
  (void)now;

  WiFiClient c = httpServer.available();
  if (!c) return;

  String req = "";
  uint32_t t0 = millis();

  while (c.connected() && (millis() - t0) < 1200) {
    while (c.available()) {
      char ch = (char)c.read();
      req += ch;
      if (req.endsWith("\r\n\r\n")) break;
    }
    if (req.endsWith("\r\n\r\n")) break;
  }

  if (req.length() == 0) {
    c.stop();
    return;
  }

  int sp1 = req.indexOf(' ');
  int sp2 = req.indexOf(' ', sp1 + 1);
  if (sp1 < 0 || sp2 < 0) {
    c.stop();
    return;
  }

  String path = req.substring(sp1 + 1, sp2);
  String route = path;
  int q = route.indexOf('?');
  if (q >= 0) route = route.substring(0, q);

  if (route == "/api/status") {
    httpSendJson(c, buildJsonStatus());
    c.stop();
    return;
  }

  if (route == "/api/run") {
    String setS = getQueryParam(path, "set");
    if (!setS.length()) {
      httpSendText(c, "400 Bad Request", "missing set=0 or set=1");
      c.stop();
      return;
    }

    int v = setS.toInt();
    machineRun = (v != 0);

    if (!machineRun) {
      enterState(ST_WAIT_BOTTLE, millis());
    }

    httpSendText(c, "200 OK", machineRun ? "OK RUN=1" : "OK RUN=0");
    c.stop();
    return;
  }

  if (route == "/api/alarm/reset") {
    clearAlarms(millis());
    machineRun = false;
    httpSendText(c, "200 OK", "OK alarms reset");
    c.stop();
    return;
  }

  if (route == "/api/set") {
    String delayS   = getQueryParam(path, "delay");
    String fillS    = getQueryParam(path, "fill");
    String indelayS = getQueryParam(path, "indelay");
    String outminS  = getQueryParam(path, "outmin");
    String filterS  = getQueryParam(path, "filter");

    if (delayS.length())   P.tDelayFill = clampf(delayS.toFloat(),   0.00f, 10.0f);
    if (fillS.length())    P.tFill      = clampf(fillS.toFloat(),    0.00f, 10.0f);
    if (indelayS.length()) P.tInDelay   = clampf(indelayS.toFloat(), 0.00f, 10.0f);
    if (outminS.length())  P.tOutMin    = clampf(outminS.toFloat(),  0.00f, 10.0f);
    if (filterS.length())  P.tFilter    = clampf(filterS.toFloat(),  0.01f, 1.00f);

    sensor1Filter.setSec(P.tFilter);
    // sensor2Filter.setSec(P.tFilter);
    bool saved = persist_write();

    String json = "{";
    json += "\"ok\":" + String(saved ? "true" : "false") + ",";
    json += "\"params\":{";
    json += "\"delay\":" + String(P.tDelayFill, 3) + ",";
    json += "\"fill\":" + String(P.tFill, 3) + ",";
    json += "\"indelay\":" + String(P.tInDelay, 3) + ",";
    json += "\"outmin\":" + String(P.tOutMin, 3) + ",";
    json += "\"filter\":" + String(P.tFilter, 3);
    json += "}}";

    httpSendJson(c, json);
    c.stop();
    return;
  }

  if (route == "/api/net") {
    String mode = getQueryParam(path, "mode");
    if (!mode.length()) {
      httpSendText(c, "400 Bad Request", "missing mode=dhcp|static");
      c.stop();
      return;
    }

    if (mode.equalsIgnoreCase("dhcp")) {
      netCfg.mode = NET_DHCP;
      httpSendText(c, "200 OK", "OK mode=dhcp reboot required to apply");
      c.stop();
      return;
    }

    if (mode.equalsIgnoreCase("static")) {
      String ipS = getQueryParam(path, "ip");
      String gwS = getQueryParam(path, "gw");
      String snS = getQueryParam(path, "sn");

      if (!ipS.length() || !gwS.length() || !snS.length()) {
        httpSendText(c, "400 Bad Request", "missing ip/gw/sn");
        c.stop();
        return;
      }

      uint8_t ipb[4], gwb[4], snb[4];
      if (!parseIpStr(ipS, ipb)) { httpSendText(c, "400 Bad Request", "bad ip"); c.stop(); return; }
      if (!parseIpStr(gwS, gwb)) { httpSendText(c, "400 Bad Request", "bad gw"); c.stop(); return; }
      if (!parseIpStr(snS, snb)) { httpSendText(c, "400 Bad Request", "bad sn"); c.stop(); return; }

      netCfg.mode = NET_STATIC;
      memcpy(netCfg.ip, ipb, 4);
      memcpy(netCfg.gw, gwb, 4);
      memcpy(netCfg.sn, snb, 4);

      httpSendText(c, "200 OK", "OK mode=static reboot required to apply");
      c.stop();
      return;
    }

    httpSendText(c, "400 Bad Request", "mode must be dhcp or static");
    c.stop();
    return;
  }
  if (route == "/api/mode") {
    String set = getQueryParam(path, "set");

    if (set.equalsIgnoreCase("auto")) {
      runMode = MODE_AUTO;
      manInOpen = false;
      manOutOpen = false;
      manConveyorRun = false;
      manFillPulseReq = false;
      manFillPulseStartMs = 0;
      enterState(ST_WAIT_BOTTLE, millis());
      httpSendText(c, "200 OK", "OK mode=auto");
      c.stop();
      return;
    }

    if (set.equalsIgnoreCase("manual")) {
      runMode = MODE_MANUAL;
      machineRun = false;
      manInOpen = false;
      manOutOpen = false;
      manConveyorRun = false;
      manFillPulseReq = false;
      manFillPulseStartMs = 0;
      httpSendText(c, "200 OK", "OK mode=manual");
      c.stop();
      return;
    }

    httpSendText(c, "400 Bad Request", "use set=auto|manual");
    c.stop();
    return;
  }

  if (route == "/api/manual") {
    if (runMode != MODE_MANUAL) {
      httpSendText(c, "409 Conflict", "ERROR: not in manual mode");
      c.stop();
      return;
    }

    String inS = getQueryParam(path, "in");
    String outS = getQueryParam(path, "out");
    String conveyorS = getQueryParam(path, "conveyor");
    String pulseS = getQueryParam(path, "pulse");

    if (inS.length()) manInOpen = (inS.toInt() != 0);
    if (outS.length()) manOutOpen = (outS.toInt() != 0);
    if (conveyorS.length()) manConveyorRun = (conveyorS.toInt() != 0);

    if (pulseS.length() && pulseS.toInt() != 0) {
      manFillPulseReq = true;
      manFillPulseStartMs = 0;
    }

    httpSendText(c, "200 OK", "OK");
    c.stop();
    return;
  }       

  httpSendText(c, "404 Not Found", "Not found");
  c.stop();
}

static void serviceUdpTelemetry(uint32_t now) {
  bool changed = false;

  if ((int)state != lastSentState) changed = true;
  if (alarms != lastSentAlarms) changed = true;
  if (bottlesTotal != lastSentTotal) changed = true;
  if (cyclesTotal != lastSentCycles) changed = true;
  if (machineRun != lastSentRun) changed = true;

  if (P.tDelayFill != lastSentDelay) changed = true;
  if (P.tFill != lastSentFill) changed = true;
  if (P.tInDelay != lastSentInDelay) changed = true;
  if (P.tOutMin != lastSentOutMin) changed = true;
  if (P.tFilter != lastSentFilter) changed = true;

  if (changed || (now - lastUdpSendMs) >= UDP_HEARTBEAT_MS) {
    sendUdpStatusNow(now);

    lastUdpSendMs = now;
    lastSentState = (int)state;
    lastSentAlarms = alarms;
    lastSentTotal = bottlesTotal;
    lastSentCycles = cyclesTotal;
    lastSentRun = machineRun;

    lastSentDelay = P.tDelayFill;
    lastSentFill = P.tFill;
    lastSentInDelay = P.tInDelay;
    lastSentOutMin = P.tOutMin;
    lastSentFilter = P.tFilter;
  }
}
// =====================================================
// WIFI
// =====================================================
static const char* wifiStatusStr(uint8_t s) {
  switch (s) {
    case WL_NO_SHIELD: return "WL_NO_SHIELD";
    case WL_IDLE_STATUS: return "WL_IDLE_STATUS";
    case WL_NO_SSID_AVAIL: return "WL_NO_SSID_AVAIL";
    case WL_SCAN_COMPLETED: return "WL_SCAN_COMPLETED";
    case WL_CONNECTED: return "WL_CONNECTED";
    case WL_CONNECT_FAILED: return "WL_CONNECT_FAILED";
    case WL_CONNECTION_LOST: return "WL_CONNECTION_LOST";
    case WL_DISCONNECTED: return "WL_DISCONNECTED";
    default: return "WL_???";
  }
}

static void setupWifiFromSettings() {
  if (netCfg.mode == NET_STATIC) {
    IPAddress ip = ipFrom4(netCfg.ip);
    IPAddress gw = ipFrom4(netCfg.gw);
    IPAddress sn = ipFrom4(netCfg.sn);
    WiFi.config(ip, gw, sn);
  }

  WiFi.disconnect();
  delay(300);

  Serial.println("Connecting WiFi...");
  Serial.print("SSID: ");
  Serial.println(wifiCfg.ssid);

  WiFi.begin(wifiCfg.ssid, wifiCfg.pass);

  uint32_t start = millis();
  while (WiFi.status() != mWL_CONNECTED && (millis() - start) < 20000UL) {
    Serial.print(".");
    delay(500);
  }
  Serial.println();

  Serial.print("WiFi status: ");
  Serial.println(wifiStatusStr(WiFi.status()));

  if (WiFi.status() == mWL_CONNECTED) {
    Serial.print("IP: ");
    Serial.println(WiFi.localIP());
    Serial.print("RSSI: ");
    Serial.println(WiFi.RSSI());
  }
}

// =====================================================
// INPUT UPDATE
// =====================================================
static void updateInputs(uint32_t now) {
  sensor1 = sensor1Filter.update(readSensor1Raw(), now);
  // sensor2 = sensor2Filter.update(readSensor2Raw(), now);
  sensor = sensor1; // || sensor2;   // compatibility / station occupied

  static bool pbRawLast = false;
  static uint32_t pbRawChangeMs = 0;
  static const uint32_t PB_FILTER_MS = 40;

  bool rawPb = readPbRaw();

  if (rawPb != pbRawLast) {
    pbRawLast = rawPb;
    pbRawChangeMs = now;
  }

  if ((now - pbRawChangeMs) >= PB_FILTER_MS) {
    pbStable = pbRawLast;
  }
}

static void updatePushbuttonLogic(uint32_t now) {
  bool pbRise = (pbStable && !pbLastStable);
  bool pbFall = (!pbStable && pbLastStable);

  if (pbRise) {
    pbPressed = true;
    pbLongHandled = false;
    pbPressStartMs = now;
  }

  if (pbPressed && pbStable && !pbLongHandled) {
    if ((now - pbPressStartMs) >= T_LONG_PRESS_MS) {
      pbLongHandled = true;
      clearAlarms(now);
      machineRun = false;
    }
  }

  if (pbFall && pbPressed) {
    if (!pbLongHandled) {
      machineRun = !machineRun;
      if (!machineRun) {
        enterState(ST_WAIT_BOTTLE, now);
      }
    }
    pbPressed = false;
  }

  pbLastStable = pbStable;
}

// =====================================================
// ALARMS
// =====================================================
static void evaluateAlarms(uint32_t now) {
  bool sensorShouldClear =
      (state == ST_WAIT_BOTTLE) ||
      //(state == ST_WAIT_2ND_BOTTLE) ||
      (state == ST_OUT_OPENING) ||
      (state == ST_IN_REOPEN_DELAY);

  if (sensorShouldClear && sensor) {
    if (sensorOnStartMs == 0) sensorOnStartMs = now;
    if ((now - sensorOnStartMs) >= T_SENSOR_STUCK_ON_MS) {
      raiseAlarm(ALM_SENSOR_STUCK_ON, now);
    }
  } else {
    sensorOnStartMs = 0;
  }

  if (cycleStartMs != 0 && (now - cycleStartMs) > T_CYCLE_TIMEOUT_MS) {
    raiseAlarm(ALM_CYCLE_TIMEOUT, now);
  }

  if (state == ST_OUT_OPENING && outOpenStartMs != 0) {
    if (sensor && (now - outOpenStartMs) > T_EXIT_TIMEOUT_MS) {
      raiseAlarm(ALM_EXIT_TIMEOUT, now);
    }
  }
}


// =====================================================
// MACHINE LOGIC
// =====================================================
static void runMachine(uint32_t now) {
  outFillRelay = false;

  if (runMode == MODE_MANUAL) {
    outInOpen = manInOpen;
    outOutOpen = manOutOpen;
    outConveyorRun = manConveyorRun;

    if (manFillPulseReq) {
      if (manFillPulseStartMs == 0) manFillPulseStartMs = now;
      outFillRelay = true;

      if ((now - manFillPulseStartMs) >= s_to_ms(FILL_PULSE_S) ||
          (now - manFillPulseStartMs) >= FILL_FAILSAFE_MS) {
        outFillRelay = false;
        manFillPulseReq = false;
        manFillPulseStartMs = 0;
      }
    } else {
      outFillRelay = false;
    }

    return;
  }

  if (!machineRun) {
    outConveyorRun = true;   // stopped until operator presses Start
    cycleStartMs = 0;
    enterState(ST_WAIT_BOTTLE, now);
    return;
  }

  if (alarmLatched) {
    enterState(ST_ALARM, now);
    return;
  }

  switch (state) {
    case ST_WAIT_BOTTLE:
      outInOpen = true;
      outOutOpen = false;
      outConveyorRun = false;   // conveyor running/stop semantics preserved from your 3.1.2 behavior

      if (sensor1) {
        enterState(ST_DELAY_BEFORE_FILL, now);
      }
      break;
/*
    case ST_WAIT_2ND_BOTTLE:
      outInOpen = true;
      outOutOpen = false;
      outConveyorRun = false;

      if (!sensor1) {
        enterState(ST_WAIT_BOTTLE, now);
      } else if (sensor2) {
        enterState(ST_DELAY_BEFORE_FILL, now);
      }
      break;
*/
    case ST_DELAY_BEFORE_FILL:
      if ((now - tStateMs) >= s_to_ms(P.tDelayFill)) {
        outConveyorRun = true;   // preserve your current conveyor semantics
        fillPulseStartMs = now;
        outInOpen = false;
        enterState(ST_FILL_PULSE, now);
      }
      break;

    case ST_FILL_PULSE:
      outFillRelay = true;
      if ((now - fillPulseStartMs) >= s_to_ms(FILL_PULSE_S) ||
          (now - fillPulseStartMs) >= FILL_FAILSAFE_MS) {
        outFillRelay = false;
        enterState(ST_WAIT_FILL_DONE, now);
      }
      break;

    case ST_WAIT_FILL_DONE:
      if ((now - tStateMs) >= s_to_ms(P.tFill)) {
        bottlesTotal += 1;
        cyclesTotal += 1;
        count10s += 1;
        enterState(ST_OUT_OPENING, now);
      }
      break;

    case ST_OUT_OPENING:
      outOutOpen = true;
      outConveyorRun = false;
      if ((now - tStateMs) >= s_to_ms(P.tOutMin)) {
        enterState(ST_IN_REOPEN_DELAY, now);
      }
      break;

    case ST_IN_REOPEN_DELAY:
      outOutOpen = false;
      if ((now - tStateMs) >= s_to_ms(P.tInDelay)) {
        cycleStartMs = 0;
        outInOpen = true;
        enterState(ST_WAIT_BOTTLE, now);
      }
      break;

    case ST_ALARM:
      outFillRelay = false;
      outOutOpen = false;
      outInOpen = true;
      outConveyorRun = true;   // preserve your current alarm-stop behavior
      break;

    default:
      break;
  }
}

// =====================================================
// BPM
// =====================================================
static void updateBpm(uint32_t now) {
  if ((now - windowStartMs) >= 10000UL) {
    bpm = (uint16_t)(count10s * 6UL);

    bpmSamples++;
    if (bpmSamples == 1) avgBpm = (float)bpm;
    else avgBpm = avgBpm + (((float)bpm - avgBpm) / (float)bpmSamples);

    count10s = 0;
    windowStartMs = now;
  }
}

// =====================================================
// DEBUG
// =====================================================
static void printStatusEvery1s(uint32_t now) {
  static uint32_t lastPrint = 0;
  if ((now - lastPrint) < 1000UL) return;
  lastPrint = now;

  Serial.print("MACHINE=");
  Serial.print(MACHINE_NAME);
  Serial.print(" FW=");
  Serial.print(FIRMWARE_VERSION);
  Serial.print(" RUN=");
  Serial.print(machineRun ? "1" : "0");
  Serial.print(" STATE=");
  Serial.print(stateName(state));
  Serial.print(" S1=");
  Serial.print(sensor1 ? "1" : "0");
  // Serial.print(" S2=");
  // Serial.print(sensor2 ? "1" : "0");
  Serial.print(" IN=");
  Serial.print(outInOpen ? "1" : "0");
  Serial.print(" OUT=");
  Serial.print(outOutOpen ? "1" : "0");
  Serial.print(" FILL=");
  Serial.print(outFillRelay ? "1" : "0");
  Serial.print(" CONV=");
  Serial.print(outConveyorRun ? "1" : "0");
  Serial.print(" ALARMS=");
  Serial.print(alarms);
  Serial.print(" TOTAL=");
  Serial.print(bottlesTotal);
  Serial.print(" CYCLES=");
  Serial.print(cyclesTotal);
  Serial.print(" BPM=");
  Serial.print(bpm);
  Serial.print(" AVG=");
  Serial.println(avgBpm, 1);
}

// =====================================================
// SETUP
// =====================================================
void setup() {
  Serial.begin(115200);
  delay(300);

  pinMode(PIN_SENSOR_1, INPUT);
  // pinMode(PIN_SENSOR_2, INPUT);
  pinMode(PIN_STARTSTOP_PB, INPUT);

  pinMode(PIN_IN_OPEN, OUTPUT);
  pinMode(PIN_OUT_OPEN, OUTPUT);
  pinMode(PIN_FILL_RELAY, OUTPUT);
  pinMode(PIN_CONVEYOR, OUTPUT);

  pinMode(LED_IN, OUTPUT);
  pinMode(LED_OUT, OUTPUT);
  pinMode(LED_FILL, OUTPUT);
  pinMode(LED_SENSOR, OUTPUT);

  if (!persist_read()) {
    persist_write();
  }

  sensor1Filter.setSec(P.tFilter);
  // sensor2Filter.setSec(P.tFilter);

  enterState(ST_WAIT_BOTTLE, millis());
  windowStartMs = millis();

  applyOutputs();

  setupWifiFromSettings();
  httpServer.begin();
  udp.begin(UDP_SERVER_PORT);
  Serial.println("OPTA Version 3.1 started");
}

// =====================================================
// LOOP
// =====================================================
void loop() {
  uint32_t now = millis();

  handleHttpOnce(now);
  updateInputs(now);
  updatePushbuttonLogic(now);
  evaluateAlarms(now);
  runMachine(now);
  updateBpm(now);
  applyOutputs();
  serviceUdpTelemetry(now);
  printStatusEvery1s(now);

  static uint32_t lastWifiDbg = 0;
  if (WiFi.status() != mWL_CONNECTED) {
    if ((now - lastWifiDbg) > 5000UL) {
      lastWifiDbg = now;
      Serial.print("[WiFi] status=");
      Serial.print((int)WiFi.status());
      Serial.print(" ");
      Serial.println(wifiStatusStr(WiFi.status()));
      setupWifiFromSettings();
    }
  }
}
