#include <Ethernet.h>

/*
  Arduino OPTA - Version 1.2.3.8
  Reduced I/O + Ethernet (PortentaEthernet) + lightweight JSON API for
  external HMI

  WiFi has been removed in favor of Ethernet. Production needs a stable
  network for HMI control during filling -- WiFi was occasionally
  dropping and stalling cycles. If you need WiFi instead, flash
  version 1.2.3.4 (kept in C:\Users\zamor\Downloads\OPTA_Filler_Indexer_v1_2_3_4).

  OUTPUTS
    D0 = IN piston relay
    D1 = OUT piston relay
    D2 = FILL relay
    D3 = CONVEYOR relay

  INPUTS
    A0 = bottle sensor
    A1 = filler done sensor (optional)
    A2 = start/stop pushbutton

  Pushbutton behavior
    Short press = Start / Stop toggle
    Long press  = Reset alarms

  API
    GET /api/status
    GET /api/run?set=1
    GET /api/run?set=0
    GET /api/alarm/reset
    GET /api/set?delay=..&fill=..&indelay=..&outmin=..&filter=..&bottleconfirm=..
    GET /api/set?reset_total=1
    GET /api/set?reset_cycles=1
    GET /api/set?total=123
    GET /api/set?cycles=456
    GET /api/set?lot=LOT123
    GET /api/net?mode=dhcp
    GET /api/net?mode=static&ip=..&gw=..&sn=..
    GET /api/mode?set=auto
    GET /api/mode?set=manual
    GET /api/manual?in=1&out=0&conveyor=1&pulse=1

  Version 1.2.3.8 updates
    - Switched to Ethernet (PortentaEthernet) as the only network
      transport. WiFi was dropping intermittently in production, which
      stalled HMI control during fill cycles. Built on top of the clean
      1.2.3.4 baseline -- WiFi state machine, types, and helpers
      replaced 1:1 with Ethernet equivalents. Same IPs (10.1.10.217
      TEST, 10.1.10.218 PROD) so existing HMI URLs keep working.
    - HTTP handler rewritten to use a stack-local EthernetClient (the
      pattern the stock Arduino WebServer example uses). The static
      WiFiClient pattern from 1.2.3.4 worked fine on WiFi for years
      but does not translate to EthernetClient on the Portenta -- the
      underlying mbed::TCPSocket isn't released between requests, so
      subsequent connections sit in the accept queue forever. With the
      client as a stack local, the destructor releases the socket on
      every return. Bounded to 200 ms total (well under the 200 ms
      fill-relay window); safe because HTTP only runs while
      !isFillingActive().

  Version 1.2.3.4 updates
    - Removed the fill relay watchdog Timeout (it was producing false
      ALM_FILL_RELAY_TIMEOUT trips). Only the pulse limiter remains, which
      is the timer that actually drops the relay. With loop gating and
      non-blocking Wi-Fi, the pulse limiter is sufficient on its own.

  Version 1.2.3.3 updates
    - Re-enabled hardware-Timeout fill relay shutoff so the relay drops
      automatically even if loop() is blocked.
    - Loop now skips HTTP, UDP, persistence, debug print, and WiFi
      service while filling is active (ST_FILL_PULSE + ST_WAIT_FILL_DONE)
      so non-critical work can never extend the fill window.
    - WiFi connect/reconnect rewritten as a non-blocking state machine.
      No more delay() or 20s while-loops in the WiFi path.

  Version 1.2.3.2 updates
    - Added hardware fill relay pulse limiter to prevent false relay-timeout alarms
    - Bottle must be present for minimum confirm time before cycle starts
    - New STARTUP_CLEAR state clears occupied station on start
    - Stop-requested cycle finishes and blocks next bottle from entering
    - Optional lot number persisted and reported in status
    - Added bottleconfirm parameter and extra debug fields
    - Alarm reset no longer auto-starts the conveyor path
    - Start command now requires alarm reset first
    - Optional A1 fill-done confirmation input with timed fallback
*/
// Uncomment the next line to enable TEST mode
#define TEST_MODE   // Comment this out for production

#include <Arduino.h>
#include <Ethernet.h>
#include <cstddef>
#include <cmath>
#include <cstring>
#include "mbed.h"

using namespace mbed;

// =====================================================
// FORWARD DECLARATIONS
// =====================================================
enum CycleState : uint8_t;
static void enterState(CycleState s, uint32_t now);
static String buildUdpJson(uint32_t now);

// =====================================================
// MACHINE ID
// =====================================================
static const char* FIRMWARE_VERSION = "1.2.3.8";
#ifdef TEST_MODE
  static const char* MACHINE_NAME = "DRLZ-FILLER-INDEXER-01";
  static const uint8_t UDP_SERVER_IP[4] = {10, 1, 10, 55};
#else
  static const char* MACHINE_NAME = "DRLZ-FILLER-INDEXER-02";
  static const uint8_t UDP_SERVER_IP[4] = {10, 1, 10, 58};
#endif
// =====================================================
// UDP TELEMETRY
// =====================================================
static EthernetUDP udp;

// CHANGE THIS TO YOUR WINDOWS TELEMETRY SERVER IP

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
static float lastSentBottleConfirm = -999.0f;
static String lastSentLot = "";

// =====================================================
// TYPES
// =====================================================
enum CycleState : uint8_t {
  ST_WAIT_BOTTLE = 0,
  ST_STARTUP_CLEAR,
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
  ALM_BOTTLE_NOT_CLEARED    = 1u << 1,
  ALM_CYCLE_TIMEOUT         = 1u << 2,
  ALM_STARTUP_CLEAR_TIMEOUT = 1u << 3
};

// =====================================================
// PIN MAP
// =====================================================
static const uint8_t PIN_SENSOR_1      = A0;
static const uint8_t PIN_FILL_DONE     = A1;
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
static const bool FILL_DONE_ACTIVE_HIGH = true;
static const bool PB_ACTIVE_HIGH = true;

// If relay HIGH means conveyor RUN, keep true.
// If relay HIGH means conveyor STOP, change to false.
static const bool CONVEYOR_HIGH_MEANS_RUN = false;

enum RunMode : uint8_t { MODE_AUTO = 0, MODE_MANUAL = 1 };
static RunMode runMode = MODE_AUTO;

// Manual command bits
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
#ifdef TEST_MODE
  NET_STATIC,
  {10, 1, 10, 217},
  {10, 1, 10, 1},
  {255, 255, 255, 0}
#else
  NET_STATIC,
  {10, 1, 10, 218},
  {10, 1, 10, 1},
  {255, 255, 255, 0}
#endif
};

static EthernetServer httpServer(80);

// =====================================================
// PARAMETERS
// =====================================================
struct Params {
  float tDelayFill;
  float tFill;
  float tInDelay;
  float tOutMin;
  float tFilter;
  float tBottleConfirm;
};

static Params P = {
  1.00f,
  4.80f,
  1.35f,
  2.25f,
  0.05f,
  0.10f
};

// =====================================================
// RUNTIME COUNTERS / PERSISTENCE DIRTY FLAGS
// =====================================================
static bool persistDirty = false;
static uint32_t lastPersistMs = 0;
static const uint32_t PERSIST_SAVE_INTERVAL_MS = 15000UL;

// =====================================================
// PERSISTENCE
// =====================================================
static const uint32_t PERSIST_MAGIC   = 0x4F505431; // OPT1
static const uint32_t PERSIST_VERSION = 3;

struct PersistData {
  Params p;
  uint32_t bottlesTotal;
  uint32_t cyclesTotal;
  char lotNumber[32];
};

struct Persist {
  uint32_t magic;
  uint32_t version;
  uint32_t crc;
  PersistData d;
};

static Persist S;

static FlashIAP flash;
static uint32_t flash_base_addr = 0;
static uint32_t flash_erase_total = 0;
static uint32_t flash_prog_size = 0;
static bool stopRequested = false;
static bool justStartedRun = false;
static char lotNumber[32] = "";

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
  const uint8_t* payload = (const uint8_t*)&x.d;
  const size_t len = sizeof(Persist) - offsetof(Persist, d);
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

  if (isnan(tmp.d.p.tDelayFill) || isnan(tmp.d.p.tFill) || isnan(tmp.d.p.tInDelay) ||
      isnan(tmp.d.p.tOutMin) || isnan(tmp.d.p.tFilter) || isnan(tmp.d.p.tBottleConfirm)) return false;

  S = tmp;
  P = S.d.p;

  extern uint32_t bottlesTotal;
  extern uint32_t cyclesTotal;
  bottlesTotal = S.d.bottlesTotal;
  cyclesTotal = S.d.cyclesTotal;

  memcpy(lotNumber, S.d.lotNumber, sizeof(lotNumber));
  lotNumber[sizeof(lotNumber) - 1] = '\0';

  return true;
}

static bool persist_write() {
  static bool region_ok = false;
  if (!region_ok) {
    if (!flash_prepare_region(sizeof(Persist))) return false;
    region_ok = true;
  }

  extern uint32_t bottlesTotal;
  extern uint32_t cyclesTotal;

  S.magic = PERSIST_MAGIC;
  S.version = PERSIST_VERSION;
  S.d.p = P;
  S.d.bottlesTotal = bottlesTotal;
  S.d.cyclesTotal = cyclesTotal;
  memset(S.d.lotNumber, 0, sizeof(S.d.lotNumber));
  strncpy(S.d.lotNumber, lotNumber, sizeof(S.d.lotNumber) - 1);
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

static bool persist_save_all() {
  return persist_write();
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

static bool parseUnsignedStrict(String s, uint32_t& out) {
  s.trim();
  if (!s.length()) return false;
  for (int i = 0; i < (int)s.length(); i++) {
    if (s[i] < '0' || s[i] > '9') return false;
  }
  unsigned long v = strtoul(s.c_str(), nullptr, 10);
  out = (uint32_t)v;
  return true;
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
static StableFilter fillDoneFilter;

// =====================================================
// RUNTIME
// =====================================================
static const float FILL_PULSE_S = 0.20f;
static const uint32_t FILL_FAILSAFE_MS             = 1000UL;
// The pulse limiter is a hardware timer, so the fill relay turns off even if
// Wi-Fi/API handling delays the next pass through loop().
static const uint32_t FILL_RELAY_PULSE_LIMIT_MS    = 250UL;
static const uint32_t T_SENSOR_STUCK_ON_MS         = 8000UL;
static const uint32_t T_BOTTLE_CLEAR_TIMEOUT_MS    = 3000UL;
static const uint32_t T_SENSOR_RESET_CONFIRM_MS    = 250UL;
static const uint32_t T_CYCLE_TIMEOUT_MS           = 15000UL;
static const uint32_t T_LONG_PRESS_MS              = 2500UL;
static const uint32_t T_STARTUP_CLEAR_TIMEOUT_MS   = 5000UL;
static const uint32_t T_FILL_DONE_CONFIRM_MS       = 60UL;
static const uint32_t T_FILL_DONE_MIN_MS           = 80UL;

static CycleState state = ST_WAIT_BOTTLE;

static bool machineRun = false;
static bool sensor1 = false;
static bool sensor1Prev = false;
static bool sensor1Rise = false;
static bool sensor1Fall = false;
static bool sensor = false;   // compatibility / station occupied
static bool fillDone = false;
static bool fillDonePrev = false;
static bool fillDoneRise = false;
static bool fillDoneFall = false;
static bool fillDoneSeenInactiveSinceCycle = false;
static uint32_t fillDoneConfirmStartMs = 0;

static uint32_t tStateMs = 0;
static uint32_t cycleStartMs = 0;
static uint32_t fillPulseStartMs = 0;
static uint32_t outOpenStartMs = 0;
static uint32_t sensorOnStartMs = 0;
static uint32_t sensorResetStartMs = 0;
static uint32_t bottleSeenStartMs = 0;
static bool bottleConfirmActive = false;
static uint32_t startupClearStartMs = 0;

// Production
uint32_t bottlesTotal = 0;
uint32_t cyclesTotal = 0;
static uint32_t count10s = 0;
static uint16_t bpm = 0;
static float avgBpm = 0.0f;
static uint32_t bpmSamples = 0;
static uint32_t windowStartMs = 0;

// Alarms
static uint32_t alarms = 0;
static bool alarmLatched = false;

// Pushbutton
static bool pbStable = false;
static bool pbLastStable = false;
static bool pbPressed = false;
static bool pbLongHandled = false;
static uint32_t pbPressStartMs = 0;

// Outputs
static bool outInOpen = true;
static bool outOutOpen = false;
static bool outFillRelay = false;
static bool outConveyorRun = true;
static Timeout fillRelayPulseLimiter;
static bool fillRelayPulseLimiterArmed = false;
static bool fillRelayOutputState = false;
static volatile bool fillRelayPulseLimited = false;

// =====================================================
// INPUTS
// =====================================================
static bool readSensor1Raw() {
  bool v = (digitalRead(PIN_SENSOR_1) == LOW);
  return SENSOR1_ACTIVE_HIGH ? v : !v;
}

static bool readFillDoneRaw() {
  bool v = (digitalRead(PIN_FILL_DONE) == LOW);
  return FILL_DONE_ACTIVE_HIGH ? v : !v;
}

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
// =====================================================
// FILL RELAY HARDWARE SHUTOFF
// =====================================================
// The fill relay is driven through updateFillRelayOutput() instead of a raw
// digitalWrite. When the relay is turned ON we arm one mbed::Timeout:
//
//   Pulse limiter (FILL_RELAY_PULSE_LIMIT_MS): hardware-driven cap on how
//   long the relay can be on. If loop() ever stalls (Wi-Fi reconnect, HTTP
//   handling, flash erase, etc.) this ISR drops the relay anyway. This is
//   what protects against double/triple fills.
//
// A separate watchdog used to live here as a backup that raised an alarm if
// the pulse limiter ever failed to fire. In practice it produced false
// trips on Portenta H7 (interrupt-ordering races against the Wi-Fi
// co-processor and flash driver), so it was removed. The pulse limiter
// alone has been reliable.
//
// The ISR only calls digitalWrite (interrupt-safe) and sets a volatile
// flag. No Serial, no String, no allocations.
static void forceFillRelayOff() {
  digitalWrite(PIN_FILL_RELAY, LOW);
  digitalWrite(LED_FILL, LOW);
}

static void onFillRelayPulseLimit() {
  fillRelayPulseLimited = true;
  forceFillRelayOff();
}

static void updateFillRelayOutput(bool requestedOn) {
  // Once software releases the request, clear the pulse-limited latch so the
  // next pulse can re-arm.
  if (!requestedOn) {
    fillRelayPulseLimited = false;
  }

  bool safeOn = requestedOn && !fillRelayPulseLimited;

  if (safeOn && !fillRelayOutputState) {
    // OFF -> ON transition: arm the hardware shutoff.
    fillRelayPulseLimited = false;
    fillRelayPulseLimiter.detach();
    fillRelayPulseLimiter.attach_us(callback(onFillRelayPulseLimit),
                                    (us_timestamp_t)FILL_RELAY_PULSE_LIMIT_MS * 1000ULL);
    fillRelayPulseLimiterArmed = true;
  } else if (!safeOn && fillRelayOutputState) {
    // ON -> OFF transition: cancel any pending hardware shutoff.
    if (fillRelayPulseLimiterArmed) {
      fillRelayPulseLimiter.detach();
      fillRelayPulseLimiterArmed = false;
    }
  }

  fillRelayOutputState = safeOn;
  digitalWrite(PIN_FILL_RELAY, safeOn ? HIGH : LOW);
  digitalWrite(LED_FILL, safeOn ? HIGH : LOW);
}

static void applyOutputs() {
  digitalWrite(PIN_IN_OPEN,    outInOpen ? HIGH : LOW);
  digitalWrite(PIN_OUT_OPEN,   outOutOpen ? HIGH : LOW);
  updateFillRelayOutput(outFillRelay);
  writeConveyor(outConveyorRun);

  digitalWrite(LED_IN,     outInOpen ? HIGH : LOW);
  digitalWrite(LED_OUT,    outOutOpen ? HIGH : LOW);
  digitalWrite(LED_SENSOR, sensor ? HIGH : LOW);
}

// =====================================================
// MACHINE HELPERS
// =====================================================
static bool isCycleActiveState(CycleState s) {
  switch (s) {
    case ST_STARTUP_CLEAR:
    case ST_DELAY_BEFORE_FILL:
    case ST_FILL_PULSE:
    case ST_WAIT_FILL_DONE:
    case ST_OUT_OPENING:
    case ST_IN_REOPEN_DELAY:
      return true;
    default:
      return false;
  }
}

// True while the filler is operating: the actual relay-on pulse and the
// post-pulse "wait for fill done" window. While this is true, loop() skips
// HTTP, UDP, persistence, debug Serial print, and Wi-Fi service so nothing
// non-critical can extend the fill window.
static bool isFillingActive() {
  return (state == ST_FILL_PULSE) || (state == ST_WAIT_FILL_DONE);
}

static void resetCommonTimers(uint32_t now) {
  cycleStartMs = 0;
  fillPulseStartMs = 0;
  outOpenStartMs = 0;
  sensorOnStartMs = 0;
  sensorResetStartMs = 0;
  bottleSeenStartMs = 0;
  bottleConfirmActive = false;
  startupClearStartMs = 0;
  fillDoneSeenInactiveSinceCycle = false;
  fillDoneConfirmStartMs = 0;
  tStateMs = now;
}

static void resetMachineToRunReady(uint32_t now) {
  stopRequested = false;
  justStartedRun = false;
  resetCommonTimers(now);

  outInOpen = true;
  outOutOpen = false;
  outFillRelay = false;
  outConveyorRun = false;

  enterState(ST_WAIT_BOTTLE, now);
  applyOutputs();
}

static void resetMachineToStoppedBlocked(uint32_t now) {
  stopRequested = false;
  justStartedRun = false;
  resetCommonTimers(now);

  outInOpen = false;
  outOutOpen = false;
  outFillRelay = false;
  outConveyorRun = false;

  enterState(ST_WAIT_BOTTLE, now);
  applyOutputs();
}

static void enterState(CycleState s, uint32_t now) {
  state = s;
  tStateMs = now;
  if (s == ST_DELAY_BEFORE_FILL) cycleStartMs = now;
}

static void clearAlarms(uint32_t now) {
  alarms = 0;
  alarmLatched = false;
  resetMachineToStoppedBlocked(now);
}

static void raiseAlarm(uint32_t bit, uint32_t now) {
  alarms |= bit;
  alarmLatched = true;
  outFillRelay = false;
  outConveyorRun = false;
  applyOutputs();
  enterState(ST_ALARM, now);
}

static const char* stateName(CycleState s) {
  switch (s) {
    case ST_WAIT_BOTTLE:       return "WAIT_BOTTLE";
    case ST_STARTUP_CLEAR:     return "STARTUP_CLEAR";
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
  if (a & ALM_BOTTLE_NOT_CLEARED) {
    if (s.length()) s += " | ";
    s += "BOTTLE_NOT_CLEARED";
  }
  if (a & ALM_CYCLE_TIMEOUT) {
    if (s.length()) s += " | ";
    s += "CYCLE_TIMEOUT";
  }
  if (a & ALM_STARTUP_CLEAR_TIMEOUT) {
    if (s.length()) s += " | ";
    s += "STARTUP_CLEAR_TIMEOUT";
  }

  return s;
}

static const char* firstAlarmText(uint32_t a) {
  if (a & ALM_SENSOR_STUCK_ON)       return "SENSOR_STUCK_ON";
  if (a & ALM_BOTTLE_NOT_CLEARED)    return "BOTTLE_NOT_CLEARED";
  if (a & ALM_CYCLE_TIMEOUT)         return "CYCLE_TIMEOUT";
  if (a & ALM_STARTUP_CLEAR_TIMEOUT) return "STARTUP_CLEAR_TIMEOUT";
  return "NONE";
}

// Counter helpers
static bool saveNowAndMarkClean(uint32_t now) {
  bool ok = persist_save_all();
  if (ok) {
    persistDirty = false;
    lastPersistMs = now;
  }
  return ok;
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

static void httpSend(EthernetClient& c, const char* status, const char* contentType, const String& body) {
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

static void httpSendText(EthernetClient& c, const char* status, const String& body) {
  httpSend(c, status, "text/plain; charset=utf-8", body);
}

static void httpSendJson(EthernetClient& c, const char* status, const String& body) {
  httpSend(c, status, "application/json; charset=utf-8", body);
}

static void sendUdpStatusNow(uint32_t now) {
  if (Ethernet.linkStatus() != LinkON) return;

  IPAddress serverIp(
    UDP_SERVER_IP[0],
    UDP_SERVER_IP[1],
    UDP_SERVER_IP[2],
    UDP_SERVER_IP[3]
  );

  String payload = buildUdpJson(now);
#ifdef TEST_MODE
  Serial.print("UDP LEN=");
  Serial.println(payload.length());
#endif
  udp.beginPacket(serverIp, UDP_SERVER_PORT);
  udp.write((const uint8_t*)payload.c_str(), payload.length());
  udp.endPacket();
}

// =====================================================
// JSON STATUS
// =====================================================
static String buildJsonStatus(bool includeDetail = true) {
  IPAddress cur = Ethernet.localIP();
  bool includeDiagnostics = includeDetail;

  String json;
  json.reserve(includeDiagnostics ? 900 : 420);
  json = "{";
  json += "\"ok\":true,";
  json += "\"machine_name\":\"" + String(MACHINE_NAME) + "\",";
  json += "\"firmware_version\":\"" + String(FIRMWARE_VERSION) + "\",";
  json += "\"machineRun\":" + String(machineRun ? "true" : "false") + ",";
  json += "\"run_mode\":\"" + String(runMode == MODE_AUTO ? "auto" : "manual") + "\",";
  json += "\"state\":" + String((int)state) + ",";
  json += "\"stateName\":\"" + String(stateName(state)) + "\",";
  json += "\"sensor\":" + String(sensor ? "true" : "false") + ",";
  json += "\"fill_done\":" + String(fillDone ? "true" : "false") + ",";
  json += "\"in_open\":" + String(outInOpen ? "true" : "false") + ",";
  json += "\"out_open\":" + String(outOutOpen ? "true" : "false") + ",";
  json += "\"fill_active\":" + String(outFillRelay ? "true" : "false") + ",";
  json += "\"conveyor_run\":" + String(outConveyorRun ? "true" : "false") + ",";
  json += "\"stop_requested\":" + String(stopRequested ? "true" : "false") + ",";
  json += "\"alarm_latched\":" + String(alarmLatched ? "true" : "false") + ",";
  json += "\"alarms\":" + String(alarms) + ",";
  json += "\"alarm_text\":\"" + String(firstAlarmText(alarms)) + "\",";
  json += "\"alarm_list\":\"" + alarmTextFromBits(alarms) + "\",";
  json += "\"lot_number\":\"" + String(lotNumber) + "\",";
  json += "\"total\":" + String(bottlesTotal) + ",";
  json += "\"cycles\":" + String(cyclesTotal) + ",";
  json += "\"bpm\":" + String(bpm) + ",";
  json += "\"avg_bpm\":" + String(avgBpm, 2) + ",";
  json += "\"params\":{";
  json += "\"delay\":" + String(P.tDelayFill, 3) + ",";
  json += "\"fill\":" + String(P.tFill, 3) + ",";
  json += "\"indelay\":" + String(P.tInDelay, 3) + ",";
  json += "\"outmin\":" + String(P.tOutMin, 3) + ",";
  json += "\"filter\":" + String(P.tFilter, 3) ;
 // json += "\"bottleconfirm\":" + String(P.tBottleConfirm, 3);
  json += "},";
  json += "\"net\":{";
  json += "\"transport\":\"ethernet\",";
  json += "\"link\":" + String(Ethernet.linkStatus() == LinkON ? "true" : "false") + ",";
  json += "\"ip\":\"" + ipToString(cur) + "\"";
  json += "}";
  if (includeDiagnostics) {
    json += ",\"raw\":{";
    json += "\"pin_sensor_1_high\":" + String(digitalRead(PIN_SENSOR_1) == HIGH ? "true" : "false") + ",";
    json += "\"pin_fill_done_high\":" + String(digitalRead(PIN_FILL_DONE) == HIGH ? "true" : "false") + ",";
    json += "\"sensor_active\":" + String(sensor ? "true" : "false") + ",";
    json += "\"fill_done_active\":" + String(fillDone ? "true" : "false");
    json += "},";
    json += "\"debug\":{";
    json += "\"tStateMs\":" + String(tStateMs) + ",";
    json += "\"cycleStartMs\":" + String(cycleStartMs) + ",";
    json += "\"fillPulseStartMs\":" + String(fillPulseStartMs) + ",";
    json += "\"outOpenStartMs\":" + String(outOpenStartMs) + ",";
    json += "\"sensorOnStartMs\":" + String(sensorOnStartMs) + ",";
    json += "\"bottleSeenStartMs\":" + String(bottleSeenStartMs) + ",";
    json += "\"startupClearStartMs\":" + String(startupClearStartMs) + ",";
    json += "\"sensor1Rise\":" + String(sensor1Rise ? "true" : "false") + ",";
    json += "\"sensor1Fall\":" + String(sensor1Fall ? "true" : "false") + ",";
    json += "\"fillDoneRise\":" + String(fillDoneRise ? "true" : "false") + ",";
    json += "\"fillDoneFall\":" + String(fillDoneFall ? "true" : "false") + ",";
    json += "\"fillDoneSeenInactive\":" + String(fillDoneSeenInactiveSinceCycle ? "true" : "false") + ",";
    json += "\"fillDoneConfirmStartMs\":" + String(fillDoneConfirmStartMs) + ",";
    json += "\"fillRelayPulseLimited\":" + String(fillRelayPulseLimited ? "true" : "false") + ",";
   // json += "\"bottleConfirmActive\":" + String(bottleConfirmActive ? "true" : "false") + ",";
    json += "\"justStartedRun\":" + String(justStartedRun ? "true" : "false") + ",";
    json += "\"persistDirty\":" + String(persistDirty ? "true" : "false") + ",";
    json += "\"lastPersistMs\":" + String(lastPersistMs);
    json += "}";
  }
  json += "}";
  return json;
}
static String buildUdpJson(uint32_t now) {
  IPAddress cur = Ethernet.localIP();

  String json;
  json.reserve(420);

  json += "{";
  json += "\"machine_name\":\"" + String(MACHINE_NAME) + "\",";
  json += "\"firmware_version\":\"" + String(FIRMWARE_VERSION) + "\",";
  json += "\"ip\":\"" + ipToString(cur) + "\",";
  json += "\"millis\":" + String(now) + ",";
  json += "\"machineRun\":" + String(machineRun ? "true" : "false") + ",";
  json += "\"state\":" + String((int)state) + ",";
  json += "\"stateName\":\"" + String(stateName(state)) + "\",";
  json += "\"run_mode\":\"" + String(runMode == MODE_AUTO ? "auto" : "manual") + "\",";
  json += "\"sensor\":" + String(sensor ? "true" : "false") + ",";
  json += "\"fill_done\":" + String(fillDone ? "true" : "false") + ",";
  json += "\"in_open\":" + String(outInOpen ? "true" : "false") + ",";
  json += "\"out_open\":" + String(outOutOpen ? "true" : "false") + ",";
  json += "\"fill_active\":" + String(outFillRelay ? "true" : "false") + ",";
  json += "\"conveyor_run\":" + String(outConveyorRun ? "true" : "false") + ",";
  json += "\"stop_requested\":" + String(stopRequested ? "true" : "false") + ",";
  json += "\"alarm_latched\":" + String(alarmLatched ? "true" : "false") + ",";
  json += "\"alarms\":" + String(alarms) + ",";
  json += "\"alarm_text\":\"" + String(firstAlarmText(alarms)) + "\",";
  json += "\"total\":" + String(bottlesTotal) + ",";
  json += "\"cycles\":" + String(cyclesTotal) + ",";
  json += "\"bpm\":" + String(bpm);
  json += "}";

  return json;
}
// =====================================================
// HTTP API
// =====================================================
static void handleHttpOnce(uint32_t now) {
  // Per-iteration EthernetClient. Static held the underlying mbed
  // TCPSocket between iterations, which wasn't reliably released --
  // first request worked, then subsequent connections sat in the
  // accept queue forever. Stack-local means the destructor runs at
  // function exit and frees the socket every time. Same pattern the
  // stock Arduino EthernetServer/WebServer example uses, which has
  // been verified working on this Portenta. The total time spent
  // here is bounded to 200 ms (well under the 200 ms fill-relay
  // window). Safe because handleHttpOnce() runs only while
  // !isFillingActive().
  EthernetClient activeClient = httpServer.available();
  if (!activeClient) return;

  String req;
  req.reserve(256);
  const int HTTP_REQ_MAX_LEN = 1024;

  uint32_t deadline = millis() + 200UL;
  while (activeClient.connected() && (int32_t)(millis() - deadline) < 0) {
    bool gotByte = false;
    while (activeClient.available()) {
      char ch = (char)activeClient.read();
      gotByte = true;
      if (req.length() < HTTP_REQ_MAX_LEN) {
        req += ch;
      } else {
        httpSendText(activeClient, "413 Payload Too Large", "request too large");
        activeClient.stop();
        return;
      }
    }
    if (req.endsWith("\r\n\r\n")) break;
    if (!gotByte) delay(1);
  }

  if (!req.endsWith("\r\n\r\n")) {
    activeClient.stop();
    return;
  }

  EthernetClient& c = activeClient;

  int sp1 = req.indexOf(' ');
  int sp2 = req.indexOf(' ', sp1 + 1);
  if (sp1 < 0 || sp2 < 0) {
    c.stop();
    req = "";
    return;
  }

  String path = req.substring(sp1 + 1, sp2);
  String route = path;
  int q = route.indexOf('?');
  if (q >= 0) route = route.substring(0, q);

  if (route == "/api/status" || route == "/api/status/full") {
    bool fullStatus =
        route == "/api/status/full" ||
        getQueryParam(path, "full") == "1" ||
        !machineRun;

    httpSendJson(c, "200 OK", buildJsonStatus(fullStatus));
    c.stop();
    req = "";
    return;
  }

  if (route == "/api/run") {
    String setS = getQueryParam(path, "set");
    if (!setS.length()) {
      httpSendText(c, "400 Bad Request", "missing set=0 or set=1");
      c.stop();
      req = "";
      return;
    }

    int v = setS.toInt();

    if (v != 0) {
      if (alarmLatched || state == ST_ALARM) {
        httpSendText(c, "409 Conflict", "alarm active - reset alarm first");
        c.stop();
        req = "";
        return;
      }

      stopRequested = false;
      machineRun = true;
      justStartedRun = true;

      httpSendText(c, "200 OK", "OK RUN=1");
      c.stop();
      req = "";
      return;
    } else {
      if (machineRun) {
        if (isCycleActiveState(state)) {
          stopRequested = true;
          httpSendText(c, "200 OK", "OK STOP REQUESTED - cycle will finish");
        } else {
          machineRun = false;
          stopRequested = false;
          justStartedRun = false;
          resetMachineToStoppedBlocked(millis());
          httpSendText(c, "200 OK", "OK RUN=0");
        }
      } else {
        stopRequested = false;
        machineRun = false;
        justStartedRun = false;
        resetMachineToStoppedBlocked(millis());
        httpSendText(c, "200 OK", "OK RUN=0");
      }

      c.stop();
      req = "";
      return;
    }
  }

  if (route == "/api/alarm/reset") {
    clearAlarms(millis());
    machineRun = false;
    justStartedRun = false;
    httpSendText(c, "200 OK", "OK alarms reset");
    c.stop();
    req = "";
    return;
  }

  if (route == "/api/set") {
    String delayS         = getQueryParam(path, "delay");
    String fillS          = getQueryParam(path, "fill");
    String indelayS       = getQueryParam(path, "indelay");
    String outminS        = getQueryParam(path, "outmin");
    String filterS        = getQueryParam(path, "filter");
    String bottleConfirmS = getQueryParam(path, "bottleconfirm");
    String lotS           = getQueryParam(path, "lot");

    String resetTotalS    = getQueryParam(path, "reset_total");
    String resetCyclesS   = getQueryParam(path, "reset_cycles");
    String totalS         = getQueryParam(path, "total");
    String cyclesS        = getQueryParam(path, "cycles");

    bool changed = false;
    bool saved = true;
    uint32_t parsedValue = 0;

    if (delayS.length()) {
      P.tDelayFill = clampf(delayS.toFloat(), 0.00f, 10.0f);
      changed = true;
    }
    if (fillS.length()) {
      P.tFill = clampf(fillS.toFloat(), 0.00f, 10.0f);
      changed = true;
    }
    if (indelayS.length()) {
      P.tInDelay = clampf(indelayS.toFloat(), 0.00f, 10.0f);
      changed = true;
    }
    if (outminS.length()) {
      P.tOutMin = clampf(outminS.toFloat(), 0.00f, 10.0f);
      changed = true;
    }
    if (filterS.length()) {
      P.tFilter = clampf(filterS.toFloat(), 0.01f, 1.00f);
      changed = true;
    }
    if (bottleConfirmS.length()) {
      P.tBottleConfirm = clampf(bottleConfirmS.toFloat(), 0.02f, 1.00f);
      changed = true;
    }

    sensor1Filter.setSec(P.tFilter);

    if (lotS.length()) {
      lotS.trim();
      memset(lotNumber, 0, sizeof(lotNumber));
      if (lotS.length() > (int)sizeof(lotNumber) - 1) {
        lotS = lotS.substring(0, sizeof(lotNumber) - 1);
      }
      strncpy(lotNumber, lotS.c_str(), sizeof(lotNumber) - 1);
      changed = true;
    }

    if (resetTotalS.length() && resetTotalS.toInt() != 0) {
      bottlesTotal = 0;
      changed = true;
    }

    if (resetCyclesS.length() && resetCyclesS.toInt() != 0) {
      cyclesTotal = 0;
      changed = true;
    }

    if (totalS.length()) {
      if (!parseUnsignedStrict(totalS, parsedValue)) {
        httpSendText(c, "400 Bad Request", "invalid total");
        c.stop();
        req = "";
        return;
      }
      bottlesTotal = parsedValue;
      changed = true;
    }

    if (cyclesS.length()) {
      if (!parseUnsignedStrict(cyclesS, parsedValue)) {
        httpSendText(c, "400 Bad Request", "invalid cycles");
        c.stop();
        req = "";
        return;
      }
      cyclesTotal = parsedValue;
      changed = true;
    }

    if (changed) {
      saved = saveNowAndMarkClean(now);
    }

    String json = "{";
    json += "\"ok\":" + String(saved ? "true" : "false") + ",";
    json += "\"lot_number\":\"" + String(lotNumber) + "\",";
    json += "\"total\":" + String(bottlesTotal) + ",";
    json += "\"cycles\":" + String(cyclesTotal) + ",";
    json += "\"params\":{";
    json += "\"delay\":" + String(P.tDelayFill, 3) + ",";
    json += "\"fill\":" + String(P.tFill, 3) + ",";
    json += "\"indelay\":" + String(P.tInDelay, 3) + ",";
    json += "\"outmin\":" + String(P.tOutMin, 3) + ",";
    json += "\"filter\":" + String(P.tFilter, 3) + ",";
    json += "\"bottleconfirm\":" + String(P.tBottleConfirm, 3);
    json += "}}";

    httpSendJson(c, "200 OK", json);
    c.stop();
    req = "";
    return;
  }

  if (route == "/api/net") {
    String mode = getQueryParam(path, "mode");
    if (!mode.length()) {
      httpSendText(c, "400 Bad Request", "missing mode=dhcp|static");
      c.stop();
      req = "";
      return;
    }

    if (mode.equalsIgnoreCase("dhcp")) {
      netCfg.mode = NET_DHCP;
      httpSendText(c, "200 OK", "OK mode=dhcp reboot required to apply");
      c.stop();
      req = "";
      return;
    }

    if (mode.equalsIgnoreCase("static")) {
      String ipS = getQueryParam(path, "ip");
      String gwS = getQueryParam(path, "gw");
      String snS = getQueryParam(path, "sn");

      if (!ipS.length() || !gwS.length() || !snS.length()) {
        httpSendText(c, "400 Bad Request", "missing ip/gw/sn");
        c.stop();
        req = "";
        return;
      }

      uint8_t ipb[4], gwb[4], snb[4];
      if (!parseIpStr(ipS, ipb)) { httpSendText(c, "400 Bad Request", "bad ip"); c.stop(); req = ""; return; }
      if (!parseIpStr(gwS, gwb)) { httpSendText(c, "400 Bad Request", "bad gw"); c.stop(); req = ""; return; }
      if (!parseIpStr(snS, snb)) { httpSendText(c, "400 Bad Request", "bad sn"); c.stop(); req = ""; return; }

      netCfg.mode = NET_STATIC;
      memcpy(netCfg.ip, ipb, 4);
      memcpy(netCfg.gw, gwb, 4);
      memcpy(netCfg.sn, snb, 4);

      httpSendText(c, "200 OK", "OK mode=static reboot required to apply");
      c.stop();
      req = "";
      return;
    }

    httpSendText(c, "400 Bad Request", "mode must be dhcp or static");
    c.stop();
    req = "";
    return;
  }

  if (route == "/api/mode") {
    String set = getQueryParam(path, "set");

    if (set.equalsIgnoreCase("auto")) {
      runMode = MODE_AUTO;
      machineRun = false;
      stopRequested = false;
      justStartedRun = false;

      manInOpen = false;
      manOutOpen = false;
      manConveyorRun = false;
      manFillPulseReq = false;
      manFillPulseStartMs = 0;

      clearAlarms(millis());

      httpSendText(c, "200 OK", "OK mode=auto");
      c.stop();
      req = "";
      return;
    }

    if (set.equalsIgnoreCase("manual")) {
      runMode = MODE_MANUAL;
      machineRun = false;
      stopRequested = false;
      justStartedRun = false;

      manInOpen = false;
      manOutOpen = false;
      manConveyorRun = false;
      manFillPulseReq = false;
      manFillPulseStartMs = 0;

      resetMachineToStoppedBlocked(millis());

      httpSendText(c, "200 OK", "OK mode=manual");
      c.stop();
      req = "";
      return;
    }

    httpSendText(c, "400 Bad Request", "use set=auto|manual");
    c.stop();
    req = "";
    return;
  }

  if (route == "/api/manual") {
    if (runMode != MODE_MANUAL) {
      httpSendText(c, "409 Conflict", "ERROR: not in manual mode");
      c.stop();
      req = "";
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
    req = "";
    return;
  }

  httpSendText(c, "404 Not Found", "Not found");
  c.stop();
  req = "";
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
  if (P.tBottleConfirm != lastSentBottleConfirm) changed = true;
  if (String(lotNumber) != lastSentLot) changed = true;

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
    lastSentBottleConfirm = P.tBottleConfirm;
    lastSentLot = String(lotNumber);
  }
}

// =====================================================
// ETHERNET - non-blocking link monitor
// =====================================================
// Ethernet.begin() is called once at boot (in setup()) with the static
// IP. After that, serviceEthernet() just polls linkStatus() and logs
// link-up / link-down transitions. linkStatus() is a cheap PHY register
// read so polling every loop iteration is fine. We never re-call
// Ethernet.begin() at runtime because that can block for several seconds
// when no cable is plugged in.
enum EthFsm : uint8_t {
  EFSM_WAIT_LINK = 0,
  EFSM_UP,
  EFSM_DOWN
};

static EthFsm ethFsm = EFSM_WAIT_LINK;
static uint32_t ethFsmMs = 0;

static void serviceEthernet(uint32_t now) {
  bool linkUp = (Ethernet.linkStatus() == LinkON);

  switch (ethFsm) {
    case EFSM_WAIT_LINK: {
      if (linkUp) {
        Serial.print("[ETH] link up IP=");
        Serial.println(Ethernet.localIP());
        ethFsmMs = now;
        ethFsm = EFSM_UP;
      }
      break;
    }
    case EFSM_UP: {
      if (!linkUp) {
        Serial.println("[ETH] link lost");
        ethFsmMs = now;
        ethFsm = EFSM_DOWN;
      }
      break;
    }
    case EFSM_DOWN: {
      if (linkUp) {
        Serial.print("[ETH] link restored IP=");
        Serial.println(Ethernet.localIP());
        ethFsmMs = now;
        ethFsm = EFSM_UP;
      }
      break;
    }
  }
}

// =====================================================
// INPUT UPDATE
// =====================================================
static void updateInputs(uint32_t now) {
  bool newSensor1 = sensor1Filter.update(readSensor1Raw(), now);
  bool newFillDone = fillDoneFilter.update(readFillDoneRaw(), now);

  sensor1Rise = (!sensor1Prev && newSensor1);
  sensor1Fall = (sensor1Prev && !newSensor1);
  fillDoneRise = (!fillDonePrev && newFillDone);
  fillDoneFall = (fillDonePrev && !newFillDone);

  sensor1 = newSensor1;
  fillDone = newFillDone;
  sensor = sensor1;

  sensor1Prev = newSensor1;
  fillDonePrev = newFillDone;

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
      stopRequested = false;
      justStartedRun = false;
    }
  }

  if (pbFall && pbPressed) {
    if (!pbLongHandled) {
      if (!machineRun) {
        stopRequested = false;
        machineRun = true;
        justStartedRun = true;
      } else {
        if (isCycleActiveState(state)) {
          stopRequested = true;
        } else {
          machineRun = false;
          stopRequested = false;
          justStartedRun = false;
          resetMachineToStoppedBlocked(now);
        }
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
      (state == ST_STARTUP_CLEAR) ||
      (state == ST_OUT_OPENING) ||
      (state == ST_IN_REOPEN_DELAY);

  if (sensorShouldClear && sensor) {
    if (sensorOnStartMs == 0) sensorOnStartMs = now;
    if ((now - sensorOnStartMs) >= T_SENSOR_STUCK_ON_MS) {
      raiseAlarm(ALM_SENSOR_STUCK_ON, now);
      return;
    }
  } else {
    sensorOnStartMs = 0;
  }

  if (cycleStartMs != 0 && (now - cycleStartMs) > T_CYCLE_TIMEOUT_MS) {
    raiseAlarm(ALM_CYCLE_TIMEOUT, now);
    return;
  }

  if (state == ST_OUT_OPENING && outOpenStartMs != 0) {
    if ((now - outOpenStartMs) > (s_to_ms(P.tOutMin) + T_BOTTLE_CLEAR_TIMEOUT_MS) && sensor) {
      raiseAlarm(ALM_BOTTLE_NOT_CLEARED, now);
      return;
    }
  }

  if (state == ST_STARTUP_CLEAR && startupClearStartMs != 0) {
    if ((now - startupClearStartMs) > T_STARTUP_CLEAR_TIMEOUT_MS && sensor) {
      raiseAlarm(ALM_STARTUP_CLEAR_TIMEOUT, now);
      return;
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

  if (alarmLatched) {
    enterState(ST_ALARM, now);
    return;
  }

  if (!machineRun) {
    resetMachineToStoppedBlocked(now);
    return;
  }

  switch (state) {
    case ST_WAIT_BOTTLE:
      outOutOpen = false;
      outFillRelay = false;
      sensorResetStartMs = 0;

      if (stopRequested) {
        machineRun = false;
        stopRequested = false;
        justStartedRun = false;
        resetMachineToStoppedBlocked(now);
        return;
      }

      outInOpen = true;
      outConveyorRun = false;

      if (justStartedRun) {
        justStartedRun = false;
        if (sensor) {
          outInOpen = false;
          startupClearStartMs = 0;
          bottleSeenStartMs = 0;
          bottleConfirmActive = false;
          enterState(ST_STARTUP_CLEAR, now);
          return;
        }
      }

      if (sensor) {
        if (!bottleConfirmActive) {
          bottleConfirmActive = true;
          bottleSeenStartMs = now;
        } else if ((now - bottleSeenStartMs) >= s_to_ms(P.tBottleConfirm)) {
          bottleConfirmActive = false;
          bottleSeenStartMs = 0;
          enterState(ST_DELAY_BEFORE_FILL, now);
        }
      } else {
        bottleConfirmActive = false;
        bottleSeenStartMs = 0;
      }
      break;

    case ST_STARTUP_CLEAR:
      outInOpen = false;
      outOutOpen = true;
      outFillRelay = false;
      outConveyorRun = false;
      sensorResetStartMs = 0;

      if (startupClearStartMs == 0) startupClearStartMs = now;

      if (!sensor) {
        startupClearStartMs = 0;
        outOpenStartMs = 0;
        bottleConfirmActive = false;
        bottleSeenStartMs = 0;
        enterState(ST_WAIT_BOTTLE, now);
        return;
      }
      break;

    case ST_DELAY_BEFORE_FILL:
      outInOpen = true;
      outOutOpen = false;
      outConveyorRun = true;

      if ((now - tStateMs) >= s_to_ms(P.tDelayFill)) {
        fillPulseStartMs = now;
        fillDoneSeenInactiveSinceCycle = !fillDone;
        fillDoneConfirmStartMs = 0;
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
      if (!fillDone) {
        fillDoneSeenInactiveSinceCycle = true;
        fillDoneConfirmStartMs = 0;
      } else if (fillDoneSeenInactiveSinceCycle) {
        if (fillDoneConfirmStartMs == 0) fillDoneConfirmStartMs = now;
      } else {
        fillDoneConfirmStartMs = 0;
      }

      {
        bool fillDoneConfirmed =
            fillDone &&
            fillDoneSeenInactiveSinceCycle &&
            fillDoneConfirmStartMs != 0 &&
            (now - fillDoneConfirmStartMs) >= T_FILL_DONE_CONFIRM_MS &&
            (now - tStateMs) >= T_FILL_DONE_MIN_MS;

        if (fillDoneConfirmed || (now - tStateMs) >= s_to_ms(P.tFill)) {
          bottlesTotal += 1;
          cyclesTotal += 1;
          count10s += 1;
          persistDirty = true;
          outOpenStartMs = 0;
          fillDoneConfirmStartMs = 0;
          enterState(ST_OUT_OPENING, now);
        }
      }
      break;

    case ST_OUT_OPENING:
      outOutOpen = true;
      outConveyorRun = false;

      if (outOpenStartMs == 0) {
        outOpenStartMs = now;
      }

      if ((now - outOpenStartMs) >= s_to_ms(P.tOutMin)) {
        if (!sensor) {
          if (sensorResetStartMs == 0) sensorResetStartMs = now;

          if ((now - sensorResetStartMs) >= T_SENSOR_RESET_CONFIRM_MS) {
            outOpenStartMs = 0;
            sensorResetStartMs = 0;
            enterState(ST_IN_REOPEN_DELAY, now);
          }
        } else {
          sensorResetStartMs = 0;
        }
      } else {
        sensorResetStartMs = 0;
      }
      break;

    case ST_IN_REOPEN_DELAY:
      outOutOpen = false;

      if (sensor) {
        sensorResetStartMs = 0;
        outOpenStartMs = 0;
        outOutOpen = true;
        enterState(ST_OUT_OPENING, now);
        return;
      }

      if (stopRequested) {
        if ((now - tStateMs) >= s_to_ms(P.tInDelay)) {
          machineRun = false;
          stopRequested = false;
          justStartedRun = false;
          resetMachineToStoppedBlocked(now);
          return;
        }
      } else {
        if ((now - tStateMs) >= s_to_ms(P.tInDelay)) {
          cycleStartMs = 0;
          outInOpen = true;
          bottleConfirmActive = false;
          bottleSeenStartMs = 0;
          enterState(ST_WAIT_BOTTLE, now);
        }
      }
      break;

    case ST_ALARM:
      outFillRelay = false;
      outOutOpen = false;
      outInOpen = false;
      outConveyorRun = false;
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
// PERSISTENCE SERVICE
// =====================================================
static void servicePersistence(uint32_t now) {
  if (persistDirty && (now - lastPersistMs) >= PERSIST_SAVE_INTERVAL_MS) {
    if (persist_save_all()) {
      persistDirty = false;
      lastPersistMs = now;
    }
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
  Serial.print(" RISE=");
  Serial.print(sensor1Rise ? "1" : "0");
  Serial.print(" FALL=");
  Serial.print(sensor1Fall ? "1" : "0");
  Serial.print(" BCONF=");
  Serial.print(bottleConfirmActive ? "1" : "0");
  Serial.print(" JSTART=");
  Serial.print(justStartedRun ? "1" : "0");
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
  Serial.print(" LOT=");
  Serial.print(lotNumber);
  Serial.print(" TOTAL=");
  Serial.print(bottlesTotal);
  Serial.print(" CYCLES=");
  Serial.print(cyclesTotal);
  Serial.print(" BPM=");
  Serial.print(bpm);
  Serial.print(" AVG=");
  Serial.print(avgBpm, 1);
  Serial.print(" PDIRTY=");
  Serial.println(persistDirty ? "1" : "0");
}

// =====================================================
// SETUP
// =====================================================
void setup() {
  Serial.begin(115200);
  delay(300);

  pinMode(PIN_SENSOR_1, INPUT);
  pinMode(PIN_FILL_DONE, INPUT);
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
    bottlesTotal = 0;
    cyclesTotal = 0;
    memset(lotNumber, 0, sizeof(lotNumber));
    persist_write();
  }

  sensor1Filter.setSec(P.tFilter);
  fillDoneFilter.setSec(0.01f);

  enterState(ST_WAIT_BOTTLE, millis());
  windowStartMs = millis();
  lastPersistMs = millis();

  applyOutputs();

  // Bring up Ethernet. Single-arg form matches the stock Arduino
  // WebServer example which is proven to work on this Portenta. The
  // four-arg form (ip, dns, gw, sn) appeared to break TCP listening
  // on PortentaEthernet while leaving UDP working.
  IPAddress ip = ipFrom4(netCfg.ip);
  Ethernet.begin(ip);
  Serial.print("[ETH] configured static IP=");
  Serial.println(ip);

  httpServer.begin();
  udp.begin(UDP_SERVER_PORT);

  ethFsm = EFSM_WAIT_LINK;
  ethFsmMs = millis();
  Serial.println("OPTA Version 1.2.3.8 started");
}

// =====================================================
// LOOP
// =====================================================
void loop() {
  uint32_t now = millis();

  // ---- Always-run: input sampling, alarms, machine logic, outputs. ----
  // These keep the state machine alive and the relay timing accurate.
  // Pushbutton is treated as critical so emergency stop / alarm reset
  // always work, even during a fill.
  updateInputs(now);
  updatePushbuttonLogic(now);
  evaluateAlarms(now);
  runMachine(now);
  updateBpm(now);
  applyOutputs();

  // ---- Gated: skip non-critical work while filling is active. ----
  // The hardware Timeout already guarantees the fill relay can't stay on
  // longer than FILL_RELAY_PULSE_LIMIT_MS, but skipping HTTP/UDP/flash/
  // Serial/Ethernet here keeps the loop tight so that the software
  // shutoff happens on time and the hardware backstop never has to fire.
  if (!isFillingActive()) {
    handleHttpOnce(now);
    serviceUdpTelemetry(now);
    servicePersistence(now);
    printStatusEvery1s(now);
    serviceEthernet(now);
  }
}
