require('dotenv').config();
const express = require('express');
const axios = require('axios');
const cors = require('cors');
const crypto = require('crypto');
// const cheerio = require('cheerio'); // Removed for Node compatibility
const TrafficDatabase = require('./database');

const app = express();
const PORT = process.env.PORT || 3002;

// Keep process alive while logging background async failures for Railway crash triage.
process.on('unhandledRejection', (reason) => {
  console.error('❌ Unhandled promise rejection:', reason);
});

// Enable CORS for external frontend
app.use(cors({
  origin: '*',
  methods: ['GET', 'POST'],
  allowedHeaders: ['Content-Type', 'Cache-Control', 'Accept']
}));

app.use(express.json());

// HERE API configuration
const HERE_API_KEY = process.env.HERE_API_KEY || 'YOUR_HERE_API_KEY_NEEDED';
const HERE_BASE_URL = 'https://data.traffic.hereapi.com/v7/flow';
const NORTH_VAN_BBOX = '-123.187,49.300,-123.020,49.400';
const DEBUG_ROUTE_CACHE_TTL_MS = 2 * 60 * 1000;
const TRAFFIC_TODAY_CACHE_TTL_MS = 60 * 1000;
const TRAFFIC_TODAY_CACHE_MAX_KEYS = (() => {
  const parsed = Number.parseInt(process.env.TRAFFIC_TODAY_CACHE_MAX_KEYS || '3', 10);
  if (!Number.isFinite(parsed)) return 3;
  return Math.max(1, Math.min(12, parsed));
})();
const TRAFFIC_DAY_WINDOW_CACHE_TTL_MS = 60 * 1000;
const TRAFFIC_DAY_WINDOW_CACHE_MAX_KEYS = (() => {
  const parsed = Number.parseInt(process.env.TRAFFIC_DAY_WINDOW_CACHE_MAX_KEYS || '6', 10);
  if (!Number.isFinite(parsed)) return 6;
  return Math.max(1, Math.min(24, parsed));
})();
const TRAFFIC_DAY_WINDOW_MAX_RADIUS = 2;
const TRAFFIC_HEAVY_READ_MAX_CONCURRENCY = (() => {
  const parsed = Number.parseInt(process.env.TRAFFIC_HEAVY_READ_MAX_CONCURRENCY || '2', 10);
  if (!Number.isFinite(parsed)) return 2;
  return Math.max(1, Math.min(8, parsed));
})();
const TRAFFIC_HEAVY_READ_WAIT_TIMEOUT_MS = (() => {
  const parsed = Number.parseInt(process.env.TRAFFIC_HEAVY_READ_WAIT_TIMEOUT_MS || '8000', 10);
  if (!Number.isFinite(parsed)) return 8000;
  return Math.max(1000, Math.min(30000, parsed));
})();
const TRAFFIC_HEAP_SOFT_LIMIT_MB = (() => {
  const parsed = Number.parseInt(process.env.TRAFFIC_HEAP_SOFT_LIMIT_MB || '320', 10);
  if (!Number.isFinite(parsed)) return 320;
  return Math.max(128, Math.min(1024, parsed));
})();
const TRAFFIC_HEAP_HARD_REJECT_MB = (() => {
  const parsed = Number.parseInt(process.env.TRAFFIC_HEAP_HARD_REJECT_MB || '400', 10);
  if (!Number.isFinite(parsed)) return 400;
  return Math.max(160, Math.min(1400, parsed));
})();
const TRAFFIC_TODAY_DEFAULT_SERVICE_DAYS = 2;
const TRAFFIC_TODAY_MAX_SERVICE_DAYS = 21;
const TRAFFIC_TODAY_ABSOLUTE_SAFE_MAX_SERVICE_DAYS = 7;
const TRAFFIC_TODAY_RUNTIME_SAFE_MAX_SERVICE_DAYS = (() => {
  const parsed = Number.parseInt(process.env.TRAFFIC_TODAY_RUNTIME_SAFE_MAX_SERVICE_DAYS || '5', 10);
  if (!Number.isFinite(parsed)) return 5;
  return Math.max(
    1,
    Math.min(
      TRAFFIC_TODAY_MAX_SERVICE_DAYS,
      TRAFFIC_TODAY_ABSOLUTE_SAFE_MAX_SERVICE_DAYS,
      parsed
    )
  );
})();
const MANUAL_TRACKED_SOURCE_IDS = new Set([
  'here-net-0dcfe4832adf37',
  'here-net-b9879cd7423d5d',
  'here-net-cbb01b6ccf8c63',
  'here-net-e47c5902d3946c',
  'here-net-217f7880e97341',
  'here-net-383887eeb1c4b2',
  'here-net-ad611c65317425',
  'here-net-067035bd6cc5d7',
  'here-net-3a171ea8d95b6f',
  'here-net-1f008d0747be84',
  'here-net-fdc3c855f1a803'
]);
const TRAFFIC_DB_STALE_MAX_AGE_MS = 12 * 60 * 1000;
const TRAFFIC_STALE_THRESHOLD_MINUTES = (() => {
  const parsed = Number.parseInt(process.env.TRAFFIC_STALE_THRESHOLD_MINUTES || '12', 10);
  if (!Number.isFinite(parsed)) return 12;
  return Math.max(2, Math.min(180, parsed));
})();
const TRAFFIC_STALE_THRESHOLD_MS = TRAFFIC_STALE_THRESHOLD_MINUTES * 60 * 1000;
const TRAFFIC_WATCHDOG_STALE_MINUTES = (() => {
  const parsed = Number.parseInt(process.env.TRAFFIC_WATCHDOG_STALE_MINUTES || '15', 10);
  if (!Number.isFinite(parsed)) return 15;
  return Math.max(3, Math.min(360, parsed));
})();
const TRAFFIC_WATCHDOG_STALE_MS = TRAFFIC_WATCHDOG_STALE_MINUTES * 60 * 1000;
const TRAFFIC_WATCHDOG_INTERVAL_MS = 60 * 1000;
const TRAFFIC_COLLECTION_TIME_ZONE = 'America/Vancouver';
const TRAFFIC_COLLECTION_PEAK_INTERVAL_MINUTES = (() => {
  const parsed = Number.parseInt(process.env.TRAFFIC_COLLECTION_PEAK_INTERVAL_MINUTES || '2', 10);
  if (!Number.isFinite(parsed)) return 2;
  return Math.max(1, Math.min(60, parsed));
})();
const TRAFFIC_COLLECTION_OFFPEAK_INTERVAL_MINUTES = (() => {
  const parsed = Number.parseInt(process.env.TRAFFIC_COLLECTION_OFFPEAK_INTERVAL_MINUTES || '10', 10);
  if (!Number.isFinite(parsed)) return 10;
  return Math.max(TRAFFIC_COLLECTION_PEAK_INTERVAL_MINUTES, Math.min(180, parsed));
})();
const TRAFFIC_COLLECTION_OFFPEAK_START_HOUR = (() => {
  const parsed = Number.parseInt(process.env.TRAFFIC_COLLECTION_OFFPEAK_START_HOUR || '2', 10);
  if (!Number.isFinite(parsed)) return 2;
  return Math.max(0, Math.min(23, parsed));
})();
const TRAFFIC_COLLECTION_OFFPEAK_END_HOUR = (() => {
  const parsed = Number.parseInt(process.env.TRAFFIC_COLLECTION_OFFPEAK_END_HOUR || '5', 10);
  if (!Number.isFinite(parsed)) return 5;
  return Math.max(0, Math.min(23, parsed));
})();
const TRAFFIC_COLLECTION_PEAK_INTERVAL_MS = TRAFFIC_COLLECTION_PEAK_INTERVAL_MINUTES * 60 * 1000;
const TRAFFIC_COLLECTION_OFFPEAK_INTERVAL_MS = TRAFFIC_COLLECTION_OFFPEAK_INTERVAL_MINUTES * 60 * 1000;
const trafficCollectionHourFormatter = new Intl.DateTimeFormat('en-US', {
  timeZone: TRAFFIC_COLLECTION_TIME_ZONE,
  hour: '2-digit',
  hour12: false,
  hourCycle: 'h23'
});
const trafficCollectionMinuteFormatter = new Intl.DateTimeFormat('en-US', {
  timeZone: TRAFFIC_COLLECTION_TIME_ZONE,
  hour: '2-digit',
  minute: '2-digit',
  hour12: false,
  hourCycle: 'h23'
});
const trafficCollectionDayFormatter = new Intl.DateTimeFormat('en-CA', {
  timeZone: TRAFFIC_COLLECTION_TIME_ZONE,
  year: 'numeric',
  month: '2-digit',
  day: '2-digit'
});
const MIN_FILTERED_SEGMENTS_FOR_TRACKED = 12;
const TRAFFIC_STATS_CACHE_TTL_MS = 2 * 60 * 1000;
const TRAFFIC_STATS_CACHE_MAX_KEYS = (() => {
  const parsed = Number.parseInt(process.env.TRAFFIC_STATS_CACHE_MAX_KEYS || '18', 10);
  if (!Number.isFinite(parsed)) return 18;
  return Math.max(4, Math.min(60, parsed));
})();
const TRAFFIC_STATS_MAX_LOOKBACK_WEEKS = 20;
const TRAFFIC_STATS_DEFAULT_LOOKBACK_WEEKS = 8;
const TRAFFIC_STATS_DEFAULT_MAX_SAME_WEEKDAY_SAMPLES = 8;
const TRAFFIC_STATS_SNAPSHOT_STEP_MINUTES = 2;
const TRAFFIC_STATS_WINDOW_MINUTES = 30;
const TRAFFIC_STATS_STATUS_THRESHOLDS = {
  freeFlow: 0.55,
  moderate: 0.42,
  heavy: 0.28
};
const TRAFFIC_STATS_COMMUTE_WINDOWS = {
  morning: {
    startMinutes: 6 * 60,
    endMinutes: 10 * 60
  },
  afternoon: {
    startMinutes: 15 * 60,
    endMinutes: 19 * 60
  }
};
const BRIDGE_CORRIDORS = {
  lionsGate: {
    minLng: -123.152,
    maxLng: -123.115,
    minLat: 49.296,
    maxLat: 49.323
  },
  ironworkers: {
    minLng: -123.055,
    maxLng: -123.0,
    minLat: 49.274,
    maxLat: 49.305
  }
};

// OPTIMIZED: Tier 1 + Tier 2 critical monitoring roads only
// Reduced from 21 roads (282 segments) to 15 roads (~45 segments) to solve database crisis
// Database growth: 4MB/hour → 0.6MB/hour (85% reduction)
const NORTH_VAN_ROADS = [
  // TIER 1 - CRITICAL ROADS (high priority = 4 segments each for focused monitoring)
  { name: 'Lions Gate Bridge', bbox: '49.314,-123.140,49.316,-123.136', type: 'bridge', priority: 'high' },
  { name: 'Ironworkers Memorial Bridge', bbox: '49.292,-123.027,49.306,-123.020', type: 'bridge', priority: 'high' },
  { name: 'Highway 1 - The Cut (Lynn Valley to Ironworkers)', bbox: '49.324,-123.060,49.330,-123.020', type: 'highway', priority: 'high' },
  { name: 'Taylor Way (Marine to Hwy 1)', bbox: '49.324,-123.140,49.328,-123.138', type: 'arterial', priority: 'high' },
  { name: 'Marine Drive (Taylor to Five-Way)', bbox: '49.324,-123.140,49.326,-123.070', type: 'arterial', priority: 'high' },
  { name: 'Main Street / Dollarton Hwy (Bridge Approaches)', bbox: '49.294,-123.026,49.296,-123.022', type: 'ramp', priority: 'high' },
  { name: 'Highway 1 at Capilano Interchange', bbox: '49.327,-123.112,49.330,-123.108', type: 'highway', priority: 'high' },
  { name: 'Cotton Road / Lynn Creek Bridge', bbox: '49.311,-123.046,49.313,-123.044', type: 'arterial', priority: 'high' },
  
  // TIER 2 - HIGH PRIORITY ROADS (medium priority = 3 segments each)
  { name: 'Keith Road Corridor (Ridgeway to Brooksbank)', bbox: '49.311,-123.066,49.313,-123.052', type: 'arterial', priority: 'medium' },
  { name: 'Mountain Highway / Hwy 1 Interchange', bbox: '49.329,-123.052,49.331,-123.048', type: 'arterial', priority: 'medium' },
  { name: 'Lynn Valley Road / Hwy 1 Interchange', bbox: '49.331,-123.037,49.333,-123.033', type: 'arterial', priority: 'medium' },
  { name: 'Lonsdale Avenue at Marine Drive', bbox: '49.319,-123.076,49.321,-123.072', type: 'arterial', priority: 'medium' },
  { name: 'Phibbs Exchange Area', bbox: '49.294,-123.026,49.296,-123.022', type: 'transit', priority: 'medium' },
  { name: 'Highway 1 at Taylor Way Interchange', bbox: '49.327,-123.140,49.329,-123.138', type: 'ramp', priority: 'medium' },
  { name: 'Fern Street Overpass / Mt Seymour Connection', bbox: '49.324,-123.040,49.326,-123.036', type: 'arterial', priority: 'medium' },
  
  // ADDITIONAL ROADS - Fill coverage gaps identified by visual inspection
  { name: 'Keith Road East', bbox: '49.311,-123.050,49.313,-123.020', type: 'arterial', priority: 'medium' },
  { name: 'Low Level Road Extended', bbox: '49.308,-123.090,49.315,-123.015', type: 'arterial', priority: 'medium' },
  { name: '3rd Street Central (Moody to Gladstone)', bbox: '49.315,-123.085,49.318,-123.065', type: 'arterial', priority: 'medium' },
  { name: '3rd Street East Extended', bbox: '49.314,-123.065,49.318,-123.018', type: 'arterial', priority: 'medium' },
  { name: 'Mountain Highway South', bbox: '49.320,-123.052,49.330,-123.048', type: 'arterial', priority: 'medium' },
  { name: 'Brooksbank Avenue South', bbox: '49.312,-123.055,49.320,-123.050', type: 'arterial', priority: 'medium' },
  { name: 'Arborlynn Drive', bbox: '49.325,-123.040,49.330,-123.035', type: 'collector', priority: 'low' },
  { name: 'Highway 1 West (21st to Capilano)', bbox: '49.325,-123.120,49.330,-123.110', type: 'highway', priority: 'high' },
  { name: 'Highway 1 Central (Capilano to Lonsdale)', bbox: '49.325,-123.110,49.330,-123.070', type: 'highway', priority: 'high' },
  { name: 'Highway 1 East (Lonsdale to Ironworkers)', bbox: '49.290,-123.070,49.295,-123.025', type: 'highway', priority: 'high' },
  { name: 'Taylor Way Upper (Hwy 1 to British Properties)', bbox: '49.328,-123.142,49.350,-123.135', type: 'arterial', priority: 'medium' },
  { name: 'Grand Boulevard', bbox: '49.315,-123.076,49.340,-123.072', type: 'arterial', priority: 'medium' },
  { name: 'Main Street Onramp to Highway 1 South', bbox: '49.322,-123.100,49.325,-123.096', type: 'onramp', priority: 'medium' }
];

// Data storage
let trafficIntervals = [];
let segmentData = {};
let isCollecting = false;
let trafficCollectionTimer = null;
let trafficCollectionInFlight = false;
let lastTrafficCollectionCadenceLabel = null;

// Database integration
let db;
if (process.env.DATABASE_URL) {
  db = new TrafficDatabase();
  console.log('✅ Database integration enabled');
} else {
  console.log('⚠️  No DATABASE_URL - using memory storage only');
}

// Counter-flow data storage
let counterFlowData = {
  currentStatus: null, // 'outbound-1', 'outbound-2', etc.
  statusSince: null, // timestamp when current status started
  lastChecked: null, // last scrape timestamp
  history: [], // array of status changes for pattern analysis
  lastError: null,
  sourceUrl: null
};

// Debug route overlay cache (all routes + tracked overlay)
let debugRouteSnapshot = {
  fetchedAt: null,
  dataSource: 'uninitialized',
  allSegments: {},
  trackedSourceIds: [],
  rawSegmentCount: 0,
  filteredSegmentCount: 0,
  selectedSegmentCount: 0,
  filterMode: 'none',
  manualTrackedConfiguredCount: MANUAL_TRACKED_SOURCE_IDS.size,
  manualTrackedMatchedCount: 0
};

let trafficTodayCache = new Map();
const trafficTodayInFlight = new Map();
let trafficDayWindowCache = new Map();
const trafficDayWindowInFlight = new Map();
let trafficStatsCache = new Map();
const trafficStatsInFlight = new Map();
let activeTrafficHeavyReads = 0;
const trafficHeavyReadQueue = [];
let lastSuccessfulTrafficCollectionAtMs = null;
let lastTrafficCollectionFailureAtMs = null;
let consecutiveTrafficCollectionFailures = 0;
let lastTrafficCollectionFailureMessage = null;
let lastTrafficWatchdogAlertAtMs = 0;

const getTrafficTodayCacheKey = (serviceDays) => `serviceDays:${serviceDays}`;
const getTrafficDayWindowCacheKey = (centerDay, radiusDays) => `day:${centerDay}|radius:${radiusDays}`;
const getTrafficStatsCacheKey = (dayKey, lookbackWeeks, maxSamples) =>
  `stats:${dayKey}|lookback:${lookbackWeeks}|samples:${maxSamples}`;

const getTrafficTodayCachePayload = (cacheKey) => {
  const cacheEntry = trafficTodayCache.get(cacheKey);
  if (!cacheEntry) return null;
  if (Date.now() >= cacheEntry.expiresAt) {
    trafficTodayCache.delete(cacheKey);
    return null;
  }
  return cacheEntry.payload;
};

const setTrafficTodayCachePayload = (cacheKey, payload) => {
  if (trafficTodayCache.has(cacheKey)) {
    trafficTodayCache.delete(cacheKey);
  }
  trafficTodayCache.set(cacheKey, {
    expiresAt: Date.now() + TRAFFIC_TODAY_CACHE_TTL_MS,
    payload
  });

  // Keep a bounded cache so serviceDays variants do not evict each other instantly.
  while (trafficTodayCache.size > TRAFFIC_TODAY_CACHE_MAX_KEYS) {
    const oldestKey = trafficTodayCache.keys().next().value;
    if (!oldestKey) break;
    trafficTodayCache.delete(oldestKey);
  }
};

const invalidateTrafficTodayCache = () => {
  trafficTodayCache.clear();
  trafficDayWindowCache.clear();
  trafficStatsCache.clear();
};

const getHeapUsedMb = () => {
  const heapUsed = process.memoryUsage?.().heapUsed;
  if (!Number.isFinite(heapUsed)) return 0;
  return Math.round(heapUsed / (1024 * 1024));
};

const shouldHardRejectForMemory = () => getHeapUsedMb() >= TRAFFIC_HEAP_HARD_REJECT_MB;
const isSoftMemoryPressure = () => getHeapUsedMb() >= TRAFFIC_HEAP_SOFT_LIMIT_MB;

const promoteTrafficHeavyReadQueue = () => {
  while (activeTrafficHeavyReads < TRAFFIC_HEAVY_READ_MAX_CONCURRENCY && trafficHeavyReadQueue.length > 0) {
    const entry = trafficHeavyReadQueue.shift();
    if (!entry) break;
    if (entry.timeout) {
      clearTimeout(entry.timeout);
    }
    activeTrafficHeavyReads += 1;
    const waitedMs = Date.now() - entry.enqueuedAt;
    entry.resolve({
      release: (() => {
        let released = false;
        return () => {
          if (released) return;
          released = true;
          activeTrafficHeavyReads = Math.max(0, activeTrafficHeavyReads - 1);
          promoteTrafficHeavyReadQueue();
        };
      })(),
      waitedMs
    });
  }
};

const acquireTrafficHeavyReadSlot = async (label) => {
  if (activeTrafficHeavyReads < TRAFFIC_HEAVY_READ_MAX_CONCURRENCY) {
    activeTrafficHeavyReads += 1;
    return {
      release: (() => {
        let released = false;
        return () => {
          if (released) return;
          released = true;
          activeTrafficHeavyReads = Math.max(0, activeTrafficHeavyReads - 1);
          promoteTrafficHeavyReadQueue();
        };
      })(),
      waitedMs: 0
    };
  }

  return await new Promise((resolve, reject) => {
    const queueEntry = {
      label,
      enqueuedAt: Date.now(),
      resolve,
      reject,
      timeout: null
    };

    queueEntry.timeout = setTimeout(() => {
      const queueIndex = trafficHeavyReadQueue.indexOf(queueEntry);
      if (queueIndex >= 0) {
        trafficHeavyReadQueue.splice(queueIndex, 1);
      }
      const error = new Error(
        `Heavy read queue timeout after ${TRAFFIC_HEAVY_READ_WAIT_TIMEOUT_MS}ms for ${label}`
      );
      error.code = 'HEAVY_READ_QUEUE_TIMEOUT';
      reject(error);
    }, TRAFFIC_HEAVY_READ_WAIT_TIMEOUT_MS);

    trafficHeavyReadQueue.push(queueEntry);
    console.warn(
      `⏳ Heavy read queued (${label}): active=${activeTrafficHeavyReads}, ` +
      `queue=${trafficHeavyReadQueue.length}, max=${TRAFFIC_HEAVY_READ_MAX_CONCURRENCY}`
    );
  });
};

const runTrafficHeavyRead = async (label, fn) => {
  if (isSoftMemoryPressure()) {
    console.warn(`⚠️ Memory pressure (soft ${getHeapUsedMb()}MB). Clearing traffic response caches before ${label}`);
    invalidateTrafficTodayCache();
  }
  if (shouldHardRejectForMemory()) {
    const error = new Error(`Memory pressure hard reject before ${label}: heap=${getHeapUsedMb()}MB`);
    error.code = 'MEMORY_PRESSURE_HARD_REJECT';
    throw error;
  }

  const slot = await acquireTrafficHeavyReadSlot(label);
  if (slot.waitedMs > 0) {
    console.log(`⏱️ Heavy read slot acquired for ${label} after waiting ${slot.waitedMs}ms`);
  }

  try {
    return await fn();
  } finally {
    slot.release();
    if (isSoftMemoryPressure()) {
      console.warn(`⚠️ Memory pressure (soft ${getHeapUsedMb()}MB) after ${label}. Clearing traffic response caches.`);
      invalidateTrafficTodayCache();
    }
  }
};

const getTrafficDayWindowCachePayload = (cacheKey) => {
  const cacheEntry = trafficDayWindowCache.get(cacheKey);
  if (!cacheEntry) return null;
  if (Date.now() >= cacheEntry.expiresAt) {
    trafficDayWindowCache.delete(cacheKey);
    return null;
  }
  return cacheEntry.payload;
};

const setTrafficDayWindowCachePayload = (cacheKey, payload) => {
  if (trafficDayWindowCache.has(cacheKey)) {
    trafficDayWindowCache.delete(cacheKey);
  }
  trafficDayWindowCache.set(cacheKey, {
    expiresAt: Date.now() + TRAFFIC_DAY_WINDOW_CACHE_TTL_MS,
    payload
  });

  while (trafficDayWindowCache.size > TRAFFIC_DAY_WINDOW_CACHE_MAX_KEYS) {
    const oldestKey = trafficDayWindowCache.keys().next().value;
    if (!oldestKey) break;
    trafficDayWindowCache.delete(oldestKey);
  }
};

const getTrafficStatsCachePayload = (cacheKey) => {
  const cacheEntry = trafficStatsCache.get(cacheKey);
  if (!cacheEntry) return null;
  if (Date.now() >= cacheEntry.expiresAt) {
    trafficStatsCache.delete(cacheKey);
    return null;
  }
  return cacheEntry.payload;
};

const setTrafficStatsCachePayload = (cacheKey, payload) => {
  if (trafficStatsCache.has(cacheKey)) {
    trafficStatsCache.delete(cacheKey);
  }
  trafficStatsCache.set(cacheKey, {
    expiresAt: Date.now() + TRAFFIC_STATS_CACHE_TTL_MS,
    payload
  });

  while (trafficStatsCache.size > TRAFFIC_STATS_CACHE_MAX_KEYS) {
    const oldestKey = trafficStatsCache.keys().next().value;
    if (!oldestKey) break;
    trafficStatsCache.delete(oldestKey);
  }
};

const getTrafficCollectionLocalHour = (date = new Date()) => {
  const hourText = trafficCollectionHourFormatter.format(date);
  const parsedHour = Number.parseInt(hourText, 10);
  if (Number.isFinite(parsedHour)) return parsedHour;
  return date.getUTCHours();
};

const isHourInCollectionWindow = (hour, startHour, endHour) => {
  if (startHour === endHour) return false;
  if (startHour < endHour) {
    return hour >= startHour && hour < endHour;
  }
  return hour >= startHour || hour < endHour;
};

const getTrafficCollectionCadence = (date = new Date()) => {
  const localHour = getTrafficCollectionLocalHour(date);
  const inOffPeakWindow = isHourInCollectionWindow(
    localHour,
    TRAFFIC_COLLECTION_OFFPEAK_START_HOUR,
    TRAFFIC_COLLECTION_OFFPEAK_END_HOUR
  );
  const intervalMs = inOffPeakWindow
    ? TRAFFIC_COLLECTION_OFFPEAK_INTERVAL_MS
    : TRAFFIC_COLLECTION_PEAK_INTERVAL_MS;
  const intervalMinutes = Math.round(intervalMs / (60 * 1000));
  const label = `${inOffPeakWindow ? 'off-peak' : 'peak'}:${intervalMinutes}m`;
  return {
    intervalMs,
    intervalMinutes,
    inOffPeakWindow,
    localHour,
    label
  };
};

const scheduleNextTrafficCollection = (delayMs = null) => {
  if (!isCollecting) return;
  if (trafficCollectionTimer) {
    clearTimeout(trafficCollectionTimer);
  }
  const cadence = getTrafficCollectionCadence(new Date());
  const effectiveDelayMs = Number.isFinite(delayMs) && delayMs >= 0
    ? delayMs
    : cadence.intervalMs;
  trafficCollectionTimer = setTimeout(() => {
    runTrafficCollectionCycle().catch((error) => {
      console.error('❌ Traffic collection cycle crashed:', error);
      if (isCollecting) {
        scheduleNextTrafficCollection(15 * 1000);
      }
    });
  }, effectiveDelayMs);
};

const runTrafficCollectionCycle = async () => {
  if (!isCollecting) return;
  if (trafficCollectionInFlight) {
    console.warn('⚠️ Traffic collection overlap detected; delaying next cycle by 15s');
    scheduleNextTrafficCollection(15 * 1000);
    return;
  }

  const cycleStartedAt = Date.now();
  let startCadence = null;
  try {
    startCadence = getTrafficCollectionCadence(new Date(cycleStartedAt));
  } catch (error) {
    console.error('❌ Failed to resolve traffic collection cadence:', error);
  }

  if (startCadence && startCadence.label !== lastTrafficCollectionCadenceLabel) {
    console.log(
      `⏱️ Traffic collection cadence: ${startCadence.label} ` +
      `(hour ${startCadence.localHour}, tz=${TRAFFIC_COLLECTION_TIME_ZONE}, ` +
      `off-peak window ${TRAFFIC_COLLECTION_OFFPEAK_START_HOUR}:00-${TRAFFIC_COLLECTION_OFFPEAK_END_HOUR}:00)`
    );
    lastTrafficCollectionCadenceLabel = startCadence.label;
  }

  trafficCollectionInFlight = true;
  try {
    await collectTrafficData();
  } catch (error) {
    console.error('❌ Traffic collection cycle failed:', error);
  } finally {
    trafficCollectionInFlight = false;
    if (isCollecting) {
      try {
        const nextCadence = getTrafficCollectionCadence(new Date());
        const elapsedMs = Date.now() - cycleStartedAt;
        const nextDelayMs = Math.max(1000, nextCadence.intervalMs - elapsedMs);
        scheduleNextTrafficCollection(nextDelayMs);
      } catch (error) {
        console.error('❌ Failed to schedule next traffic collection cycle:', error);
        scheduleNextTrafficCollection(15 * 1000);
      }
    }
  }
};

const toFiniteNumber = (value) => {
  const numeric = Number(value);
  return Number.isFinite(numeric) ? numeric : null;
};

const addUniqueCoordinate = (target, point) => {
  if (!Array.isArray(target) || !Array.isArray(point) || point.length < 2) return;
  const [lng, lat] = point;
  if (!Number.isFinite(lng) || !Number.isFinite(lat)) return;

  const previous = target[target.length - 1];
  if (previous && Math.abs(previous[0] - lng) < 1e-7 && Math.abs(previous[1] - lat) < 1e-7) {
    return;
  }
  target.push([lng, lat]);
};

const extractCoordinatesFromHereSegment = (segment) => {
  const coordinates = [];
  const links = segment?.location?.shape?.links;
  if (!Array.isArray(links)) return coordinates;

  links.forEach((link) => {
    const points = Array.isArray(link?.points) ? link.points : [];
    points.forEach((point) => {
      const lat = toFiniteNumber(point?.lat);
      const lng = toFiniteNumber(point?.lng);
      if (lat === null || lng === null) return;
      addUniqueCoordinate(coordinates, [lng, lat]);
    });
  });

  return coordinates;
};

const extractHereReference = (segment) => {
  const refs = [];
  const location = segment?.location || {};
  const pushRef = (value) => {
    if (value === undefined || value === null) return;
    const text = String(value).trim();
    if (!text) return;
    refs.push(text);
  };

  pushRef(location.id);
  pushRef(location.locationId);
  pushRef(location.locationRef);

  const links = location?.shape?.links;
  if (Array.isArray(links)) {
    links.forEach((link) => {
      pushRef(link?.id);
      pushRef(link?.linkId);
      pushRef(link?.locationRef);
      pushRef(link?.ref);
    });
  }

  const uniqueRefs = [...new Set(refs)];
  if (uniqueRefs.length === 0) return null;
  return uniqueRefs.join('|');
};

const extractNumericTokens = (...values) => {
  const tokens = new Set();
  values.forEach((value) => {
    if (value === undefined || value === null) return;
    const text = String(value);
    const matches = text.match(/\d+/g);
    if (!matches) return;
    matches.forEach((match) => tokens.add(match));
  });
  return [...tokens];
};

const buildStableHereSegmentId = (segment, coordinates) => {
  const description = String(segment?.location?.description || '').trim().toLowerCase();
  const hereReference = String(extractHereReference(segment) || '').trim().toLowerCase();
  const roundedCoordinates = coordinates
    .map(([lng, lat]) => `${lng.toFixed(5)},${lat.toFixed(5)}`)
    .join(';');
  const hashInput = `${description}|${hereReference}|${roundedCoordinates}|${coordinates.length}`;
  const digest = crypto.createHash('sha1').update(hashInput).digest('hex').slice(0, 14);
  return `here-net-${digest}`;
};

const inferSegmentType = (description = '') => {
  const text = String(description || '').trim().toLowerCase();
  if (!text) return 'road';

  if (
    text.includes('lions gate') ||
    text.includes('lions-gate') ||
    text.includes('lionsgate') ||
    text.includes('ironworkers') ||
    text.includes('iron workers') ||
    text.includes('second narrows') ||
    text.includes('memorial bridge') ||
    text.includes('bridge')
  ) {
    return 'bridge';
  }

  if (
    text.includes('hwy1') ||
    text.includes('hwy 1') ||
    text.includes('highway 1') ||
    text.includes('trans-canada') ||
    text.includes('upper levels')
  ) {
    return 'highway';
  }

  if (
    text.includes('onramp') ||
    text.includes('offramp') ||
    text.includes('off-ramp') ||
    text.includes('on-ramp') ||
    text.includes('interchange') ||
    text.includes('ramp')
  ) {
    return 'ramp';
  }

  if (
    text.includes('avenue') ||
    text.includes(' ave ') ||
    text.endsWith(' ave') ||
    text.includes(' street') ||
    text.endsWith(' st') ||
    text.includes(' road') ||
    text.includes(' drive') ||
    text.includes(' boulevard') ||
    text.includes(' blvd') ||
    text.includes(' corridor')
  ) {
    return 'arterial';
  }

  return 'road';
};

const touchesBridgeCorridor = (coordinates, corridor) => {
  if (!Array.isArray(coordinates) || !corridor) return false;
  return coordinates.some((coord) => {
    if (!Array.isArray(coord) || coord.length < 2) return false;
    const [lng, lat] = coord;
    return Number.isFinite(lng) &&
      Number.isFinite(lat) &&
      lng >= corridor.minLng &&
      lng <= corridor.maxLng &&
      lat >= corridor.minLat &&
      lat <= corridor.maxLat;
  });
};

const inferBridgeHint = (description = '', coordinates = []) => {
  const text = String(description || '').trim().toLowerCase();
  if (
    text.includes('lions gate') ||
    text.includes('lions-gate') ||
    text.includes('lionsgate')
  ) {
    return 'lions-gate';
  }
  if (
    text.includes('ironworkers') ||
    text.includes('iron workers') ||
    text.includes('second narrows') ||
    text.includes('memorial bridge')
  ) {
    return 'ironworkers';
  }

  if (touchesBridgeCorridor(coordinates, BRIDGE_CORRIDORS.lionsGate)) {
    return 'lions-gate';
  }
  if (touchesBridgeCorridor(coordinates, BRIDGE_CORRIDORS.ironworkers)) {
    return 'ironworkers';
  }

  return null;
};

const buildHereSegmentRecord = (segment, index) => {
  const coordinates = extractCoordinatesFromHereSegment(segment);
  if (coordinates.length < 2) return null;

  const currentFlow = segment?.currentFlow || {};
  const freeFlow = segment?.freeFlow || {};
  const currentSpeed = toFiniteNumber(currentFlow.speed);
  const freeFlowSpeed = toFiniteNumber(currentFlow.freeFlow ?? freeFlow.speed ?? currentSpeed);
  const jamFactor = toFiniteNumber(currentFlow.jamFactor);
  const confidence = toFiniteNumber(currentFlow.confidence);
  const description = String(segment?.location?.description || `Traffic Segment ${index + 1}`).trim();
  const hereReference = extractHereReference(segment);
  const sourceId = buildStableHereSegmentId(segment, coordinates);
  const numericIds = extractNumericTokens(sourceId, hereReference, description);
  const bridgeHint = inferBridgeHint(description, coordinates);
  const type = inferSegmentType(description);

  return {
    sourceId,
    name: description || `Traffic Segment ${index + 1}`,
    description: description || null,
    coordinates,
    type,
    bridgeHint,
    hereReference,
    numericIds,
    currentSpeed,
    freeFlowSpeed,
    jamFactor,
    confidence
  };
};

const buildFallbackAllSegmentsFromTracked = (segments = {}) => {
  const fallback = {};
  Object.entries(segments).forEach(([segmentId, segment]) => {
    const coordinates = Array.isArray(segment?.coordinates) ? segment.coordinates : [];
    if (coordinates.length < 2) return;

    const sourceId = typeof segment?.sourceId === 'string' && segment.sourceId
      ? segment.sourceId
      : `tracked-${segmentId}`;
    fallback[sourceId] = {
      id: sourceId,
      name: segment?.name || segmentId,
      description: segment?.description || segment?.name || segmentId,
      coordinates,
      type: segment?.type || 'road',
      bridgeHint: segment?.bridgeHint || null,
      hereReference: segment?.hereReference || null,
      numericIds: extractNumericTokens(sourceId, segment?.name, segment?.hereReference),
      currentSpeed: null,
      freeFlowSpeed: null,
      jamFactor: null,
      confidence: null
    };
  });
  return fallback;
};

const updateDebugRouteSnapshotFromFetch = (fetchResult, fetchedAt = new Date().toISOString()) => {
  const allSegments = fetchResult?.allSegmentMetadata || {};
  if (Object.keys(allSegments).length === 0) return;

  const trackedSourceIds = fetchResult?.debugMeta?.trackedSourceIds ||
    Object.values(fetchResult?.segmentMetadata || {})
      .map((segment) => segment?.sourceId)
      .filter((value) => typeof value === 'string' && value.length > 0);

  debugRouteSnapshot = {
    fetchedAt,
    dataSource: fetchResult?.debugMeta?.dataSource || 'here-live',
    allSegments,
    trackedSourceIds: [...new Set(trackedSourceIds)],
    rawSegmentCount: fetchResult?.debugMeta?.rawSegmentCount || Object.keys(allSegments).length,
    filteredSegmentCount: fetchResult?.debugMeta?.filteredSegmentCount || 0,
    selectedSegmentCount: fetchResult?.debugMeta?.selectedSegmentCount || 0,
    filterMode: fetchResult?.debugMeta?.filterMode || 'none',
    manualTrackedConfiguredCount: fetchResult?.debugMeta?.manualTrackedConfiguredCount || MANUAL_TRACKED_SOURCE_IDS.size,
    manualTrackedMatchedCount: fetchResult?.debugMeta?.manualTrackedMatchedCount || 0
  };
};

const parseIntervalTimestampMs = (interval) => {
  if (!interval || typeof interval !== 'object') return null;
  const rawTimestamp = interval.timestamp;
  if (rawTimestamp === undefined || rawTimestamp === null) return null;

  if (rawTimestamp instanceof Date) {
    const ms = rawTimestamp.getTime();
    return Number.isFinite(ms) ? ms : null;
  }

  if (typeof rawTimestamp === 'number' && Number.isFinite(rawTimestamp)) {
    if (rawTimestamp > 1e14) return Math.floor(rawTimestamp / 1000);
    if (rawTimestamp < 1e12) return Math.floor(rawTimestamp * 1000);
    return Math.floor(rawTimestamp);
  }

  const parsedMs = Date.parse(String(rawTimestamp));
  return Number.isFinite(parsedMs) ? parsedMs : null;
};

const parseTrafficDayKey = (dayKey) => {
  if (typeof dayKey !== 'string') return null;
  const match = dayKey.trim().match(/^(\d{4})-(\d{2})-(\d{2})$/);
  if (!match) return null;
  const year = Number(match[1]);
  const month = Number(match[2]);
  const day = Number(match[3]);
  const candidate = new Date(Date.UTC(year, month - 1, day, 12, 0, 0));
  if (
    candidate.getUTCFullYear() !== year ||
    candidate.getUTCMonth() + 1 !== month ||
    candidate.getUTCDate() !== day
  ) {
    return null;
  }
  return { year, month, day };
};

const formatTrafficDayKey = (year, month, day) =>
  `${String(year).padStart(4, '0')}-${String(month).padStart(2, '0')}-${String(day).padStart(2, '0')}`;

const getTrafficServiceDayKey = (date = new Date()) => {
  const parts = trafficCollectionDayFormatter.formatToParts(date);
  const values = {};
  parts.forEach((part) => {
    if (part.type !== 'literal') values[part.type] = part.value;
  });
  const year = Number(values.year);
  const month = Number(values.month);
  const day = Number(values.day);
  if (!Number.isFinite(year) || !Number.isFinite(month) || !Number.isFinite(day)) {
    const fallback = new Date(date);
    return formatTrafficDayKey(
      fallback.getUTCFullYear(),
      fallback.getUTCMonth() + 1,
      fallback.getUTCDate()
    );
  }
  return formatTrafficDayKey(year, month, day);
};

const getTrafficServiceDayKeyFromTimestampMs = (timestampMs) => {
  if (!Number.isFinite(timestampMs)) return null;
  return getTrafficServiceDayKey(new Date(timestampMs));
};

const getDayOfWeekFromTrafficDayKey = (dayKey) => {
  const parsed = parseTrafficDayKey(dayKey);
  if (!parsed) return null;
  return new Date(Date.UTC(parsed.year, parsed.month - 1, parsed.day, 12, 0, 0)).getUTCDay();
};

const getWeekdayLabelFromTrafficDayKey = (dayKey) => {
  const parsed = parseTrafficDayKey(dayKey);
  if (!parsed) return null;
  return new Date(Date.UTC(parsed.year, parsed.month - 1, parsed.day, 12, 0, 0))
    .toLocaleDateString('en-US', { weekday: 'long', timeZone: 'UTC' });
};

const clampTrafficRatio = (value) => {
  const ratio = Number(value);
  if (!Number.isFinite(ratio)) return null;
  return Math.max(0, Math.min(1, ratio));
};

const getTrafficStatusLevelFromRatio = (ratio) => {
  const normalized = clampTrafficRatio(ratio);
  if (normalized === null) return 'unknown';
  if (normalized > TRAFFIC_STATS_STATUS_THRESHOLDS.freeFlow) return 'free';
  if (normalized > TRAFFIC_STATS_STATUS_THRESHOLDS.moderate) return 'moderate';
  if (normalized > TRAFFIC_STATS_STATUS_THRESHOLDS.heavy) return 'heavy';
  return 'gridlock';
};

const getLocalMinutesOfDayInCollectionZone = (timestampMs) => {
  if (!Number.isFinite(timestampMs)) return null;
  const parts = trafficCollectionMinuteFormatter.formatToParts(new Date(timestampMs));
  const values = {};
  parts.forEach((part) => {
    if (part.type !== 'literal') values[part.type] = part.value;
  });
  const hour = Number(values.hour);
  const minute = Number(values.minute);
  if (!Number.isFinite(hour) || !Number.isFinite(minute)) return null;
  return (hour * 60) + minute;
};

const isRushHourMinutes = (minutesOfDay) => {
  if (!Number.isFinite(minutesOfDay)) return false;
  const isMorningRush = minutesOfDay >= (6 * 60) && minutesOfDay < (10 * 60);
  const isEveningRush = minutesOfDay >= (15 * 60) && minutesOfDay < (19 * 60);
  return isMorningRush || isEveningRush;
};

const getTrafficAverageRatioForSegments = (interval, segmentIds = []) => {
  if (!interval || typeof interval !== 'object') return null;
  if (!Array.isArray(segmentIds) || segmentIds.length === 0) return null;

  let sum = 0;
  let count = 0;
  segmentIds.forEach((segmentId) => {
    const ratio = clampTrafficRatio(interval[segmentId]);
    if (ratio === null) return;
    sum += ratio;
    count += 1;
  });

  return count > 0 ? (sum / count) : null;
};

const getCoordinatesForBridgeHint = (rawCoordinates) => {
  if (Array.isArray(rawCoordinates)) return rawCoordinates;
  if (typeof rawCoordinates === 'string') {
    try {
      const parsed = JSON.parse(rawCoordinates);
      return Array.isArray(parsed) ? parsed : [];
    } catch (_error) {
      return [];
    }
  }
  return [];
};

const resolveBridgeTypeFromSegment = (segmentId, segment) => {
  const hint = String(segment?.bridgeHint || '').trim().toLowerCase();
  if (hint === 'lions-gate' || hint === 'lions gate' || hint === 'lionsgate') return 'lions-gate';
  if (
    hint === 'ironworkers' ||
    hint === 'iron workers' ||
    hint === 'second-narrows' ||
    hint === 'second narrows'
  ) {
    return 'ironworkers';
  }

  const descriptionParts = [
    segmentId,
    segment?.name,
    segment?.description,
    segment?.hereReference
  ].filter((value) => typeof value === 'string' && value.trim().length > 0);
  const description = descriptionParts.join(' ').trim();
  const coordinates = getCoordinatesForBridgeHint(segment?.coordinates);
  return inferBridgeHint(description, coordinates);
};

const buildTrafficIntervalSeries = (intervals = [], segments = {}) => {
  const resolvedIntervals = (Array.isArray(intervals) ? intervals : [])
    .map((interval) => ({
      interval,
      timestampMs: parseIntervalTimestampMs(interval)
    }))
    .filter((entry) => Number.isFinite(entry.timestampMs))
    .sort((a, b) => a.timestampMs - b.timestampMs);

  const allSegmentIdsSet = new Set();
  resolvedIntervals.forEach(({ interval }) => {
    Object.keys(interval || {}).forEach((key) => {
      if (key && key !== 'timestamp') allSegmentIdsSet.add(key);
    });
  });

  const allSegmentIds = [...allSegmentIdsSet];
  const lionsGateSegmentIds = [];
  const ironworkersSegmentIds = [];

  allSegmentIds.forEach((segmentId) => {
    const bridgeType = resolveBridgeTypeFromSegment(segmentId, segments?.[segmentId]);
    if (bridgeType === 'lions-gate') lionsGateSegmentIds.push(segmentId);
    if (bridgeType === 'ironworkers') ironworkersSegmentIds.push(segmentId);
  });

  const series = resolvedIntervals.map(({ interval, timestampMs }) => ({
    timestampMs,
    overallRatio: getTrafficAverageRatioForSegments(interval, allSegmentIds),
    lionsGateRatio: getTrafficAverageRatioForSegments(interval, lionsGateSegmentIds),
    ironworkersRatio: getTrafficAverageRatioForSegments(interval, ironworkersSegmentIds)
  }));

  return {
    series,
    segmentCounts: {
      overall: allSegmentIds.length,
      lionsGate: lionsGateSegmentIds.length,
      ironworkers: ironworkersSegmentIds.length
    }
  };
};

const calculateAverageFromSeries = (series = [], ratioKey = 'overallRatio') => {
  const values = [];
  (Array.isArray(series) ? series : []).forEach((entry) => {
    const ratio = clampTrafficRatio(entry?.[ratioKey]);
    if (ratio !== null) values.push(ratio);
  });
  if (values.length === 0) return null;
  return values.reduce((sum, value) => sum + value, 0) / values.length;
};

const isMinutesInTrafficWindow = (minutesOfDay, timeWindow) => {
  if (!timeWindow || typeof timeWindow !== 'object') return true;
  const startMinutes = Number(timeWindow.startMinutes);
  const endMinutes = Number(timeWindow.endMinutes);
  if (!Number.isFinite(startMinutes) || !Number.isFinite(endMinutes)) return true;
  if (!Number.isFinite(minutesOfDay)) return false;
  if (startMinutes === endMinutes) return true;
  if (startMinutes < endMinutes) {
    return minutesOfDay >= startMinutes && minutesOfDay < endMinutes;
  }
  // Support overnight windows if needed.
  return minutesOfDay >= startMinutes || minutesOfDay < endMinutes;
};

const calculateBestEscapeWindow = (series = [], ratioKey = 'overallRatio', timeWindow = null) => {
  const samples = (Array.isArray(series) ? series : [])
    .filter((entry) => {
      const ratio = clampTrafficRatio(entry?.[ratioKey]);
      if (ratio === null) return false;
      if (!timeWindow) return true;
      const minutesOfDay = getLocalMinutesOfDayInCollectionZone(entry?.timestampMs);
      return isMinutesInTrafficWindow(minutesOfDay, timeWindow);
    });
  if (samples.length === 0) return null;

  const desiredWindowSamples = Math.max(
    1,
    Math.round(TRAFFIC_STATS_WINDOW_MINUTES / TRAFFIC_STATS_SNAPSHOT_STEP_MINUTES)
  );
  const windowSamples = Math.min(desiredWindowSamples, samples.length);

  let best = null;
  for (let i = 0; i <= samples.length - windowSamples; i += 1) {
    const window = samples.slice(i, i + windowSamples);
    let sum = 0;
    window.forEach((entry) => {
      sum += clampTrafficRatio(entry[ratioKey]) || 0;
    });
    const averageRatio = sum / window.length;
    if (!best || averageRatio > best.averageRatio) {
      best = {
        averageRatio,
        startTimestampMs: window[0].timestampMs,
        endTimestampMs: window[window.length - 1].timestampMs,
        windowMinutes: window.length * TRAFFIC_STATS_SNAPSHOT_STEP_MINUTES,
        sampleCount: window.length
      };
    }
  }

  if (!best) return null;
  return {
    ...best,
    statusLevel: getTrafficStatusLevelFromRatio(best.averageRatio)
  };
};

const calculateCommuteEscapeWindows = (series = [], ratioKey = 'overallRatio') => ({
  morning: calculateBestEscapeWindow(series, ratioKey, TRAFFIC_STATS_COMMUTE_WINDOWS.morning),
  afternoon: calculateBestEscapeWindow(series, ratioKey, TRAFFIC_STATS_COMMUTE_WINDOWS.afternoon)
});

const calculateWorstCongestionMoment = (series = [], ratioKey = 'overallRatio') => {
  const samples = (Array.isArray(series) ? series : [])
    .map((entry) => ({
      timestampMs: entry?.timestampMs,
      ratio: clampTrafficRatio(entry?.[ratioKey])
    }))
    .filter((entry) => Number.isFinite(entry.timestampMs) && entry.ratio !== null);
  if (samples.length === 0) return null;

  let worstIndex = 0;
  for (let i = 1; i < samples.length; i += 1) {
    if (samples[i].ratio < samples[worstIndex].ratio) {
      worstIndex = i;
    }
  }

  const worst = samples[worstIndex];
  const recoveryThreshold = TRAFFIC_STATS_STATUS_THRESHOLDS.freeFlow;
  let recoveredAtTimestampMs = null;
  for (let i = worstIndex + 1; i < samples.length; i += 1) {
    if (samples[i].ratio >= recoveryThreshold) {
      recoveredAtTimestampMs = samples[i].timestampMs;
      break;
    }
  }

  const recoveryMinutesToFreeFlow = Number.isFinite(recoveredAtTimestampMs)
    ? Math.max(0, Math.round((recoveredAtTimestampMs - worst.timestampMs) / (60 * 1000)))
    : null;

  return {
    timestampMs: worst.timestampMs,
    ratio: worst.ratio,
    statusLevel: getTrafficStatusLevelFromRatio(worst.ratio),
    recoveredAtTimestampMs,
    recoveryMinutesToFreeFlow
  };
};

const calculateRushHourPainMinutes = (series = [], ratioKey = 'overallRatio') => {
  const totals = {
    moderate: 0,
    heavy: 0,
    gridlock: 0,
    total: 0
  };

  (Array.isArray(series) ? series : []).forEach((entry) => {
    const ratio = clampTrafficRatio(entry?.[ratioKey]);
    if (ratio === null) return;
    const minutesOfDay = getLocalMinutesOfDayInCollectionZone(entry.timestampMs);
    if (!isRushHourMinutes(minutesOfDay)) return;

    const statusLevel = getTrafficStatusLevelFromRatio(ratio);
    if (statusLevel === 'moderate' || statusLevel === 'heavy' || statusLevel === 'gridlock') {
      totals[statusLevel] += TRAFFIC_STATS_SNAPSHOT_STEP_MINUTES;
      totals.total += TRAFFIC_STATS_SNAPSHOT_STEP_MINUTES;
    }
  });

  return totals;
};

const calculateBridgeBattle = (lionsGateAverageRatio, ironworkersAverageRatio) => {
  const lionsScore = clampTrafficRatio(lionsGateAverageRatio);
  const ironScore = clampTrafficRatio(ironworkersAverageRatio);
  if (lionsScore === null && ironScore === null) {
    return {
      winner: 'no-data',
      lionsGateScore: null,
      ironworkersScore: null,
      scoreDelta: null
    };
  }
  if (lionsScore !== null && ironScore === null) {
    return {
      winner: 'lions-gate',
      lionsGateScore: Math.round(lionsScore * 1000) / 10,
      ironworkersScore: null,
      scoreDelta: null
    };
  }
  if (lionsScore === null && ironScore !== null) {
    return {
      winner: 'ironworkers',
      lionsGateScore: null,
      ironworkersScore: Math.round(ironScore * 1000) / 10,
      scoreDelta: null
    };
  }

  const delta = lionsScore - ironScore;
  const winner = Math.abs(delta) < 0.005
    ? 'tie'
    : (delta > 0 ? 'lions-gate' : 'ironworkers');

  return {
    winner,
    lionsGateScore: Math.round(lionsScore * 1000) / 10,
    ironworkersScore: Math.round(ironScore * 1000) / 10,
    scoreDelta: Math.round(delta * 1000) / 10
  };
};

const buildDailyTrafficStatsSummary = (dayKey, intervals = [], segments = {}) => {
  const { series, segmentCounts } = buildTrafficIntervalSeries(intervals, segments);

  const overallAverageRatio = calculateAverageFromSeries(series, 'overallRatio');
  const lionsGateAverageRatio = calculateAverageFromSeries(series, 'lionsGateRatio');
  const ironworkersAverageRatio = calculateAverageFromSeries(series, 'ironworkersRatio');

  const firstTimestampMs = series.length > 0 ? series[0].timestampMs : null;
  const lastTimestampMs = series.length > 0 ? series[series.length - 1].timestampMs : null;

  return {
    dayKey,
    weekday: getWeekdayLabelFromTrafficDayKey(dayKey),
    hasData: series.length > 0,
    intervalCount: series.length,
    segmentCounts,
    firstTimestampMs,
    lastTimestampMs,
    averages: {
      overallRatio: overallAverageRatio,
      lionsGateRatio: lionsGateAverageRatio,
      ironworkersRatio: ironworkersAverageRatio
    },
    best30MinuteEscapeWindow: {
      overall: calculateBestEscapeWindow(series, 'overallRatio'),
      lionsGate: calculateBestEscapeWindow(series, 'lionsGateRatio'),
      ironworkers: calculateBestEscapeWindow(series, 'ironworkersRatio')
    },
    best30MinuteCommuteEscapeWindows: {
      overall: calculateCommuteEscapeWindows(series, 'overallRatio'),
      lionsGate: calculateCommuteEscapeWindows(series, 'lionsGateRatio'),
      ironworkers: calculateCommuteEscapeWindows(series, 'ironworkersRatio')
    },
    worstCongestionMoment: {
      overall: calculateWorstCongestionMoment(series, 'overallRatio'),
      lionsGate: calculateWorstCongestionMoment(series, 'lionsGateRatio'),
      ironworkers: calculateWorstCongestionMoment(series, 'ironworkersRatio')
    },
    rushHourPainMinutes: {
      overall: calculateRushHourPainMinutes(series, 'overallRatio'),
      lionsGate: calculateRushHourPainMinutes(series, 'lionsGateRatio'),
      ironworkers: calculateRushHourPainMinutes(series, 'ironworkersRatio')
    },
    bridgeBattle: calculateBridgeBattle(lionsGateAverageRatio, ironworkersAverageRatio)
  };
};

const buildSameWeekdayComparison = (selectedSummary, baselineSummaries = []) => {
  const usableBaseline = (Array.isArray(baselineSummaries) ? baselineSummaries : [])
    .filter((summary) => summary?.hasData)
    .filter((summary) => clampTrafficRatio(summary?.averages?.overallRatio) !== null);

  if (usableBaseline.length === 0) {
    return {
      available: false,
      sampleCount: 0,
      verdict: 'insufficient-data',
      baselineAverageRatio: null,
      selectedAverageRatio: clampTrafficRatio(selectedSummary?.averages?.overallRatio),
      deltaAverageRatio: null,
      baselineRushHourPainMinutes: null,
      selectedRushHourPainMinutes: Number.isFinite(selectedSummary?.rushHourPainMinutes?.overall?.total)
        ? selectedSummary.rushHourPainMinutes.overall.total
        : null,
      deltaRushHourPainMinutes: null,
      sampleDayKeys: []
    };
  }

  const averageRatios = usableBaseline
    .map((summary) => clampTrafficRatio(summary?.averages?.overallRatio))
    .filter((value) => value !== null);
  const painMinutes = usableBaseline
    .map((summary) => Number(summary?.rushHourPainMinutes?.overall?.total))
    .filter((value) => Number.isFinite(value));

  const baselineAverageRatio = averageRatios.length > 0
    ? (averageRatios.reduce((sum, value) => sum + value, 0) / averageRatios.length)
    : null;
  const baselineRushHourPainMinutes = painMinutes.length > 0
    ? (painMinutes.reduce((sum, value) => sum + value, 0) / painMinutes.length)
    : null;

  const selectedAverageRatio = clampTrafficRatio(selectedSummary?.averages?.overallRatio);
  const selectedRushHourPainMinutes = Number.isFinite(selectedSummary?.rushHourPainMinutes?.overall?.total)
    ? selectedSummary.rushHourPainMinutes.overall.total
    : null;

  const deltaAverageRatio =
    selectedAverageRatio !== null && baselineAverageRatio !== null
      ? (selectedAverageRatio - baselineAverageRatio)
      : null;
  const deltaRushHourPainMinutes =
    Number.isFinite(selectedRushHourPainMinutes) && Number.isFinite(baselineRushHourPainMinutes)
      ? (selectedRushHourPainMinutes - baselineRushHourPainMinutes)
      : null;

  let verdict = 'about-average';
  if (deltaAverageRatio !== null || deltaRushHourPainMinutes !== null) {
    const flowSignal = Number.isFinite(deltaAverageRatio) ? deltaAverageRatio : 0;
    const painSignal = Number.isFinite(deltaRushHourPainMinutes) ? deltaRushHourPainMinutes : 0;
    if (flowSignal >= 0.03 && painSignal <= -10) {
      verdict = 'better-than-usual';
    } else if (flowSignal <= -0.03 || painSignal >= 10) {
      verdict = 'worse-than-usual';
    }
  }

  return {
    available: true,
    sampleCount: usableBaseline.length,
    verdict,
    baselineAverageRatio,
    selectedAverageRatio,
    deltaAverageRatio,
    baselineRushHourPainMinutes,
    selectedRushHourPainMinutes,
    deltaRushHourPainMinutes,
    sampleDayKeys: usableBaseline
      .map((summary) => summary?.dayKey)
      .filter((value) => typeof value === 'string' && value.length > 0)
  };
};

const filterIntervalsByDayKey = (intervals = [], dayKey) => {
  if (!dayKey) return [];
  return (Array.isArray(intervals) ? intervals : []).filter((interval) => {
    const timestampMs = parseIntervalTimestampMs(interval);
    if (!Number.isFinite(timestampMs)) return false;
    return getTrafficServiceDayKeyFromTimestampMs(timestampMs) === dayKey;
  });
};

const getLatestIntervalTimestampMs = (intervals) => {
  if (!Array.isArray(intervals) || intervals.length === 0) return null;

  for (let i = intervals.length - 1; i >= 0; i -= 1) {
    const parsedMs = parseIntervalTimestampMs(intervals[i]);
    if (Number.isFinite(parsedMs)) return parsedMs;
  }

  return null;
};

const buildTrafficFreshness = (intervals, options = {}) => {
  const staleThresholdMinutes = Number.isFinite(options?.staleThresholdMinutes)
    ? Math.max(1, Math.round(options.staleThresholdMinutes))
    : TRAFFIC_STALE_THRESHOLD_MINUTES;
  const staleThresholdMs = staleThresholdMinutes * 60 * 1000;

  const latestSnapshotMs = Number.isFinite(options?.latestSnapshotMs)
    ? options.latestSnapshotMs
    : getLatestIntervalTimestampMs(intervals);

  if (!Number.isFinite(latestSnapshotMs)) {
    return {
      status: 'no-data',
      isStale: true,
      staleThresholdMinutes,
      staleThresholdMs,
      lastSnapshotAt: null,
      lastSnapshotTimestampMs: null,
      ageMs: null,
      minutesStale: null
    };
  }

  const ageMs = Math.max(0, Date.now() - latestSnapshotMs);
  return {
    status: ageMs > staleThresholdMs ? 'stale' : 'fresh',
    isStale: ageMs > staleThresholdMs,
    staleThresholdMinutes,
    staleThresholdMs,
    lastSnapshotAt: new Date(latestSnapshotMs).toISOString(),
    lastSnapshotTimestampMs: latestSnapshotMs,
    ageMs,
    minutesStale: Math.floor(ageMs / (60 * 1000))
  };
};

const recordTrafficCollectionSuccess = (timestampMs = Date.now()) => {
  const safeTimestampMs = Number.isFinite(timestampMs) ? timestampMs : Date.now();
  lastSuccessfulTrafficCollectionAtMs = safeTimestampMs;
  consecutiveTrafficCollectionFailures = 0;
  lastTrafficCollectionFailureAtMs = null;
  lastTrafficCollectionFailureMessage = null;
};

const recordTrafficCollectionFailure = (reason = 'unknown') => {
  consecutiveTrafficCollectionFailures += 1;
  lastTrafficCollectionFailureAtMs = Date.now();
  lastTrafficCollectionFailureMessage = String(reason || 'unknown');
};

const logTrafficCollectionWatchdog = () => {
  const now = Date.now();
  const fallbackLatestSnapshotMs = getLatestIntervalTimestampMs(trafficIntervals);
  const referenceSuccessMs = Number.isFinite(lastSuccessfulTrafficCollectionAtMs)
    ? lastSuccessfulTrafficCollectionAtMs
    : fallbackLatestSnapshotMs;
  const minutesSinceSuccess = Number.isFinite(referenceSuccessMs)
    ? Math.floor(Math.max(0, now - referenceSuccessMs) / (60 * 1000))
    : null;
  const freshness = buildTrafficFreshness(trafficIntervals, {
    latestSnapshotMs: referenceSuccessMs
  });

  const shouldAlert = (
    !Number.isFinite(referenceSuccessMs) ||
    (now - referenceSuccessMs) >= TRAFFIC_WATCHDOG_STALE_MS
  );

  if (shouldAlert) {
    if ((now - lastTrafficWatchdogAlertAtMs) >= (55 * 1000)) {
      const failureDetail = lastTrafficCollectionFailureMessage
        ? ` lastFailure="${lastTrafficCollectionFailureMessage}"`
        : '';
      console.error(
        `🚨 Watchdog alert: no successful traffic snapshot for ` +
        `${minutesSinceSuccess !== null ? `${minutesSinceSuccess}m` : 'unknown duration'} ` +
        `(threshold ${TRAFFIC_WATCHDOG_STALE_MINUTES}m, consecutiveFailures=${consecutiveTrafficCollectionFailures}).` +
        failureDetail
      );
      lastTrafficWatchdogAlertAtMs = now;
    }
    return;
  }

  console.log(
    `🔎 Watchdog OK: lastSnapshot=${freshness.lastSnapshotAt}, age=${freshness.minutesStale}m, ` +
    `consecutiveFailures=${consecutiveTrafficCollectionFailures}`
  );
};

const getLatestTimestampedInterval = (intervals) => {
  if (!Array.isArray(intervals) || intervals.length === 0) return null;
  for (let i = intervals.length - 1; i >= 0; i -= 1) {
    const interval = intervals[i];
    if (Number.isFinite(parseIntervalTimestampMs(interval))) {
      return interval;
    }
  }
  return null;
};

const getIntervalDataKeys = (interval) => {
  if (!interval || typeof interval !== 'object') return new Set();
  return new Set(
    Object.keys(interval).filter((key) => key !== 'timestamp')
  );
};

const areIntervalSchemasCompatible = (leftInterval, rightInterval, minOverlapRatio = 0.65) => {
  const leftKeys = getIntervalDataKeys(leftInterval);
  const rightKeys = getIntervalDataKeys(rightInterval);

  if (leftKeys.size === 0 || rightKeys.size === 0) {
    return false;
  }

  let overlapCount = 0;
  leftKeys.forEach((key) => {
    if (rightKeys.has(key)) overlapCount += 1;
  });

  const overlapRatio = overlapCount / Math.min(leftKeys.size, rightKeys.size);
  return overlapRatio >= minOverlapRatio;
};

const mergeIntervalsByTimestamp = (primaryIntervals, secondaryIntervals) => {
  const mergedByTimestamp = new Map();

  const pushInterval = (interval) => {
    const parsedMs = parseIntervalTimestampMs(interval);
    if (!Number.isFinite(parsedMs)) return;
    const dedupeKey = String(parsedMs);
    mergedByTimestamp.set(dedupeKey, interval);
  };

  // Primary first, then secondary overrides overlapping timestamps.
  if (Array.isArray(primaryIntervals)) {
    primaryIntervals.forEach(pushInterval);
  }
  if (Array.isArray(secondaryIntervals)) {
    secondaryIntervals.forEach(pushInterval);
  }

  return Array.from(mergedByTimestamp.entries())
    .sort((a, b) => Number(a[0]) - Number(b[0]))
    .map(([, interval]) => interval);
};

const buildTrackedDebugSegments = () => {
  const trackedSegments = {};
  Object.entries(segmentData).forEach(([segmentId, segment]) => {
    const coordinates = Array.isArray(segment?.coordinates) ? segment.coordinates : [];
    if (coordinates.length < 2) return;
    const sourceId = typeof segment?.sourceId === 'string' && segment.sourceId
      ? segment.sourceId
      : null;

    trackedSegments[segmentId] = {
      id: segmentId,
      sourceId,
      name: segment?.name || segmentId,
      description: segment?.description || segment?.name || segmentId,
      coordinates,
      type: segment?.type || 'road',
      hereReference: segment?.hereReference || null,
      numericIds: extractNumericTokens(segmentId, sourceId, segment?.name, segment?.hereReference)
    };
  });
  return trackedSegments;
};

// Fetch traffic data from HERE API using efficient bounding box approach
const fetchHereTrafficData = async () => {
  const buildNoDataResult = (dataSourceLabel = 'no-data') => {
    return {
      trafficData: [],
      segmentMetadata: {},
      allSegmentMetadata: {},
      debugMeta: {
        dataSource: dataSourceLabel,
        rawSegmentCount: 0,
        allSegmentCount: 0,
        filteredSegmentCount: 0,
        selectedSegmentCount: 0,
        manualTrackedConfiguredCount: MANUAL_TRACKED_SOURCE_IDS.size,
        manualTrackedMatchedCount: 0,
        filterMode: 'none',
        trackedSourceIds: []
      }
    };
  };

  if (!HERE_API_KEY || HERE_API_KEY === 'YOUR_HERE_API_KEY_NEEDED') {
    console.log('⚠️ HERE API key not configured; no live traffic data available.');
    return buildNoDataResult('no-data-no-here-key');
  }
  
  try {
    console.log('Fetching traffic data from HERE API (filtered for Tier 1+2 roads only)...');
    
    const response = await axios.get(HERE_BASE_URL, {
      params: {
        'in': `bbox:${NORTH_VAN_BBOX}`,
        'locationReferencing': 'shape',
        'apikey': HERE_API_KEY
      },
      timeout: 10000
    });
    
    const rawSegments = Array.isArray(response?.data?.results) ? response.data.results : [];
    console.log(`✅ HERE API returned ${rawSegments.length} traffic segments`);
    
    if (rawSegments.length === 0) {
      console.log('⚠️ HERE API returned no traffic segments; no live traffic data available.');
      return buildNoDataResult('no-data-empty-here-response');
    }

    // Build "all segments" payload for debug overlay (pink layer).
    const allSegmentMetadata = {};
    const segmentRecords = new Map();
    rawSegments.forEach((segment, index) => {
      const record = buildHereSegmentRecord(segment, index);
      if (!record) return;
      segmentRecords.set(segment, record);

      allSegmentMetadata[record.sourceId] = {
        id: record.sourceId,
        name: record.name,
        description: record.description,
        coordinates: record.coordinates,
        type: record.type,
        hereReference: record.hereReference,
        numericIds: record.numericIds,
        currentSpeed: record.currentSpeed,
        freeFlowSpeed: record.freeFlowSpeed,
        jamFactor: record.jamFactor,
        confidence: record.confidence
      };
    });
    
    // OPTIMIZATION: Filter segments to only Tier 1+2 roads before processing
    // Helper function to check if coordinates intersect with any of our critical roads
    const isSegmentInCriticalRoads = (segment) => {
      const segmentCoords = extractCoordinatesFromHereSegment(segment);
      if (segmentCoords.length === 0) return false;
      
      // Check if any coordinate falls within any of our critical road bounding boxes
      return NORTH_VAN_ROADS.some(road => {
        const [minLat, minLng, maxLat, maxLng] = road.bbox.split(',').map(Number);
        return segmentCoords.some(([lng, lat]) => {
          return lng >= minLng && lng <= maxLng && lat >= minLat && lat <= maxLat;
        });
      });
    };
    
    const isSegmentManuallyTracked = (segment) => {
      const record = segmentRecords.get(segment);
      if (!record || typeof record.sourceId !== 'string') return false;
      return MANUAL_TRACKED_SOURCE_IDS.has(record.sourceId);
    };

    // Track segments from critical road filters plus explicit manual picks.
    const criticalSegments = rawSegments.filter(isSegmentInCriticalRoads);
    const manuallyTrackedSegments = rawSegments.filter(isSegmentManuallyTracked);
    const filteredSegmentSet = new Set(criticalSegments);
    manuallyTrackedSegments.forEach((segment) => filteredSegmentSet.add(segment));
    const filteredSegments = [...filteredSegmentSet];

    let selectedSegments = filteredSegments;
    let filterMode = manuallyTrackedSegments.length > 0
      ? 'critical-plus-manual'
      : 'critical-only';
    if (filteredSegments.length < MIN_FILTERED_SEGMENTS_FOR_TRACKED) {
      filterMode = 'raw-fallback';
      selectedSegments = rawSegments;
      console.warn(
        `⚠️ Filtered segment count too low (${filteredSegments.length}). Falling back to all ${rawSegments.length} HERE segments.`
      );
    }
    if (manuallyTrackedSegments.length > 0) {
      console.log(`🧭 Manual tracked source IDs matched: ${manuallyTrackedSegments.length}/${MANUAL_TRACKED_SOURCE_IDS.size}`);
    }
    console.log(`🎯 Filtered ${rawSegments.length} segments to ${filteredSegments.length} tracked candidates (serving ${selectedSegments.length}, mode=${filterMode})`);
    
    // DEBUG: Log a few kept/rejected segments for troubleshooting.
    const kept = selectedSegments;
    const rejected = filterMode === 'raw-fallback'
      ? []
      : rawSegments.filter(seg => !filteredSegmentSet.has(seg));
    
    console.log('🔍 DEBUG - Sample segments KEPT:');
    kept.slice(0, 3).forEach((seg, i) => {
      const coords = seg.location?.shape?.links?.[0]?.points?.[0];
      console.log(`  ${i + 1}. ${coords?.lat?.toFixed?.(6)},${coords?.lng?.toFixed?.(6)} - Road: ${seg.location?.description || 'Unknown'}`);
    });
    
    console.log('🔍 DEBUG - Sample segments REJECTED:');  
    rejected.slice(0, 3).forEach((seg, i) => {
      const coords = seg.location?.shape?.links?.[0]?.points?.[0];
      console.log(`  ${i + 1}. ${coords?.lat?.toFixed?.(6)},${coords?.lng?.toFixed?.(6)} - Road: ${seg.location?.description || 'Unknown'}`);
    });
    
    // DEBUG: Specifically look for major infrastructure keywords
    const majorRoads = rawSegments.filter(seg => {
      const desc = (seg.location?.description || '').toLowerCase();
      return desc.includes('highway') || desc.includes('bridge') || desc.includes('trans-canada') || desc.includes('ironworkers') || desc.includes('lions gate');
    });
    
    console.log(`🏗️  DEBUG - Found ${majorRoads.length} segments with major infrastructure keywords:`);
    majorRoads.slice(0, 5).forEach((seg, i) => {
      const coords = seg.location?.shape?.links?.[0]?.points?.[0];
      const inTrackedSet = filterMode === 'raw-fallback'
        ? 'KEPT'
        : (filteredSegmentSet.has(seg) ? 'KEPT' : 'REJECTED');
      console.log(`  ${i + 1}. ${inTrackedSet}: ${seg.location?.description} at ${coords?.lat?.toFixed?.(6)},${coords?.lng?.toFixed?.(6)}`);
    });
    
    // Convert selected HERE traffic segments to tracked payload.
    const trafficData = [];
    const segmentMetadata = {};
    
    selectedSegments.forEach((segment, index) => {
      const record = segmentRecords.get(segment) || buildHereSegmentRecord(segment, index);
      if (!record) return;

      const currentSpeed = record.currentSpeed !== null ? record.currentSpeed : 50;
      const freeFlowSpeed = record.freeFlowSpeed !== null ? record.freeFlowSpeed : currentSpeed || 50;
      const flowRatio = freeFlowSpeed > 0 ? Math.min(1.0, currentSpeed / freeFlowSpeed) : 1.0;
      const segmentId = record.sourceId;
      if (typeof segmentId !== 'string' || segmentId.length === 0) return;
      
      trafficData.push({
        segmentId,
        ratio: Math.max(0.1, flowRatio),
        speed: currentSpeed,
        freeFlowSpeed,
        jamFactor: record.jamFactor ?? 0,
        confidence: record.confidence ?? 1.0
      });
      
      if (record.coordinates.length >= 2) {
        segmentMetadata[segmentId] = {
          id: segmentId,
          segmentId,
          name: record.name,
          description: record.description,
          coordinates: record.coordinates,
          type: record.type,
          bridgeHint: record.bridgeHint,
          sourceId: record.sourceId,
          hereReference: record.hereReference,
          numericIds: record.numericIds
        };
      }
    });
    
    const trackedSourceIds = [...new Set(
      Object.values(segmentMetadata)
        .map((segment) => segment?.sourceId)
        .filter((value) => typeof value === 'string' && value.length > 0)
    )];

    console.log(`✅ Processed ${trafficData.length} tracked segments (filtered from ${rawSegments.length} total, mode=${filterMode})`);
    console.log(`📍 All-routes debug coverage: ${Object.keys(allSegmentMetadata).length} segments`);

    return {
      trafficData,
      segmentMetadata,
      allSegmentMetadata,
      debugMeta: {
        dataSource: 'here-live',
        rawSegmentCount: rawSegments.length,
        allSegmentCount: Object.keys(allSegmentMetadata).length,
        filteredSegmentCount: filteredSegments.length,
        selectedSegmentCount: selectedSegments.length,
        manualTrackedConfiguredCount: MANUAL_TRACKED_SOURCE_IDS.size,
        manualTrackedMatchedCount: manuallyTrackedSegments.length,
        filterMode,
        trackedSourceIds
      }
    };
    
  } catch (error) {
    console.log('❌ HERE API failed; no live traffic data available:', error.response?.status, error.message);
    return buildNoDataResult('no-data-here-error');
  }
};

// Lions Gate Bridge Counter-Flow Data Collection
const BC_ATIS_URL = 'http://www.th.gov.bc.ca/ATIS/lgcws/private_status.htm';
const BC_ATIS_SOURCE_URLS = [
  BC_ATIS_URL,
  'https://www.th.gov.bc.ca/ATIS/lgcws/private_status.htm'
];

const extractVdsSection = (html, vdsId) => {
  if (typeof html !== 'string' || !html) return null;
  const escapedId = String(vdsId).replace(/[.*+?^${}()|[\]\\]/g, '\\$&');
  const regex = new RegExp(`VDS\\s*ID\\s*[:#-]?\\s*${escapedId}\\b[\\s\\S]*?(?=VDS\\s*ID\\s*[:#-]?\\s*\\d+\\b|ATC\\s*ID\\s*[:#-]?\\s*\\d+\\b|$)`, 'i');
  const match = html.match(regex);
  return match ? match[0] : null;
};

const extractLaneSection = (vdsSection, laneNumber) => {
  if (typeof vdsSection !== 'string' || !vdsSection) return null;
  const laneId = String(laneNumber).replace(/[.*+?^${}()|[\]\\]/g, '\\$&');
  const patterns = [
    new RegExp(
      `(?:Lane\\s*Number\\s*[:#-]?\\s*${laneId}\\b|Lane\\s+${laneId}\\b)[\\s\\S]*?(?=Lane\\s*(?:Number\\s*[:#-]?\\s*)?\\d+\\b|VDS\\s*ID\\s*[:#-]?\\s*\\d+\\b|ATC\\s*ID\\s*[:#-]?\\s*\\d+\\b|$)`,
      'i'
    ),
    new RegExp(
      `\\bLANE\\s*${laneId}\\b[\\s\\S]*?(?=\\bLANE\\s*\\d+\\b|VDS\\s*ID\\s*[:#-]?\\s*\\d+\\b|ATC\\s*ID\\s*[:#-]?\\s*\\d+\\b|$)`,
      'i'
    )
  ];

  for (const regex of patterns) {
    const match = vdsSection.match(regex);
    if (match && match[0]) return match[0];
  }
  return null;
};

const extractLaneSectionFromAnchorBlock = (vdsSection, laneNumber) => {
  if (typeof vdsSection !== 'string' || !vdsSection) return null;
  const laneId = String(laneNumber).replace(/[.*+?^${}()|[\]\\]/g, '\\$&');
  const anchorRegex = new RegExp(
    `<a[^>]*name\\s*=\\s*["']?lane\\s*${laneId}["']?[^>]*>[\\s\\S]*?(?=<a[^>]*name\\s*=\\s*["']?lane\\s*\\d+["']?[^>]*>|VDS\\s*ID\\s*[:#-]?\\s*\\d+\\b|ATC\\s*ID\\s*[:#-]?\\s*\\d+\\b|$)`,
    'i'
  );
  const match = vdsSection.match(anchorRegex);
  return match ? match[0] : null;
};

const decodeHtmlEntities = (text) => {
  if (typeof text !== 'string' || !text) return '';
  return text
    .replace(/&nbsp;/gi, ' ')
    .replace(/&amp;/gi, '&')
    .replace(/&lt;/gi, '<')
    .replace(/&gt;/gi, '>')
    .replace(/&#39;/g, '\'')
    .replace(/&quot;/gi, '"');
};

const htmlToPlainText = (text) => {
  if (typeof text !== 'string' || !text) return '';
  return decodeHtmlEntities(
    text
      .replace(/<\s*br\s*\/?>/gi, '\n')
      .replace(/<\/\s*(tr|p|div|li|td|th)\s*>/gi, '\n')
      .replace(/<[^>]*>/g, ' ')
  )
    .replace(/[ \t]+/g, ' ')
    .replace(/\s*\n\s*/g, '\n')
    .trim();
};

const parseCurrentLaneStatuses = (laneSection) => {
  if (typeof laneSection !== 'string' || !laneSection) return [];
  const plainText = htmlToPlainText(laneSection);
  if (!plainText) return [];

  const statuses = [];
  const patterns = [
    /Current\s+(?:Upstream|Downstream)\s+Loop\s+Status\s*:?\s*([^\r\n]+)/gi,
    /Current\s+(?:Upstream|Downstream)\s+Lane\s+Status\s*:?\s*([^\r\n]+)/gi,
    /Current\s+(?:Upstream|Downstream)\s+Status\s*:?\s*([^\r\n]+)/gi,
    /(?:Upstream|Downstream)\s+Current\s+Status\s*:?\s*([^\r\n]+)/gi
  ];

  patterns.forEach((regex) => {
    let match;
    while ((match = regex.exec(plainText)) !== null) {
      const statusText = String(match[1] || '')
        .replace(/\s+/g, ' ')
        .trim();
      if (statusText) statuses.push(statusText);
    }
  });

  return statuses;
};

const classifyLaneClosedFromStatus = (statusText) => {
  const text = String(statusText || '').trim().toLowerCase();
  if (!text) return null;

  if (/counter[\s-]*flow.*closed/.test(text)) return true;
  if (/counter[\s-]*flow.*open/.test(text)) return false;
  if (/\bclosed\b/.test(text)) return true;
  if (/\bopen\b|\bok\b|\bnormal\b|\bclear\b/.test(text)) return false;
  return null;
};

const extractLaneSectionFromPlainText = (vdsSection, laneNumber) => {
  const plainText = htmlToPlainText(vdsSection);
  if (!plainText) return null;
  const laneId = String(laneNumber).replace(/[.*+?^${}()|[\]\\]/g, '\\$&');
  const normalizedLines = plainText
    .split('\n')
    .map((line) => line.trim())
    .filter(Boolean);
  if (normalizedLines.length === 0) return null;

  const laneHeaderRegex = new RegExp(
    `^(?:Lane\\s*Number\\s*[:#-]?\\s*${laneId}\\b|Lane\\s*${laneId}\\b|Lane${laneId}\\b)`,
    'i'
  );
  const anyLaneHeaderRegex = /^(?:Lane\s*(?:Number\s*[:#-]?\s*)?\d+\b|Lane\d+\b)/i;
  const sensorBoundaryRegex = /^(?:VDS|ATC)\s*ID\b/i;

  const startIndex = normalizedLines.findIndex((line) => laneHeaderRegex.test(line));
  if (startIndex >= 0) {
    let endIndex = normalizedLines.length;
    for (let i = startIndex + 1; i < normalizedLines.length; i += 1) {
      const line = normalizedLines[i];
      if (sensorBoundaryRegex.test(line)) {
        endIndex = i;
        break;
      }
      if (anyLaneHeaderRegex.test(line) && !laneHeaderRegex.test(line)) {
        endIndex = i;
        break;
      }
    }

    const block = normalizedLines.slice(startIndex, endIndex).join('\n').trim();
    if (block) return block;
  }

  const blockRegex = new RegExp(
    `(?:^|\\n)\\s*(?:Lane\\s*Number\\s*[:#-]?\\s*${laneId}\\b|Lane\\s*${laneId}\\b|Lane${laneId}\\b)[\\s\\S]*?(?=\\n\\s*(?:Lane\\s*(?:Number\\s*[:#-]?\\s*)?\\d+\\b|Lane\\d+\\b|(?:VDS|ATC)\\s*ID\\b)|$)`,
    'i'
  );
  const blockMatch = plainText.match(blockRegex);
  return blockMatch ? blockMatch[0].trim() : null;
};

const extractLaneSectionRobust = (vdsSection, laneNumber) => {
  const strategies = [
    { source: 'plain-text', fn: extractLaneSectionFromPlainText },
    { source: 'html-anchor', fn: extractLaneSectionFromAnchorBlock },
    { source: 'generic', fn: extractLaneSection }
  ];

  for (const strategy of strategies) {
    const section = strategy.fn(vdsSection, laneNumber);
    if (typeof section === 'string' && section.trim().length > 0) {
      return { section, source: strategy.source };
    }
  }

  return { section: null, source: null };
};

const parseLaneTelemetryMetrics = (laneSection) => {
  const plainText = htmlToPlainText(laneSection);
  if (!plainText) {
    return {
      averagedSpeeds: [],
      loopVolumes: []
    };
  }

  const averagedSpeeds = [];
  const speedRegex = /Last\s+Averaged\s+Lane\s+Data\s*:\s*Speed\s*=\s*(-?\d+(?:\.\d+)?)/gi;
  let match;
  while ((match = speedRegex.exec(plainText)) !== null) {
    const speed = Number(match[1]);
    if (Number.isFinite(speed)) averagedSpeeds.push(speed);
  }

  const loopVolumes = [];
  const volumeRegex = /Last\s+(?:Upstream|Downstream)\s+Loop\s+Data\s*:\s*Volume\s*=\s*(-?\d+(?:\.\d+)?)/gi;
  while ((match = volumeRegex.exec(plainText)) !== null) {
    const volume = Number(match[1]);
    if (Number.isFinite(volume)) loopVolumes.push(volume);
  }

  return {
    averagedSpeeds,
    loopVolumes
  };
};

const resolveLaneClosedFromSection = (laneSection) => {
  if (typeof laneSection !== 'string' || !laneSection) return null;

  const statuses = parseCurrentLaneStatuses(laneSection);
  const metrics = parseLaneTelemetryMetrics(laneSection);
  let closedScore = 0;
  let openScore = 0;

  statuses.forEach((statusText) => {
    const classified = classifyLaneClosedFromStatus(statusText);
    if (classified === true) {
      closedScore += /counter[\s-]*flow.*closed/i.test(statusText) ? 3 : 2;
    } else if (classified === false) {
      openScore += /counter[\s-]*flow.*open/i.test(statusText) ? 3 : 2;
    }
  });

  if (metrics.averagedSpeeds.some((value) => value === -1)) {
    closedScore += 2;
  } else if (metrics.averagedSpeeds.some((value) => value >= 0)) {
    openScore += 1;
  }

  if (metrics.loopVolumes.length > 0) {
    if (metrics.loopVolumes.every((value) => value === -1)) {
      closedScore += 1;
    } else if (metrics.loopVolumes.some((value) => value >= 0)) {
      openScore += 1;
    }
  }

  if (closedScore === 0 && openScore === 0) return null;
  if (closedScore === openScore) return null;
  return closedScore > openScore;
};

const resolveLaneClosedAcrossSensors = (laneStates = [], fallbackValue = null) => {
  const resolved = laneStates.filter((value) => typeof value === 'boolean');
  if (resolved.length === 0) {
    return typeof fallbackValue === 'boolean' ? fallbackValue : null;
  }

  let closedCount = 0;
  let openCount = 0;
  resolved.forEach((value) => {
    if (value) closedCount += 1;
    else openCount += 1;
  });

  if (closedCount > openCount) return true;
  if (openCount > closedCount) return false;
  if (typeof fallbackValue === 'boolean') return fallbackValue;
  return resolved[0];
};

const recordCounterFlowFailure = (reason, timestamp = new Date().toISOString(), rawData = null) => {
  counterFlowData.lastChecked = timestamp;
  counterFlowData.lastError = reason;
  if (rawData) {
    counterFlowData.rawData = {
      ...(counterFlowData.rawData || {}),
      failure: rawData
    };
  }
};

const scrapeCounterFlowData = async () => {
  try {
    console.log('🌉 Scraping Lions Gate counter-flow data...');
    let html = null;
    let sourceUrlUsed = null;
    const fetchErrors = [];
    for (const candidateUrl of BC_ATIS_SOURCE_URLS) {
      try {
        const response = await axios.get(candidateUrl, {
          timeout: 10000,
          headers: {
            'User-Agent': 'Mozilla/5.0 (compatible; TrafficMonitor/1.0)'
          }
        });
        const candidateHtml = typeof response?.data === 'string'
          ? response.data
          : String(response?.data || '');
        if (candidateHtml.trim()) {
          html = candidateHtml;
          sourceUrlUsed = candidateUrl;
          break;
        }
      } catch (error) {
        fetchErrors.push(`${candidateUrl}: ${error.message}`);
      }
    }

    if (!html) {
      const timestamp = new Date().toISOString();
      const reason = fetchErrors.length > 0
        ? `Failed to fetch ATIS source (${fetchErrors.join(' | ')})`
        : 'Failed to fetch ATIS source (empty response)';
      console.log(`⚠️ ${reason}`);
      recordCounterFlowFailure(reason, timestamp, { fetchErrors });
      return null;
    }

    // Parse HTML for VDS lane data (primary sensors 201 + 202).
    const vds201Section = extractVdsSection(html, 201);
    const vds202Section = extractVdsSection(html, 202);
    if (!vds201Section) {
      console.log('⚠️ Could not find VDS ID: 201 section; attempting global lane parse fallback');
    }
    const sensorSections = [
      { id: 201, section: vds201Section },
      { id: 202, section: vds202Section }
    ].filter((entry) => typeof entry.section === 'string' && entry.section.length > 0);
    if (sensorSections.length === 0) {
      sensorSections.push({ id: 'global', section: html });
    }

    const previousLane1Closed = typeof counterFlowData?.rawData?.lane1Closed === 'boolean'
      ? counterFlowData.rawData.lane1Closed
      : null;
    const previousLane2Closed = typeof counterFlowData?.rawData?.lane2Closed === 'boolean'
      ? counterFlowData.rawData.lane2Closed
      : null;

    const sensorDebug = sensorSections.map((entry) => {
      const lane1Extracted = extractLaneSectionRobust(entry.section, 1);
      const lane2Extracted = extractLaneSectionRobust(entry.section, 2);
      const lane1Section = lane1Extracted.section;
      const lane2Section = lane2Extracted.section;
      const lane1CurrentStatuses = parseCurrentLaneStatuses(lane1Section);
      const lane2CurrentStatuses = parseCurrentLaneStatuses(lane2Section);
      const lane1Closed = resolveLaneClosedFromSection(lane1Section);
      const lane2Closed = resolveLaneClosedFromSection(lane2Section);
      return {
        vdsId: entry.id,
        lane1Closed,
        lane2Closed,
        lane1Source: lane1Extracted.source,
        lane2Source: lane2Extracted.source,
        lane1CurrentStatuses,
        lane2CurrentStatuses,
        lane1SectionPreview: String(lane1Section || '').substring(0, 220),
        lane2SectionPreview: String(lane2Section || '').substring(0, 220)
      };
    });

    const lane1Closed = resolveLaneClosedAcrossSensors(
      sensorDebug.map((entry) => entry.lane1Closed),
      previousLane1Closed
    );
    const lane2Closed = resolveLaneClosedAcrossSensors(
      sensorDebug.map((entry) => entry.lane2Closed),
      previousLane2Closed
    );
    if (lane1Closed === null || lane2Closed === null) {
      const timestamp = new Date().toISOString();
      const reason = 'Could not reliably resolve current lane states for VDS 201/202';
      console.log(`⚠️ ${reason}; skipping update`);
      recordCounterFlowFailure(reason, timestamp, {
        sourceUrlUsed,
        sensorDebug,
        vds201SectionPreview: typeof vds201Section === 'string' ? vds201Section.substring(0, 300) : null,
        vds202SectionPreview: typeof vds202Section === 'string' ? vds202Section.substring(0, 300) : null
      });
      return null;
    }
    
    // Determine counter-flow configuration
    let status;
    let lanesOutbound;
    
    if (lane1Closed && lane2Closed) {
      status = 'outbound-0'; // Both lanes closed (maintenance?)
      lanesOutbound = 0;
    } else if (lane1Closed && !lane2Closed) {
      status = 'outbound-1'; // 1 lane outbound, 2 inbound
      lanesOutbound = 1;
    } else if (!lane1Closed && !lane2Closed) {
      status = 'outbound-2'; // 2 lanes outbound, 1 inbound  
      lanesOutbound = 2;
    } else {
      status = 'outbound-unknown';
      lanesOutbound = 1; // fallback
    }
    
    const timestamp = new Date().toISOString();
    
    console.log(
      `✅ Lions Gate status: ${status} (${lanesOutbound} lanes outbound) ` +
      `[lane1Closed=${lane1Closed}, lane2Closed=${lane2Closed}]`
    );
    
    return {
      status,
      lanesOutbound,
      timestamp,
      sourceUrlUsed,
      rawData: {
        lane1Closed,
        lane2Closed,
        sensorDebug,
        vds201Section: typeof vds201Section === 'string' ? vds201Section.substring(0, 500) : null, // First 500 chars for debugging
        vds202Section: typeof vds202Section === 'string' ? vds202Section.substring(0, 500) : null
      }
    };
    
  } catch (error) {
    console.log('❌ Counter-flow scraping failed:', error.message);
    recordCounterFlowFailure(`Counter-flow scraping failed: ${error.message}`);
    return null;
  }
};

const updateCounterFlowData = async () => {
  const newData = await scrapeCounterFlowData();
  
  if (!newData) return; // Skip update if scraping failed
  
  const previousStatus = counterFlowData.currentStatus;
  const previousLanesOutbound = Number(counterFlowData.lanesOutbound);
  const statusChanged = previousStatus !== newData.status ||
    previousLanesOutbound !== Number(newData.lanesOutbound);
  
  if (statusChanged) {
    // Log status change
    console.log(
      `🔄 Counter-flow changed: ${previousStatus || 'unknown'} (${Number.isFinite(previousLanesOutbound) ? previousLanesOutbound : '?'}) ` +
      `→ ${newData.status} (${newData.lanesOutbound})`
    );
    
    // Add to history
    counterFlowData.history.push({
      from: previousStatus,
      to: newData.status,
      timestamp: newData.timestamp,
      duration: previousStatus && counterFlowData.statusSince ? 
        new Date(newData.timestamp) - new Date(counterFlowData.statusSince) : null
    });
    
    // Keep last 100 changes only
    if (counterFlowData.history.length > 100) {
      counterFlowData.history = counterFlowData.history.slice(-100);
    }
    
    // Update current status
    counterFlowData.statusSince = newData.timestamp;
  }
  
  // Always update these
  counterFlowData.currentStatus = newData.status;
  counterFlowData.lastChecked = newData.timestamp;
  counterFlowData.lanesOutbound = newData.lanesOutbound;
  counterFlowData.rawData = newData.rawData;
  counterFlowData.lastError = null;
  counterFlowData.sourceUrl = newData.sourceUrlUsed || counterFlowData.sourceUrl || null;
};

const bootstrapCounterFlowFromDatabase = async () => {
  if (!db) return;
  try {
    const seedData = await db.getTodayTrafficData(2);
    const seedCounterFlow = seedData?.counterFlow;
    if (!seedCounterFlow || typeof seedCounterFlow !== 'object') return;

    const seededStatus = typeof seedCounterFlow.currentStatus === 'string'
      ? seedCounterFlow.currentStatus
      : null;
    const seededLanes = Number(seedCounterFlow.lanesOutbound);
    const hasSeededLanes = Number.isFinite(seededLanes);
    if (!seededStatus && !hasSeededLanes) return;

    counterFlowData.currentStatus = seededStatus || counterFlowData.currentStatus;
    counterFlowData.lanesOutbound = hasSeededLanes ? seededLanes : counterFlowData.lanesOutbound;
    counterFlowData.statusSince = seedCounterFlow.statusSince || counterFlowData.statusSince;
    counterFlowData.lastChecked = seedCounterFlow.lastChecked || counterFlowData.lastChecked;
    counterFlowData.lastError = null;
    console.log(
      `🌉 Bootstrapped counter-flow from DB: status=${counterFlowData.currentStatus || 'unknown'}, ` +
      `lanesOutbound=${Number.isFinite(Number(counterFlowData.lanesOutbound)) ? Number(counterFlowData.lanesOutbound) : 'unknown'}`
    );
  } catch (error) {
    console.log(`⚠️ Counter-flow DB bootstrap failed: ${error.message}`);
  }
};

// Collect traffic data
const collectTrafficData = async () => {
  try {
    const timestamp = new Date().toISOString();
    console.log(`🚗 Collecting traffic data at ${timestamp}`);
    
    const fetchResult = await fetchHereTrafficData();
    const trafficData = Array.isArray(fetchResult?.trafficData) ? fetchResult.trafficData : [];
    const segmentMetadata = fetchResult?.segmentMetadata && typeof fetchResult.segmentMetadata === 'object'
      ? fetchResult.segmentMetadata
      : {};
    
    // Replace segment data entirely with new HERE metadata
    if (segmentMetadata && Object.keys(segmentMetadata).length > 0) {
      segmentData = segmentMetadata; // REPLACE, don't merge
      console.log(`📍 Replaced segment data with ${Object.keys(segmentMetadata).length} HERE segments`);
    }

    updateDebugRouteSnapshotFromFetch(fetchResult, timestamp);

    if (trafficData.length === 0) {
      console.warn('⚠️ No live traffic data returned; skipping interval capture.');
      recordTrafficCollectionFailure('No live traffic data returned');
      return;
    }
    
    // Convert to interval format
    const interval = {
      timestamp: timestamp,
    };
    
    trafficData.forEach((data) => {
      interval[data.segmentId] = data.ratio;
    });
    
    trafficIntervals.push(interval);
    
    // Keep a bounded in-memory timeline for live fallback responses.
    if (trafficIntervals.length > 720) {
      trafficIntervals = trafficIntervals.slice(-720);
    }
    invalidateTrafficTodayCache();
    recordTrafficCollectionSuccess(parseIntervalTimestampMs(interval) || Date.now());
    
    // Save to database if available
    if (db) {
      try {
        // Clean segmentMetadata for database storage (remove complex HERE API objects)
        const cleanSegmentData = {};
        Object.keys(segmentMetadata).forEach((segmentId) => {
          const segment = segmentMetadata[segmentId];
          cleanSegmentData[segmentId] = {
            id: segment.id || segmentId,
            segmentId: segment.segmentId || segmentId,
            name: segment.name,
            description: segment.description || null,
            coordinates: segment.coordinates,
            type: segment.type,
            bridgeHint: segment.bridgeHint || null,
            sourceId: segment.sourceId || segmentId,
            hereReference: segment.hereReference || null,
            numericIds: Array.isArray(segment.numericIds) ? segment.numericIds : []
            // Skip originalData - contains large HERE payloads
          };
        });
        
        await db.saveTrafficSnapshot(interval, cleanSegmentData, counterFlowData);
      } catch (error) {
        console.error('❌ Database save failed, continuing with memory storage:', error.message);
        console.error('❌ Full database error:', error);
      }
    }
    
    console.log(`✅ Collected data for ${trafficData.length} segments. Total intervals: ${trafficIntervals.length}`);
    
  } catch (error) {
    console.error('❌ Error collecting traffic data:', error.message);
    recordTrafficCollectionFailure(error.message);
  }
};

// API endpoints
app.get('/health', (req, res) => {
  const freshness = buildTrafficFreshness(trafficIntervals);
  const minutesSinceSuccessfulCollection = Number.isFinite(lastSuccessfulTrafficCollectionAtMs)
    ? Math.floor(Math.max(0, Date.now() - lastSuccessfulTrafficCollectionAtMs) / (60 * 1000))
    : null;
  res.json({
    status: 'healthy',
    timestamp: new Date().toISOString(),
    segments: Object.keys(segmentData).length,
    intervals: trafficIntervals.length,
    collecting: isCollecting,
    heapUsedMb: getHeapUsedMb(),
    activeTrafficHeavyReads,
    queuedTrafficHeavyReads: trafficHeavyReadQueue.length,
    trafficTodayCacheEntries: trafficTodayCache.size,
    trafficDayWindowCacheEntries: trafficDayWindowCache.size,
    trafficStatsCacheEntries: trafficStatsCache.size,
    freshness,
    watchdog: {
      staleThresholdMinutes: TRAFFIC_WATCHDOG_STALE_MINUTES,
      lastSuccessfulCollectionAt: Number.isFinite(lastSuccessfulTrafficCollectionAtMs)
        ? new Date(lastSuccessfulTrafficCollectionAtMs).toISOString()
        : null,
      minutesSinceSuccessfulCollection,
      consecutiveFailures: consecutiveTrafficCollectionFailures,
      lastFailureAt: Number.isFinite(lastTrafficCollectionFailureAtMs)
        ? new Date(lastTrafficCollectionFailureAtMs).toISOString()
        : null,
      lastFailureMessage: lastTrafficCollectionFailureMessage
    }
  });
});

app.get('/api/traffic/window', async (req, res) => {
  try {
    if (!db) {
      res.status(503).json({
        error: 'Database is required for day-window traffic queries'
      });
      return;
    }

    const centerDayRaw = String(req.query.centerDay || req.query.day || '').trim();
    const centerDayMatch = centerDayRaw.match(/^(\d{4})-(\d{2})-(\d{2})$/);
    if (!centerDayMatch) {
      res.status(400).json({
        error: 'Missing or invalid centerDay. Expected YYYY-MM-DD.'
      });
      return;
    }

    const requestedRadiusRaw = Number.parseInt(req.query.radius, 10);
    const requestedRadius = Number.isFinite(requestedRadiusRaw) ? requestedRadiusRaw : 1;
    const radiusDays = Math.max(0, Math.min(TRAFFIC_DAY_WINDOW_MAX_RADIUS, requestedRadius));
    if (radiusDays !== requestedRadius) {
      console.warn(
        `⚠️ /api/traffic/window requested radius=${requestedRadius} capped to ${radiusDays} ` +
        `(max ${TRAFFIC_DAY_WINDOW_MAX_RADIUS})`
      );
    }

    const refreshParam = String(req.query.refresh || '').toLowerCase();
    const bypassCache = refreshParam === '1' || refreshParam === 'true' || refreshParam === 'yes';
    const cacheKey = getTrafficDayWindowCacheKey(centerDayRaw, radiusDays);

    if (!bypassCache) {
      const cachedPayload = getTrafficDayWindowCachePayload(cacheKey);
      if (cachedPayload) {
        console.log(`📦 /api/traffic/window cache hit (${cacheKey})`);
        res.set('X-Traffic-Cache', 'HIT');
        res.json(cachedPayload);
        return;
      }
    }

    console.log(`📦 /api/traffic/window cache miss (${cacheKey}${bypassCache ? ', bypassed' : ''})`);
    res.set('X-Traffic-Cache', 'MISS');

    let inFlightGate = null;
    if (!bypassCache) {
      const existingInFlight = trafficDayWindowInFlight.get(cacheKey);
      if (existingInFlight && existingInFlight.promise) {
        console.log(`⏳ /api/traffic/window waiting for in-flight fetch (${cacheKey})`);
        try {
          await existingInFlight.promise;
        } catch (_error) {
          // Ignore gate wait errors and proceed below.
        }
        const waitedCachePayload = getTrafficDayWindowCachePayload(cacheKey);
        if (waitedCachePayload) {
          console.log(`📦 /api/traffic/window in-flight cache hit (${cacheKey})`);
          res.set('X-Traffic-Cache', 'WAIT-HIT');
          res.json(waitedCachePayload);
          return;
        }
      }

      let resolveGate;
      const gatePromise = new Promise((resolve) => {
        resolveGate = resolve;
      });
      inFlightGate = { promise: gatePromise, resolve: resolveGate };
      trafficDayWindowInFlight.set(cacheKey, inFlightGate);
    }

    try {
      console.log(`📊 API Request: /api/traffic/window centerDay=${centerDayRaw} radius=${radiusDays}`);
      const dbData = await runTrafficHeavyRead(
        '/api/traffic/window',
        () => db.getTrafficDataForDayWindow(centerDayRaw, radiusDays)
      );
      if (!dbData) {
        res.status(500).json({
          error: 'Failed to fetch day-window traffic data'
        });
        return;
      }

      const segmentCount = Object.keys(dbData.segments || {}).length;
      const windowFreshness = buildTrafficFreshness(dbData.intervals || []);
      const response = {
        intervals: dbData.intervals || [],
        segments: dbData.segments || {},
        totalSegments: segmentCount,
        currentIntervalIndex: (dbData.intervals || []).length - 1,
        maxInterval: (dbData.intervals || []).length - 1,
        coverage: `North Vancouver day window: ${segmentCount} segments across ${NORTH_VAN_ROADS.length} major roads`,
        dataSource: 'database-window',
        counterFlow: {
          status: counterFlowData.currentStatus ?? dbData.counterFlow?.currentStatus,
          lanesOutbound: counterFlowData.lanesOutbound || dbData.counterFlow?.lanesOutbound || 1,
          statusSince: counterFlowData.statusSince || dbData.counterFlow?.statusSince,
          lastChecked: counterFlowData.lastChecked || dbData.counterFlow?.lastChecked,
          durationMs: (counterFlowData.statusSince || dbData.counterFlow?.statusSince)
            ? Date.now() - new Date(counterFlowData.statusSince || dbData.counterFlow.statusSince).getTime()
            : null
        },
        fromDatabase: true,
        centerDayKey: dbData.centerDayKey || centerDayRaw,
        radiusDays: dbData.radiusDays ?? radiusDays,
        startDayKey: dbData.startDayKey,
        endDayKeyExclusive: dbData.endDayKeyExclusive,
        dbRecordCount: dbData.recordCount ?? (dbData.intervals || []).length,
        freshness: windowFreshness
      };
      setTrafficDayWindowCachePayload(cacheKey, response);
      res.json(response);
    } finally {
      if (inFlightGate && trafficDayWindowInFlight.get(cacheKey) === inFlightGate) {
        inFlightGate.resolve();
        trafficDayWindowInFlight.delete(cacheKey);
      }
    }
  } catch (error) {
    if (error?.code === 'MEMORY_PRESSURE_HARD_REJECT' || error?.code === 'HEAVY_READ_QUEUE_TIMEOUT') {
      console.warn(`⚠️ /api/traffic/window degraded response: ${error.code} (${error.message})`);
      res.set('Retry-After', '5');
      res.status(503).json({
        error: 'Traffic day-window temporarily unavailable; please retry in a few seconds.',
        code: error.code
      });
      return;
    }
    console.error('❌ Error in /api/traffic/window:', error);
    res.status(500).json({
      error: 'Failed to fetch traffic day-window',
      details: error.message
    });
  }
});

app.get('/api/traffic/today', async (req, res) => {
  try {
    let response;
    const requestedServiceDaysRaw = Math.max(
      1,
      Math.min(
        TRAFFIC_TODAY_MAX_SERVICE_DAYS,
        Number.parseInt(req.query.serviceDays, 10) || TRAFFIC_TODAY_DEFAULT_SERVICE_DAYS
      )
    );
    const requestedServiceDays = Math.min(
      TRAFFIC_TODAY_RUNTIME_SAFE_MAX_SERVICE_DAYS,
      requestedServiceDaysRaw
    );
    if (requestedServiceDays !== requestedServiceDaysRaw) {
      console.warn(
        `⚠️ /api/traffic/today requested serviceDays=${requestedServiceDaysRaw} ` +
        `capped to ${requestedServiceDays} (runtime safe max ${TRAFFIC_TODAY_RUNTIME_SAFE_MAX_SERVICE_DAYS})`
      );
    }
    const refreshParam = String(req.query.refresh || '').toLowerCase();
    const bypassCache = refreshParam === '1' || refreshParam === 'true' || refreshParam === 'yes';
    const cacheKey = getTrafficTodayCacheKey(requestedServiceDays);
    if (!bypassCache) {
      const cachedPayload = getTrafficTodayCachePayload(cacheKey);
      if (cachedPayload) {
        console.log(`📦 /api/traffic/today cache hit (${cacheKey})`);
        res.set('X-Traffic-Cache', 'HIT');
        res.json(cachedPayload);
        return;
      }
    }
    console.log(`📦 /api/traffic/today cache miss (${cacheKey}${bypassCache ? ', bypassed' : ''})`);
    res.set('X-Traffic-Cache', 'MISS');
    let inFlightGate = null;
    if (!bypassCache) {
      const existingInFlight = trafficTodayInFlight.get(cacheKey);
      if (existingInFlight && existingInFlight.promise) {
        console.log(`⏳ /api/traffic/today waiting for in-flight fetch (${cacheKey})`);
        try {
          await existingInFlight.promise;
        } catch (_error) {
          // Ignore gate wait errors and proceed to fresh read below.
        }
        const waitedCachePayload = getTrafficTodayCachePayload(cacheKey);
        if (waitedCachePayload) {
          console.log(`📦 /api/traffic/today in-flight cache hit (${cacheKey})`);
          res.set('X-Traffic-Cache', 'WAIT-HIT');
          res.json(waitedCachePayload);
          return;
        }
      }

      let resolveGate;
      const gatePromise = new Promise((resolve) => {
        resolveGate = resolve;
      });
      inFlightGate = { promise: gatePromise, resolve: resolveGate };
      trafficTodayInFlight.set(cacheKey, inFlightGate);
    }

    try {
      // Try database first if available
      if (db) {
        console.log(`📊 API Request: /api/traffic/today - checking database...`);
        try {
          const latestMemoryIntervalMs = getLatestIntervalTimestampMs(trafficIntervals);
          const memoryDataAgeMs = Number.isFinite(latestMemoryIntervalMs)
            ? Math.max(0, Date.now() - latestMemoryIntervalMs)
            : null;
          const dbData = await runTrafficHeavyRead(
            '/api/traffic/today',
            () => db.getTodayTrafficData(requestedServiceDays)
          );
          console.log(`🔍 DB data received: intervals=${dbData?.intervals?.length || 0}, segments=${Object.keys(dbData?.segments || {}).length}`);
          
          if (dbData && dbData.intervals.length > 0) {
            const latestDbIntervalMs = getLatestIntervalTimestampMs(dbData.intervals);
            const dbDataAgeMs = Number.isFinite(latestDbIntervalMs)
              ? Math.max(0, Date.now() - latestDbIntervalMs)
              : Number.POSITIVE_INFINITY;
            const isDbFreshEnough = dbDataAgeMs <= TRAFFIC_DB_STALE_MAX_AGE_MS;
            const latestDbInterval = getLatestTimestampedInterval(dbData.intervals);
            const latestMemoryInterval = getLatestTimestampedInterval(trafficIntervals);
            const canMergeDbAndMemory = areIntervalSchemasCompatible(latestDbInterval, latestMemoryInterval);

            if (!isDbFreshEnough) {
              console.warn(
                `⚠️ DB data is stale (age ${Math.round(dbDataAgeMs / 1000)}s, threshold ${Math.round(TRAFFIC_DB_STALE_MAX_AGE_MS / 1000)}s). Serving hybrid DB+memory response.`
              );
              if (!canMergeDbAndMemory) {
                console.warn('⚠️ DB and memory interval schemas appear different; merging by timestamp with unioned segment metadata.');
              }
              const mergedIntervals = mergeIntervalsByTimestamp(dbData.intervals, trafficIntervals);
              const mergedLatestMs = getLatestIntervalTimestampMs(mergedIntervals);
              const mergedAgeMs = Number.isFinite(mergedLatestMs)
                ? Math.max(0, Date.now() - mergedLatestMs)
                : null;
              const mergedFreshness = buildTrafficFreshness(mergedIntervals, {
                latestSnapshotMs: mergedLatestMs
              });
              const mergedSegments = {
                ...(dbData.segments || {}),
                ...(segmentData || {})
              };
              const mergedSegmentCount = Object.keys(mergedSegments).length;
              response = {
                intervals: mergedIntervals,
                segments: mergedSegments,
                totalSegments: mergedSegmentCount,
                currentIntervalIndex: mergedIntervals.length - 1,
                maxInterval: mergedIntervals.length - 1,
                coverage: `North Vancouver comprehensive: ${mergedSegmentCount} segments across ${NORTH_VAN_ROADS.length} major roads`,
                dataSource: canMergeDbAndMemory ? 'hybrid-db-memory' : 'hybrid-db-memory-incompatible',
                counterFlow: {
                  status: counterFlowData.currentStatus ?? dbData.counterFlow?.currentStatus,
                  lanesOutbound: counterFlowData.lanesOutbound || dbData.counterFlow?.lanesOutbound || 1,
                  statusSince: counterFlowData.statusSince || dbData.counterFlow?.statusSince,
                  lastChecked: counterFlowData.lastChecked || dbData.counterFlow?.lastChecked,
                  durationMs: (counterFlowData.statusSince || dbData.counterFlow?.statusSince)
                    ? Date.now() - new Date(counterFlowData.statusSince || dbData.counterFlow.statusSince).getTime()
                    : null
                },
                fromDatabase: true,
                fromMemory: true,
                serviceDays: requestedServiceDays,
                schemaCompatible: canMergeDbAndMemory,
                dbRecordCount: dbData.recordCount,
                latestIntervalAgeMs: mergedAgeMs,
                dbLatestIntervalAgeMs: Number.isFinite(dbDataAgeMs) ? dbDataAgeMs : null,
                memoryLatestIntervalAgeMs: memoryDataAgeMs,
                freshness: mergedFreshness
              };
              console.log(`✅ Served hybrid dataset: ${mergedIntervals.length} merged intervals (DB stale, memory tail appended)`);
              setTrafficTodayCachePayload(cacheKey, response);
              res.json(response);
              return;
            } else {
              const dbFreshness = buildTrafficFreshness(dbData.intervals, {
                latestSnapshotMs: latestDbIntervalMs
              });
              response = {
                intervals: dbData.intervals,
                segments: dbData.segments,
                totalSegments: Object.keys(dbData.segments).length,
                currentIntervalIndex: dbData.intervals.length - 1,
                maxInterval: dbData.intervals.length - 1,
                coverage: `North Vancouver comprehensive: ${Object.keys(dbData.segments).length} segments across ${NORTH_VAN_ROADS.length} major roads`,
                dataSource: 'database-persistent',
                counterFlow: {
                  status: dbData.counterFlow.currentStatus,
                  lanesOutbound: dbData.counterFlow.lanesOutbound || 1,
                  statusSince: dbData.counterFlow.statusSince,
                  lastChecked: dbData.counterFlow.lastChecked,
                  durationMs: dbData.counterFlow.statusSince ? 
                    Date.now() - new Date(dbData.counterFlow.statusSince).getTime() : null
                },
                fromDatabase: true,
                serviceDays: requestedServiceDays,
                dbRecordCount: dbData.recordCount,
                latestIntervalAgeMs: Number.isFinite(dbDataAgeMs) ? dbDataAgeMs : null,
                memoryLatestIntervalAgeMs: memoryDataAgeMs,
                freshness: dbFreshness
              };
              console.log(`✅ Successfully served ${dbData.intervals.length} intervals from database (expanded coverage)`);
              setTrafficTodayCachePayload(cacheKey, response);
              res.json(response);
              return;
            }
          } else {
            console.log('📊 No database data found (empty intervals), falling back to memory...');
          }
        } catch (dbError) {
          if (dbError?.code === 'MEMORY_PRESSURE_HARD_REJECT' || dbError?.code === 'HEAVY_READ_QUEUE_TIMEOUT') {
            console.warn(`⚠️ Skipping DB read for /api/traffic/today (${dbError.code}); falling back to memory.`);
          } else {
            console.error('❌ Database read error, falling back to memory:', dbError.message);
            console.error('❌ Full database read error:', dbError);
          }
        }
      }
      
      // Fallback to memory storage
      console.log(`📊 API Request: /api/traffic/today - ${trafficIntervals.length} intervals available in memory`);
      
      // If no data, collect some now
      if (trafficIntervals.length === 0) {
        await collectTrafficData();
      }

      const latestMemoryIntervalMs = getLatestIntervalTimestampMs(trafficIntervals);
      const memoryDataAgeMs = Number.isFinite(latestMemoryIntervalMs)
        ? Math.max(0, Date.now() - latestMemoryIntervalMs)
        : null;
      
      if (memoryDataAgeMs !== null && memoryDataAgeMs > TRAFFIC_DB_STALE_MAX_AGE_MS) {
        console.warn(`⚠️ Memory traffic data appears stale (${Math.round(memoryDataAgeMs / 1000)}s old).`);
      }
      
      const memorySegments = trafficIntervals.length > 0 ? segmentData : {};
      const memorySegmentCount = Object.keys(memorySegments).length;

      response = {
        intervals: trafficIntervals,
        segments: memorySegments,
        totalSegments: memorySegmentCount,
        currentIntervalIndex: trafficIntervals.length - 1,
        maxInterval: trafficIntervals.length - 1,
        coverage: memorySegmentCount > 0
          ? `North Vancouver comprehensive: ${memorySegmentCount} segments across ${NORTH_VAN_ROADS.length} major roads`
          : 'No live traffic data available',
        dataSource: trafficIntervals.length > 0 ? 'memory-ephemeral' : 'no-live-data',
        counterFlow: {
          status: counterFlowData.currentStatus,
          lanesOutbound: counterFlowData.lanesOutbound || 1,
          statusSince: counterFlowData.statusSince,
          lastChecked: counterFlowData.lastChecked,
          durationMs: counterFlowData.statusSince ? 
            Date.now() - new Date(counterFlowData.statusSince).getTime() : null
        },
        fromDatabase: false,
        serviceDays: requestedServiceDays,
        latestIntervalAgeMs: memoryDataAgeMs,
        freshness: buildTrafficFreshness(trafficIntervals, {
          latestSnapshotMs: latestMemoryIntervalMs
        })
      };
      
      setTrafficTodayCachePayload(cacheKey, response);
      res.json(response);
    } finally {
      if (inFlightGate && trafficTodayInFlight.get(cacheKey) === inFlightGate) {
        inFlightGate.resolve();
        trafficTodayInFlight.delete(cacheKey);
      }
    }
    
  } catch (error) {
    console.error('❌ Error in /api/traffic/today:', error);
    res.status(500).json({ 
      error: 'Failed to fetch traffic data',
      details: error.message 
    });
  }
});

app.get('/api/stats/daily', async (req, res) => {
  try {
    const rawDay = String(req.query.day || '').trim();
    const defaultDayKey = getTrafficServiceDayKey(new Date());
    const parsedDay = parseTrafficDayKey(rawDay || defaultDayKey);
    if (!parsedDay) {
      res.status(400).json({
        error: 'Missing or invalid day. Expected YYYY-MM-DD.'
      });
      return;
    }

    const dayKey = formatTrafficDayKey(parsedDay.year, parsedDay.month, parsedDay.day);
    const requestedLookbackWeeks = Number.parseInt(req.query.lookbackWeeks, 10);
    const lookbackWeeks = Number.isFinite(requestedLookbackWeeks)
      ? Math.max(1, Math.min(TRAFFIC_STATS_MAX_LOOKBACK_WEEKS, requestedLookbackWeeks))
      : TRAFFIC_STATS_DEFAULT_LOOKBACK_WEEKS;

    const requestedMaxSamples = Number.parseInt(req.query.maxSamples, 10);
    const maxSamples = Number.isFinite(requestedMaxSamples)
      ? Math.max(0, Math.min(12, requestedMaxSamples))
      : TRAFFIC_STATS_DEFAULT_MAX_SAME_WEEKDAY_SAMPLES;

    const refreshParam = String(req.query.refresh || '').toLowerCase();
    const bypassCache = refreshParam === '1' || refreshParam === 'true' || refreshParam === 'yes';
    const cacheKey = getTrafficStatsCacheKey(dayKey, lookbackWeeks, maxSamples);

    if (!bypassCache) {
      const cachedPayload = getTrafficStatsCachePayload(cacheKey);
      if (cachedPayload) {
        res.set('X-Traffic-Stats-Cache', 'HIT');
        res.json(cachedPayload);
        return;
      }
    }
    res.set('X-Traffic-Stats-Cache', 'MISS');

    let inFlightGate = null;
    if (!bypassCache) {
      const existingInFlight = trafficStatsInFlight.get(cacheKey);
      if (existingInFlight && existingInFlight.promise) {
        try {
          await existingInFlight.promise;
        } catch (_error) {
          // Ignore gate wait errors and continue.
        }
        const waitedCachePayload = getTrafficStatsCachePayload(cacheKey);
        if (waitedCachePayload) {
          res.set('X-Traffic-Stats-Cache', 'WAIT-HIT');
          res.json(waitedCachePayload);
          return;
        }
      }

      let resolveGate;
      const gatePromise = new Promise((resolve) => {
        resolveGate = resolve;
      });
      inFlightGate = { promise: gatePromise, resolve: resolveGate };
      trafficStatsInFlight.set(cacheKey, inFlightGate);
    }

    try {
      const responseBase = {
        dayKey,
        weekday: getWeekdayLabelFromTrafficDayKey(dayKey),
        lookbackWeeks,
        maxSameWeekdaySamples: maxSamples,
        generatedAt: new Date().toISOString()
      };

      let selectedSummary = buildDailyTrafficStatsSummary(dayKey, [], {});
      let baselineSummaries = [];
      let dataSource = 'memory-ephemeral';
      let fromDatabase = false;

      if (db) {
        try {
          const dbStatsPayload = await runTrafficHeavyRead(
            `/api/stats/daily:${dayKey}`,
            async () => {
              const targetDayOfWeek = getDayOfWeekFromTrafficDayKey(dayKey);
              const recentDateKeys = await db.getRecentDateKeys((lookbackWeeks * 7) + 28, dayKey, true);
              const baselineDayKeys = recentDateKeys
                .filter((candidateDayKey) => candidateDayKey < dayKey)
                .filter((candidateDayKey) => getDayOfWeekFromTrafficDayKey(candidateDayKey) === targetDayOfWeek)
                .slice(0, maxSamples);

              const dayKeysToLoad = [dayKey, ...baselineDayKeys];
              const dbDayData = await db.getTrafficDataForDateKeys(dayKeysToLoad);
              if (!dbDayData) {
                throw new Error('Failed to load day stats data from database');
              }

              const summariesByDayKey = {};
              dayKeysToLoad.forEach((candidateDayKey) => {
                const dayEntry = dbDayData?.days?.[candidateDayKey] || {};
                summariesByDayKey[candidateDayKey] = buildDailyTrafficStatsSummary(
                  candidateDayKey,
                  dayEntry.intervals || [],
                  dayEntry.segments || {}
                );
              });

              const selectedFromDb = summariesByDayKey[dayKey] || buildDailyTrafficStatsSummary(dayKey, [], {});
              const baselineFromDb = baselineDayKeys
                .map((candidateDayKey) => summariesByDayKey[candidateDayKey])
                .filter(Boolean);

              return {
                selectedSummary: selectedFromDb,
                baselineSummaries: baselineFromDb
              };
            }
          );

          selectedSummary = dbStatsPayload.selectedSummary;
          baselineSummaries = dbStatsPayload.baselineSummaries;
          dataSource = 'database';
          fromDatabase = true;
        } catch (dbStatsError) {
          console.warn(`⚠️ /api/stats/daily DB read failed, falling back to memory: ${dbStatsError.message}`);
        }
      }

      if (!fromDatabase) {
        const memoryIntervalsForDay = filterIntervalsByDayKey(trafficIntervals, dayKey);
        selectedSummary = buildDailyTrafficStatsSummary(dayKey, memoryIntervalsForDay, segmentData);
      }

      // If DB lacks current-day rows during startup, include fresh in-memory intervals.
      if (!selectedSummary.hasData && dayKey === getTrafficServiceDayKey(new Date())) {
        const memoryIntervalsForDay = filterIntervalsByDayKey(trafficIntervals, dayKey);
        if (memoryIntervalsForDay.length > 0) {
          selectedSummary = buildDailyTrafficStatsSummary(dayKey, memoryIntervalsForDay, segmentData);
          dataSource = fromDatabase ? 'hybrid-db-memory' : 'memory-ephemeral';
        }
      }

      const sameWeekdayComparison = buildSameWeekdayComparison(selectedSummary, baselineSummaries);
      const baselineDays = baselineSummaries.map((summary) => ({
        dayKey: summary.dayKey,
        weekday: summary.weekday,
        averageRatio: summary.averages?.overallRatio ?? null,
        rushHourPainMinutes: summary.rushHourPainMinutes?.overall?.total ?? null
      }));

      const response = {
        ...responseBase,
        dataSource,
        fromDatabase,
        summary: selectedSummary,
        sameWeekdayComparison,
        baselineDays
      };

      setTrafficStatsCachePayload(cacheKey, response);
      res.json(response);
    } finally {
      if (inFlightGate && trafficStatsInFlight.get(cacheKey) === inFlightGate) {
        inFlightGate.resolve();
        trafficStatsInFlight.delete(cacheKey);
      }
    }
  } catch (error) {
    if (error?.code === 'MEMORY_PRESSURE_HARD_REJECT' || error?.code === 'HEAVY_READ_QUEUE_TIMEOUT') {
      res.set('Retry-After', '5');
      res.status(503).json({
        error: 'Traffic stats are temporarily unavailable; please retry in a few seconds.',
        code: error.code
      });
      return;
    }
    console.error('❌ Error in /api/stats/daily:', error);
    res.status(500).json({
      error: 'Failed to build daily stats',
      details: error.message
    });
  }
});

// Debug endpoint: all North Van routes (pink) + tracked routes (blue)
app.get('/api/debug/routes', async (req, res) => {
  try {
    const refreshParam = String(req.query.refresh || '').toLowerCase();
    const forceRefresh = refreshParam === '1' || refreshParam === 'true' || refreshParam === 'yes';

    const existingCacheAgeMs = debugRouteSnapshot.fetchedAt
      ? Date.now() - new Date(debugRouteSnapshot.fetchedAt).getTime()
      : Number.POSITIVE_INFINITY;
    const cacheHasData = Object.keys(debugRouteSnapshot.allSegments || {}).length > 0;
    const shouldRefresh = forceRefresh || !cacheHasData || existingCacheAgeMs > DEBUG_ROUTE_CACHE_TTL_MS;

    if (shouldRefresh) {
      const refreshedAt = new Date().toISOString();
      const refreshResult = await fetchHereTrafficData();
      updateDebugRouteSnapshotFromFetch(refreshResult, refreshedAt);

      // If HERE is unavailable, still provide a usable overlay from current tracked geometry.
      if (Object.keys(debugRouteSnapshot.allSegments || {}).length === 0) {
        const fallbackAllSegments = buildFallbackAllSegmentsFromTracked(segmentData);
        debugRouteSnapshot = {
          fetchedAt: refreshedAt,
          dataSource: 'tracked-fallback',
          allSegments: fallbackAllSegments,
          trackedSourceIds: Object.keys(fallbackAllSegments),
          rawSegmentCount: Object.keys(fallbackAllSegments).length,
          filteredSegmentCount: Object.keys(segmentData).length,
          selectedSegmentCount: Object.keys(segmentData).length,
          filterMode: 'tracked-fallback',
          manualTrackedConfiguredCount: MANUAL_TRACKED_SOURCE_IDS.size,
          manualTrackedMatchedCount: 0
        };
      }
    }

    const trackedSegments = buildTrackedDebugSegments();
    const fallbackAllSegments = buildFallbackAllSegmentsFromTracked(segmentData);
    const baseAllSegments = Object.keys(debugRouteSnapshot.allSegments || {}).length > 0
      ? debugRouteSnapshot.allSegments
      : fallbackAllSegments;

    const trackedSourceIds = new Set(debugRouteSnapshot.trackedSourceIds || []);
    Object.values(trackedSegments).forEach((segment) => {
      if (typeof segment?.sourceId === 'string' && segment.sourceId) {
        trackedSourceIds.add(segment.sourceId);
      }
    });

    const allSegments = {};
    let overlapSegments = 0;
    Object.entries(baseAllSegments).forEach(([sourceId, segment]) => {
      const tracked = trackedSourceIds.has(sourceId);
      if (tracked) overlapSegments += 1;
      allSegments[sourceId] = {
        ...segment,
        tracked
      };
    });

    const cacheAgeMs = debugRouteSnapshot.fetchedAt
      ? Math.max(0, Date.now() - new Date(debugRouteSnapshot.fetchedAt).getTime())
      : null;

    res.json({
      timestamp: new Date().toISOString(),
      fetchedAt: debugRouteSnapshot.fetchedAt,
      dataSource: debugRouteSnapshot.dataSource,
      bbox: NORTH_VAN_BBOX,
      summary: {
        allSegments: Object.keys(allSegments).length,
        trackedSegments: Object.keys(trackedSegments).length,
        overlapSegments,
        rawSegmentCount: debugRouteSnapshot.rawSegmentCount,
        filteredSegmentCount: debugRouteSnapshot.filteredSegmentCount,
        selectedSegmentCount: debugRouteSnapshot.selectedSegmentCount,
        filterMode: debugRouteSnapshot.filterMode,
        manualTrackedConfiguredCount: debugRouteSnapshot.manualTrackedConfiguredCount,
        manualTrackedMatchedCount: debugRouteSnapshot.manualTrackedMatchedCount,
        cacheAgeMs
      },
      allSegments,
      trackedSegments
    });
  } catch (error) {
    console.error('❌ Error in /api/debug/routes:', error);
    res.status(500).json({
      error: 'Failed to fetch debug route data',
      details: error.message
    });
  }
});

// Database statistics endpoint
app.get('/api/database/stats', async (req, res) => {
  try {
    if (db) {
      const stats = await db.getStats();
      res.json({ 
        database: true, 
        stats,
        message: 'Database operational'
      });
    } else {
      res.json({ 
        database: false, 
        message: 'Using memory storage - no DATABASE_URL configured'
      });
    }
  } catch (error) {
    res.status(500).json({ 
      database: false,
      error: error.message 
    });
  }
});

// Counterflow-specific endpoint
app.get('/api/counterflow/status', async (req, res) => {
  try {
    const now = Date.now();
    const parsedLanesOutbound = Number(counterFlowData.lanesOutbound);
    const hasLanesOutbound = Number.isFinite(parsedLanesOutbound);
    const hasCounterflowStatus =
      hasLanesOutbound ||
      (typeof counterFlowData.currentStatus === 'string' && counterFlowData.currentStatus.length > 0);
    const isActive = hasLanesOutbound
      ? parsedLanesOutbound >= 2
      : (hasCounterflowStatus ? counterFlowData.currentStatus === 'outbound-2' : null);
    const stateStartTime = counterFlowData.statusSince
      ? new Date(counterFlowData.statusSince).getTime()
      : null;
    const response = {
      hasData: hasCounterflowStatus,
      isActive,
      status: counterFlowData.currentStatus,
      lanesOutbound: hasCounterflowStatus
        ? (hasLanesOutbound ? parsedLanesOutbound : null)
        : null,
      stateStartTime,
      currentDuration: stateStartTime ? now - stateStartTime : null,
      lastChecked: counterFlowData.lastChecked ? new Date(counterFlowData.lastChecked).getTime() : null,
      lastUpdated: counterFlowData.lastChecked ? new Date(counterFlowData.lastChecked).getTime() : null,
      isHealthy: !!(
        counterFlowData.lastChecked &&
        (now - new Date(counterFlowData.lastChecked).getTime()) < 5 * 60 * 1000
      ), // healthy if checked within 5 minutes
      rawStatus: counterFlowData.currentStatus,
      lastError: counterFlowData.lastError || null
    };

    if (req.query.debug === '1') {
      response.sourceUrl = counterFlowData.sourceUrl || BC_ATIS_SOURCE_URLS[0];
      response.sourceCandidates = BC_ATIS_SOURCE_URLS;
      response.parserDebug = counterFlowData.rawData || null;
    }
    
    res.json(response);
  } catch (error) {
    res.status(500).json({ 
      error: 'Failed to get counterflow status',
      details: error.message 
    });
  }
});

// Counterflow history endpoint
app.get('/api/counterflow/history', async (req, res) => {
  try {
    const limit = parseInt(req.query.limit) || 50;
    res.json({
      history: counterFlowData.history.slice(-limit),
      totalChanges: counterFlowData.history.length,
      currentStatus: counterFlowData.currentStatus
    });
  } catch (error) {
    res.status(500).json({ 
      error: 'Failed to get counterflow history',
      details: error.message 
    });
  }
});

// Initialize and start
const startServer = async () => {
  console.log('🚀 Starting North Vancouver Traffic Server (HERE API)...');
  console.log(
    `⏱️ Traffic cadence config: peak=${TRAFFIC_COLLECTION_PEAK_INTERVAL_MINUTES}m, ` +
    `off-peak=${TRAFFIC_COLLECTION_OFFPEAK_INTERVAL_MINUTES}m, ` +
    `window=${TRAFFIC_COLLECTION_OFFPEAK_START_HOUR}:00-${TRAFFIC_COLLECTION_OFFPEAK_END_HOUR}:00 ` +
    `${TRAFFIC_COLLECTION_TIME_ZONE}`
  );

  const httpServer = app.listen(PORT, () => {
    console.log(`🌐 Server running on port ${PORT}`);
    console.log(`📍 Monitoring ${NORTH_VAN_ROADS.length} major roads with ${Object.keys(segmentData).length} segments`);
    console.log(`🔑 HERE API: ${HERE_API_KEY !== 'YOUR_HERE_API_KEY_NEEDED' ? 'Configured' : 'Not configured'}`);
    console.log(
      `🛡️ Read guard: concurrency=${TRAFFIC_HEAVY_READ_MAX_CONCURRENCY}, ` +
      `waitTimeoutMs=${TRAFFIC_HEAVY_READ_WAIT_TIMEOUT_MS}, heapSoft=${TRAFFIC_HEAP_SOFT_LIMIT_MB}MB, ` +
      `heapHard=${TRAFFIC_HEAP_HARD_REJECT_MB}MB, cacheToday=${TRAFFIC_TODAY_CACHE_MAX_KEYS}, ` +
      `cacheWindow=${TRAFFIC_DAY_WINDOW_CACHE_MAX_KEYS}`
    );
    console.log(
      `🛰️ Freshness guard: staleThreshold=${TRAFFIC_STALE_THRESHOLD_MINUTES}m, ` +
      `watchdogAlertThreshold=${TRAFFIC_WATCHDOG_STALE_MINUTES}m`
    );
  });

  httpServer.on('error', (error) => {
    console.error('❌ HTTP server error:', error);
  });

  // Start background collectors after server is listening so Railway can pass health checks quickly.
  isCollecting = true;
  runTrafficCollectionCycle().catch((error) => {
    console.error('❌ Initial traffic collection failed:', error);
    if (isCollecting) {
      scheduleNextTrafficCollection(15 * 1000);
    }
  });

  // Lightweight memory heartbeat for crash triage on Railway.
  setInterval(() => {
    const usage = process.memoryUsage();
    const toMb = (bytes) => Math.round((bytes / (1024 * 1024)) * 10) / 10;
    console.log(
      `🧠 Memory rss=${toMb(usage.rss)}MB heapUsed=${toMb(usage.heapUsed)}MB ` +
      `heapTotal=${toMb(usage.heapTotal)}MB ext=${toMb(usage.external)}MB`
    );
  }, 5 * 60 * 1000);

  // Minute-level self-check: logs health and emits alert when snapshot freshness is too old.
  logTrafficCollectionWatchdog();
  setInterval(logTrafficCollectionWatchdog, TRAFFIC_WATCHDOG_INTERVAL_MS);

  // Prime counter-flow state from latest persisted snapshot when available.
  bootstrapCounterFlowFromDatabase()
    .catch((error) => {
      console.warn(`⚠️ Counter-flow DB bootstrap failed: ${error.message}`);
    })
    .finally(() => {
      updateCounterFlowData().catch((error) => {
        console.warn(`⚠️ Initial counter-flow update failed: ${error.message}`);
      });
    });

  // Collect counter-flow data every 60 seconds.
  setInterval(() => {
    updateCounterFlowData().catch((error) => {
      console.warn(`⚠️ Counter-flow update failed: ${error.message}`);
    });
  }, 60 * 1000);
};

startServer().catch(console.error);
