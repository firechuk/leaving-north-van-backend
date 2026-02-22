require('dotenv').config();
const express = require('express');
const axios = require('axios');
const cors = require('cors');
const crypto = require('crypto');
// const cheerio = require('cheerio'); // Removed for Node compatibility
const TrafficDatabase = require('./database');

const app = express();
const PORT = process.env.PORT || 3002;

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
const TRAFFIC_TODAY_CACHE_MAX_KEYS = 8;
const TRAFFIC_TODAY_DEFAULT_SERVICE_DAYS = 2;
const TRAFFIC_TODAY_MAX_SERVICE_DAYS = 21;
const TRAFFIC_TODAY_ABSOLUTE_SAFE_MAX_SERVICE_DAYS = 3;
const TRAFFIC_TODAY_RUNTIME_SAFE_MAX_SERVICE_DAYS = (() => {
  const parsed = Number.parseInt(process.env.TRAFFIC_TODAY_RUNTIME_SAFE_MAX_SERVICE_DAYS || '2', 10);
  if (!Number.isFinite(parsed)) return 2;
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
const MIN_FILTERED_SEGMENTS_FOR_TRACKED = 12;
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

const getTrafficTodayCacheKey = (serviceDays) => `serviceDays:${serviceDays}`;

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

const getLatestIntervalTimestampMs = (intervals) => {
  if (!Array.isArray(intervals) || intervals.length === 0) return null;

  for (let i = intervals.length - 1; i >= 0; i -= 1) {
    const parsedMs = parseIntervalTimestampMs(intervals[i]);
    if (Number.isFinite(parsedMs)) return parsedMs;
  }

  return null;
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
    
    // Keep last 24 hours (720 2-minute intervals)
    if (trafficIntervals.length > 720) {
      trafficIntervals = trafficIntervals.slice(-720);
    }
    invalidateTrafficTodayCache();
    
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
  }
};

// API endpoints
app.get('/health', (req, res) => {
  res.json({
    status: 'healthy',
    timestamp: new Date().toISOString(),
    segments: Object.keys(segmentData).length,
    intervals: trafficIntervals.length,
    collecting: isCollecting
  });
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
          const dbData = await db.getTodayTrafficData(requestedServiceDays);
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
                memoryLatestIntervalAgeMs: memoryDataAgeMs
              };
              console.log(`✅ Served hybrid dataset: ${mergedIntervals.length} merged intervals (DB stale, memory tail appended)`);
              setTrafficTodayCachePayload(cacheKey, response);
              res.json(response);
              return;
            } else {
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
                memoryLatestIntervalAgeMs: memoryDataAgeMs
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
          console.error('❌ Database read error, falling back to memory:', dbError.message);
          console.error('❌ Full database read error:', dbError);
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
        latestIntervalAgeMs: memoryDataAgeMs
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
  
  // Start data collection
  isCollecting = true;
  await collectTrafficData();
  
  // Collect every 2 minutes
  setInterval(collectTrafficData, 2 * 60 * 1000);

  // Lightweight memory heartbeat for crash triage on Railway.
  setInterval(() => {
    const usage = process.memoryUsage();
    const toMb = (bytes) => Math.round((bytes / (1024 * 1024)) * 10) / 10;
    console.log(
      `🧠 Memory rss=${toMb(usage.rss)}MB heapUsed=${toMb(usage.heapUsed)}MB ` +
      `heapTotal=${toMb(usage.heapTotal)}MB ext=${toMb(usage.external)}MB`
    );
  }, 5 * 60 * 1000);
  
  // Prime counter-flow state from latest persisted snapshot when available.
  await bootstrapCounterFlowFromDatabase();

  // Collect counter-flow data every 60 seconds
  await updateCounterFlowData(); // Initial collection
  setInterval(updateCounterFlowData, 60 * 1000);
  
  app.listen(PORT, () => {
    console.log(`🌐 Server running on port ${PORT}`);
    console.log(`📍 Monitoring ${NORTH_VAN_ROADS.length} major roads with ${Object.keys(segmentData).length} segments`);
    console.log(`🔑 HERE API: ${HERE_API_KEY !== 'YOUR_HERE_API_KEY_NEEDED' ? 'Configured' : 'Not configured'}`);
  });
};

startServer().catch(console.error);
