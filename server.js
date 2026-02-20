require('dotenv').config();
const express = require('express');
const axios = require('axios');
const cors = require('cors');
const fs = require('fs');
const path = require('path');
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
const TRAFFIC_DB_STALE_MAX_AGE_MS = 12 * 60 * 1000;
const MIN_FILTERED_SEGMENTS_FOR_TRACKED = 12;

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
  history: [] // array of status changes for pattern analysis
};

// Debug route overlay cache (all routes + tracked overlay)
let debugRouteSnapshot = {
  fetchedAt: null,
  dataSource: 'uninitialized',
  allSegments: {},
  trackedSourceIds: [],
  rawSegmentCount: 0,
  filteredSegmentCount: 0
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

  return {
    sourceId,
    name: description || `Traffic Segment ${index + 1}`,
    description: description || null,
    coordinates,
    type: 'road',
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
    filteredSegmentCount: fetchResult?.debugMeta?.filteredSegmentCount || 0
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

// Generate segments from road bboxes
const initializeSegments = () => {
  console.log('Initializing North Vancouver road segments...');
  
  NORTH_VAN_ROADS.forEach((road, roadIndex) => {
    const [minLat, minLng, maxLat, maxLng] = road.bbox.split(',').map(Number);
    
    // OPTIMIZED: Reduced segment counts to solve database crisis
    // Old: high=8, medium=4, low=2 (282 total segments)
    // New: high=3, medium=3, low=2 (~45 total segments)
    const segmentCount = road.priority === 'high' ? 3 : road.priority === 'medium' ? 3 : 2;
    
    for (let i = 0; i < segmentCount; i++) {
      const segmentId = `here-${roadIndex}-${i}`;
      
      // Distribute segments along the road bbox
      const latStep = (maxLat - minLat) / segmentCount;
      const lngStep = (maxLng - minLng) / segmentCount;
      
      const segmentMinLat = minLat + (latStep * i);
      const segmentMaxLat = minLat + (latStep * (i + 1));
      const segmentMinLng = minLng + (lngStep * i);
      const segmentMaxLng = minLng + (lngStep * (i + 1));
      
      // Create simple line segment coordinates
      const coordinates = [
        [segmentMinLng, segmentMinLat],
        [segmentMaxLng, segmentMaxLat]
      ];
      
      segmentData[segmentId] = {
        name: `${road.name} (${i + 1})`,
        coordinates: coordinates,
        type: road.type,
        priority: road.priority,
        roadIndex: roadIndex,
        segmentIndex: i
      };
    }
  });
  
  console.log(`✅ Initialized ${Object.keys(segmentData).length} segments across ${NORTH_VAN_ROADS.length} roads`);
};

// Fetch traffic data from HERE API using efficient bounding box approach
const fetchHereTrafficData = async () => {
  const buildSyntheticFallback = (dataSourceLabel = 'synthetic-fallback') => {
    const syntheticData = generateSyntheticTrafficData();
    const allSegmentMetadata = buildFallbackAllSegmentsFromTracked(segmentData);
    const trackedSourceIds = Object.entries(segmentData)
      .map(([segmentId, segment]) => {
        if (typeof segment?.sourceId === 'string' && segment.sourceId) {
          return segment.sourceId;
        }
        return `tracked-${segmentId}`;
      });

    return {
      trafficData: syntheticData,
      segmentMetadata: {},
      allSegmentMetadata,
      debugMeta: {
        dataSource: dataSourceLabel,
        rawSegmentCount: Object.keys(allSegmentMetadata).length,
        allSegmentCount: Object.keys(allSegmentMetadata).length,
        filteredSegmentCount: syntheticData.length,
        trackedSourceIds
      }
    };
  };

  if (!HERE_API_KEY || HERE_API_KEY === 'YOUR_HERE_API_KEY_NEEDED') {
    console.log('⚠️ HERE API key not configured, using synthetic data');
    return buildSyntheticFallback('synthetic-no-here-key');
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
      console.log('⚠️ No traffic data in HERE response, using synthetic data');
      return buildSyntheticFallback('synthetic-empty-here-response');
    }

    // Build "all segments" payload for debug overlay (pink layer).
    const allSegmentMetadata = {};
    rawSegments.forEach((segment, index) => {
      const record = buildHereSegmentRecord(segment, index);
      if (!record) return;

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
    
    // Filter segments to only critical roads first.
    const filteredSegments = rawSegments.filter(isSegmentInCriticalRoads);
    let selectedSegments = filteredSegments;
    let filterMode = 'critical-only';
    if (filteredSegments.length < MIN_FILTERED_SEGMENTS_FOR_TRACKED) {
      filterMode = 'raw-fallback';
      selectedSegments = rawSegments;
      console.warn(
        `⚠️ Filtered segment count too low (${filteredSegments.length}). Falling back to all ${rawSegments.length} HERE segments.`
      );
    }
    console.log(`🎯 Filtered ${rawSegments.length} segments to ${filteredSegments.length} critical road segments (serving ${selectedSegments.length}, mode=${filterMode})`);
    
    // DEBUG: Log a few kept/rejected segments for troubleshooting.
    const kept = selectedSegments;
    const rejected = filterMode === 'raw-fallback'
      ? []
      : rawSegments.filter(seg => !isSegmentInCriticalRoads(seg));
    
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
        : (isSegmentInCriticalRoads(seg) ? 'KEPT' : 'REJECTED');
      console.log(`  ${i + 1}. ${inTrackedSet}: ${seg.location?.description} at ${coords?.lat?.toFixed?.(6)},${coords?.lng?.toFixed?.(6)}`);
    });
    
    // Convert selected HERE traffic segments to tracked payload.
    const trafficData = [];
    const segmentMetadata = {};
    
    selectedSegments.forEach((segment, index) => {
      const record = buildHereSegmentRecord(segment, index);
      if (!record) return;

      const currentSpeed = record.currentSpeed !== null ? record.currentSpeed : 50;
      const freeFlowSpeed = record.freeFlowSpeed !== null ? record.freeFlowSpeed : currentSpeed || 50;
      const flowRatio = freeFlowSpeed > 0 ? Math.min(1.0, currentSpeed / freeFlowSpeed) : 1.0;
      const segmentId = `here-0-${index}`;
      
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
          name: record.name,
          description: record.description,
          coordinates: record.coordinates,
          type: record.type,
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
        filterMode,
        trackedSourceIds
      }
    };
    
  } catch (error) {
    console.log('❌ HERE API failed, falling back to synthetic data:', error.response?.status, error.message);
    return buildSyntheticFallback('synthetic-here-error');
  }
};

// Generate realistic synthetic traffic data
const generateSyntheticTrafficData = () => {
  const now = new Date();
  const hour = now.getHours();
  const dayOfWeek = now.getDay(); // 0 = Sunday
  
  console.log(`🤖 Generating synthetic traffic data for ${hour}:00 on day ${dayOfWeek}`);
  
  const trafficData = [];
  
  Object.keys(segmentData).forEach(segmentId => {
    const segment = segmentData[segmentId];
    const ratio = generateTimeBasedTrafficRatio(segment.type, segment.priority, hour, dayOfWeek);
    
    trafficData.push({
      segmentId: segmentId,
      ratio: ratio
    });
  });
  
  return trafficData;
};

// Generate time-based traffic ratios
const generateTimeBasedTrafficRatio = (roadType, priority = 'medium', hour = null, dayOfWeek = null) => {
  if (hour === null) hour = new Date().getHours();
  if (dayOfWeek === null) dayOfWeek = new Date().getDay();
  
  let baseRatio = 1.0; // Free flow
  
  // Rush hour patterns
  const isWeekday = dayOfWeek >= 1 && dayOfWeek <= 5;
  const isMorningRush = hour >= 7 && hour <= 9;
  const isEveningRush = hour >= 17 && hour <= 19;
  const isWeekend = dayOfWeek === 0 || dayOfWeek === 6;
  
  if (isWeekday) {
    if (isMorningRush || isEveningRush) {
      // Heavy congestion during rush hours
      if (roadType === 'bridge') baseRatio = 0.3 + Math.random() * 0.2; // 30-50% of free flow
      else if (roadType === 'highway') baseRatio = 0.4 + Math.random() * 0.2; // 40-60%
      else if (roadType === 'arterial') baseRatio = 0.5 + Math.random() * 0.3; // 50-80%
      else baseRatio = 0.7 + Math.random() * 0.2; // 70-90%
    } else {
      // Light to moderate congestion off-peak
      if (roadType === 'bridge') baseRatio = 0.6 + Math.random() * 0.3; // 60-90%
      else baseRatio = 0.8 + Math.random() * 0.2; // 80-100%
    }
  } else if (isWeekend) {
    // Weekend patterns
    if (hour >= 10 && hour <= 16) {
      // Weekend afternoon activity
      baseRatio = 0.7 + Math.random() * 0.2; // 70-90%
    } else if (hour >= 17 && hour <= 21) {
      // Sunday evening return traffic - heavy congestion
      if (roadType === 'bridge') baseRatio = 0.2 + Math.random() * 0.2; // 20-40% (very heavy)
      else if (roadType === 'highway') baseRatio = 0.3 + Math.random() * 0.2; // 30-50% (heavy)
      else if (roadType === 'arterial') baseRatio = 0.4 + Math.random() * 0.3; // 40-70% (moderate-heavy)
      else baseRatio = 0.6 + Math.random() * 0.2; // 60-80% (moderate)
    } else {
      baseRatio = 0.85 + Math.random() * 0.15; // 85-100%
    }
  }
  
  // Priority adjustments
  if (priority === 'high') baseRatio *= 0.9; // High priority roads more congested
  if (priority === 'low') baseRatio = Math.min(1.0, baseRatio * 1.1); // Low priority less congested
  
  return Math.max(0.1, Math.min(1.0, baseRatio));
};

// Lions Gate Bridge Counter-Flow Data Collection
const BC_ATIS_URL = 'http://www.th.gov.bc.ca/ATIS/lgcws/private_status.htm';

const scrapeCounterFlowData = async () => {
  try {
    console.log('🌉 Scraping Lions Gate counter-flow data...');
    
    const response = await axios.get(BC_ATIS_URL, {
      timeout: 10000,
      headers: {
        'User-Agent': 'Mozilla/5.0 (compatible; TrafficMonitor/1.0)'
      }
    });
    
    // Parse HTML for VDS ID: 201 data
    const html = response.data;
    const vds201Match = html.match(/VDS ID:\s*201[\s\S]*?(?=VDS ID:|$)/i);
    
    if (!vds201Match) {
      console.log('⚠️ Could not find VDS ID: 201 in response');
      return null;
    }
    
    const vds201Section = vds201Match[0];
    
    // Parse lane statuses
    const lane1Closed = /Lane 1[\s\S]*?COUNTER FLOW LANE IS CLOSED/i.test(vds201Section);
    const lane2Closed = /Lane 2[\s\S]*?COUNTER FLOW LANE IS CLOSED/i.test(vds201Section);
    
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
    
    console.log(`✅ Lions Gate status: ${status} (${lanesOutbound} lanes outbound)`);
    
    return {
      status,
      lanesOutbound,
      timestamp,
      rawData: {
        lane1Closed,
        lane2Closed,
        vds201Section: vds201Section.substring(0, 500) // First 500 chars for debugging
      }
    };
    
  } catch (error) {
    console.log('❌ Counter-flow scraping failed:', error.message);
    return null;
  }
};

const updateCounterFlowData = async () => {
  const newData = await scrapeCounterFlowData();
  
  if (!newData) return; // Skip update if scraping failed
  
  const previousStatus = counterFlowData.currentStatus;
  const statusChanged = previousStatus !== newData.status;
  
  if (statusChanged) {
    // Log status change
    console.log(`🔄 Counter-flow changed: ${previousStatus || 'unknown'} → ${newData.status}`);
    
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
    
    // Save to database if available
    if (db) {
      try {
        // Clean segmentMetadata for database storage (remove complex HERE API objects)
        const cleanSegmentData = {};
        Object.keys(segmentMetadata).forEach((segmentId) => {
          const segment = segmentMetadata[segmentId];
          cleanSegmentData[segmentId] = {
            name: segment.name,
            coordinates: segment.coordinates,
            type: segment.type
            // Skip originalData - contains circular references from HERE API
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

// Emergency database clearing endpoint (one-time use)
app.post('/api/admin/clear-database', async (req, res) => {
  try {
    if (!db) {
      return res.status(400).json({
        error: 'Database not configured',
        message: 'No DATABASE_URL environment variable found'
      });
    }
    
    console.log('🚨 ADMIN ACTION: Database clearing requested');
    const result = await db.clearAllTrafficData();
    
    if (result.success) {
      res.json({
        success: true,
        message: `Successfully cleared ${result.deletedRows} traffic snapshots`,
        freedSpace: 'Database space freed up for optimized collection',
        timestamp: new Date().toISOString()
      });
    } else {
      res.status(500).json({
        success: false,
        error: result.error,
        message: 'Failed to clear database'
      });
    }
    
  } catch (error) {
    console.error('❌ Database clearing failed:', error);
    res.status(500).json({
      success: false,
      error: error.message,
      message: 'Database clearing operation failed'
    });
  }
});

app.get('/api/traffic/today', async (req, res) => {
  try {
    let response;
    
    // Try database first if available
    if (db) {
      console.log(`📊 API Request: /api/traffic/today - checking database...`);
      try {
        const dbData = await db.getTodayTrafficData();
        console.log(`🔍 DB data received: intervals=${dbData?.intervals?.length || 0}, segments=${Object.keys(dbData?.segments || {}).length}`);
        
        if (dbData && dbData.intervals.length > 0) {
          const latestDbIntervalMs = getLatestIntervalTimestampMs(dbData.intervals);
          const dbDataAgeMs = Number.isFinite(latestDbIntervalMs)
            ? Math.max(0, Date.now() - latestDbIntervalMs)
            : Number.POSITIVE_INFINITY;
          const isDbFreshEnough = dbDataAgeMs <= TRAFFIC_DB_STALE_MAX_AGE_MS;

          if (!isDbFreshEnough) {
            console.warn(
              `⚠️ DB data is stale (age ${Math.round(dbDataAgeMs / 1000)}s, threshold ${Math.round(TRAFFIC_DB_STALE_MAX_AGE_MS / 1000)}s). Falling back to memory.`
            );
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
              dbRecordCount: dbData.recordCount,
              latestIntervalAgeMs: Number.isFinite(dbDataAgeMs) ? dbDataAgeMs : null
            };
            console.log(`✅ Successfully served ${dbData.intervals.length} intervals from database (expanded coverage)`);
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
    
    response = {
      intervals: trafficIntervals,
      segments: segmentData,
      totalSegments: Object.keys(segmentData).length,
      currentIntervalIndex: trafficIntervals.length - 1,
      maxInterval: trafficIntervals.length - 1,
      coverage: `North Vancouver comprehensive: ${Object.keys(segmentData).length} segments across ${NORTH_VAN_ROADS.length} major roads`,
      dataSource: 'memory-ephemeral',
      counterFlow: {
        status: counterFlowData.currentStatus,
        lanesOutbound: counterFlowData.lanesOutbound || 1,
        statusSince: counterFlowData.statusSince,
        lastChecked: counterFlowData.lastChecked,
        durationMs: counterFlowData.statusSince ? 
          Date.now() - new Date(counterFlowData.statusSince).getTime() : null
      },
      fromDatabase: false,
      latestIntervalAgeMs: memoryDataAgeMs
    };
    
    res.json(response);
    
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
          filteredSegmentCount: Object.keys(segmentData).length
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
    const response = {
      isActive: counterFlowData.currentStatus === 'outbound-2', // 2 lanes outbound = counterflow
      status: counterFlowData.currentStatus,
      lanesOutbound: counterFlowData.lanesOutbound || 1,
      stateStartTime: counterFlowData.statusSince ? new Date(counterFlowData.statusSince).getTime() : now,
      currentDuration: counterFlowData.statusSince ? now - new Date(counterFlowData.statusSince).getTime() : 0,
      lastChecked: counterFlowData.lastChecked ? new Date(counterFlowData.lastChecked).getTime() : null,
      lastUpdated: counterFlowData.lastChecked ? new Date(counterFlowData.lastChecked).getTime() : null,
      isHealthy: counterFlowData.lastChecked && (now - new Date(counterFlowData.lastChecked).getTime()) < 5 * 60 * 1000, // healthy if checked within 5 minutes
      rawStatus: counterFlowData.currentStatus
    };
    
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
  
  // Initialize road segments
  initializeSegments();
  
  // Start data collection
  isCollecting = true;
  await collectTrafficData();
  
  // Collect every 2 minutes
  setInterval(collectTrafficData, 2 * 60 * 1000);
  
  // Collect counter-flow data every 60 seconds
  await updateCounterFlowData(); // Initial collection
  setInterval(updateCounterFlowData, 60 * 1000);
  
  app.listen(PORT, () => {
    console.log(`🌐 Server running on port ${PORT}`);
    console.log(`📍 Monitoring ${NORTH_VAN_ROADS.length} major roads with ${Object.keys(segmentData).length} segments`);
    console.log(`🔑 HERE API: ${HERE_API_KEY !== 'YOUR_HERE_API_KEY_NEEDED' ? 'Configured' : 'Using synthetic data'}`);
  });
};

startServer().catch(console.error);
