require('dotenv').config();
const express = require('express');
const axios = require('axios');
const cors = require('cors');
const fs = require('fs');
const path = require('path');

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

// Comprehensive North Vancouver road network
const NORTH_VAN_ROADS = [
  // Major Bridges
  { name: 'Lions Gate Bridge', bbox: '49.314,-123.140,49.316,-123.136', type: 'bridge', priority: 'high' },
  { name: 'Ironworkers Memorial Bridge', bbox: '49.292,-123.025,49.295,-123.021', type: 'bridge', priority: 'high' },
  { name: 'Second Narrows Bridge', bbox: '49.293,-123.024,49.296,-123.020', type: 'bridge', priority: 'high' },
  
  // Highways & Major Routes
  { name: 'Trans-Canada Highway (Hwy 1)', bbox: '49.328,-123.090,49.332,-123.020', type: 'highway', priority: 'high' },
  { name: 'Highway 99 (Sea to Sky)', bbox: '49.314,-123.140,49.320,-123.120', type: 'highway', priority: 'high' },
  { name: 'Upper Levels Highway', bbox: '49.325,-123.130,49.330,-123.020', type: 'highway', priority: 'high' },
  
  // Major Arterials (North-South)
  { name: 'Lonsdale Avenue', bbox: '49.310,-123.075,49.340,-123.072', type: 'arterial', priority: 'high' },
  { name: 'Capilano Road', bbox: '49.314,-123.116,49.340,-123.112', type: 'arterial', priority: 'medium' },
  { name: 'Lynn Valley Road', bbox: '49.320,-123.037,49.350,-123.033', type: 'arterial', priority: 'medium' },
  { name: 'Mountain Highway', bbox: '49.325,-123.052,49.350,-123.048', type: 'arterial', priority: 'medium' },
  { name: 'Westview Drive', bbox: '49.330,-123.090,49.350,-123.086', type: 'arterial', priority: 'low' },
  
  // Major Arterials (East-West)
  { name: 'Marine Drive', bbox: '49.324,-123.130,49.326,-123.020', type: 'arterial', priority: 'high' },
  { name: 'Keith Road', bbox: '49.310,-123.080,49.314,-123.020', type: 'arterial', priority: 'medium' },
  { name: '3rd Street', bbox: '49.315,-123.080,49.318,-123.020', type: 'arterial', priority: 'medium' },
  { name: '13th Street', bbox: '49.320,-123.080,49.322,-123.020', type: 'arterial', priority: 'medium' },
  { name: '29th Street', bbox: '49.330,-123.080,49.332,-123.020', type: 'arterial', priority: 'low' },
  
  // Collectors & Local Roads
  { name: 'Dollarton Highway', bbox: '49.305,-123.080,49.315,-123.020', type: 'collector', priority: 'low' },
  { name: 'Deep Cove Road', bbox: '49.320,-123.040,49.330,-123.020', type: 'collector', priority: 'low' },
  { name: 'Brooksbank Avenue', bbox: '49.315,-123.080,49.325,-123.076', type: 'collector', priority: 'low' },
  { name: 'Pemberton Avenue', bbox: '49.320,-123.100,49.340,-123.096', type: 'collector', priority: 'low' }
];

// Data storage
let trafficIntervals = [];
let segmentData = {};
let isCollecting = false;

// Counter-flow data storage
let counterFlowData = {
  currentStatus: null, // 'outbound-1', 'outbound-2', etc.
  statusSince: null, // timestamp when current status started
  lastChecked: null, // last scrape timestamp
  history: [] // array of status changes for pattern analysis
};

// Generate segments from road bboxes
const initializeSegments = () => {
  console.log('Initializing North Vancouver road segments...');
  
  NORTH_VAN_ROADS.forEach((road, roadIndex) => {
    const [minLat, minLng, maxLat, maxLng] = road.bbox.split(',').map(Number);
    
    // Create multiple segments per road for granular visualization
    const segmentCount = road.priority === 'high' ? 8 : road.priority === 'medium' ? 4 : 2;
    
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
  if (!HERE_API_KEY || HERE_API_KEY === 'YOUR_HERE_API_KEY_NEEDED') {
    console.log('⚠️ HERE API key not configured, using synthetic data');
    const syntheticData = generateSyntheticTrafficData();
    return { trafficData: syntheticData, segmentMetadata: {} };
  }
  
  try {
    console.log('Fetching traffic data from HERE API (single bounding box)...');
    
    // Single bounding box for all of North Vancouver (Gemini's approach)
    // Format: West Longitude, South Latitude, East Longitude, North Latitude  
    const northVanBBox = "-123.187,49.300,-123.020,49.400";
    
    const response = await axios.get(HERE_BASE_URL, {
      params: {
        'in': `bbox:${northVanBBox}`,
        'locationReferencing': 'shape',
        'apikey': HERE_API_KEY
      },
      timeout: 10000
    });
    
    console.log(`✅ HERE API returned ${response.data?.results?.length || 0} traffic segments`);
    
    if (!response.data || !response.data.results || response.data.results.length === 0) {
      console.log('⚠️ No traffic data in HERE response, using synthetic data');
      return generateSyntheticTrafficData();
    }
    
    // Convert HERE traffic segments to our format
    const trafficData = [];
    const segmentMetadata = {};
    
    response.data.results.forEach((segment, index) => {
      // Extract traffic flow data
      const currentFlow = segment.currentFlow || {};
      const freeFlow = segment.freeFlow || {};
      
      // HERE API puts free flow speed inside currentFlow object
      const currentSpeed = currentFlow.speed || 50;
      const freeFlowSpeed = currentFlow.freeFlow || freeFlow.speed || currentSpeed || 50;
      const flowRatio = freeFlowSpeed > 0 ? Math.min(1.0, currentSpeed / freeFlowSpeed) : 1.0;
      
      // Create segment data with real coordinates from HERE
      const segmentId = `here-0-${index}`;
      
      // Extract coordinates from HERE API's nested structure
      let geojsonCoords = [];
      const shape = segment.location?.shape;
      if (shape && shape.links && Array.isArray(shape.links)) {
        // Flatten all points from all links into a single coordinate array
        shape.links.forEach(link => {
          if (link.points && Array.isArray(link.points)) {
            link.points.forEach(point => {
              if (point.lat && point.lng) {
                geojsonCoords.push([point.lng, point.lat]);
              }
            });
          }
        });
      }
      
      trafficData.push({
        segmentId: segmentId,
        ratio: Math.max(0.1, flowRatio),
        speed: currentSpeed,
        freeFlowSpeed: freeFlowSpeed,
        jamFactor: currentFlow.jamFactor || 0,
        confidence: currentFlow.confidence || 1.0
      });
      
      // Store segment metadata for frontend
      segmentMetadata[segmentId] = {
        name: `Traffic Segment ${index + 1}`,
        coordinates: geojsonCoords,
        type: 'road', // HERE doesn't categorize, so use generic
        originalData: {
          shape: shape,
          currentFlow: currentFlow,
          freeFlow: freeFlow
        }
      };
    });
    
    console.log(`✅ Processed ${trafficData.length} live traffic segments`);
    return { trafficData, segmentMetadata };
    
  } catch (error) {
    console.log('❌ HERE API failed, falling back to synthetic data:', error.response?.status, error.message);
    const syntheticData = generateSyntheticTrafficData();
    return { trafficData: syntheticData, segmentMetadata: {} };
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
    
    const { trafficData, segmentMetadata } = await fetchHereTrafficData();
    
    // Replace segment data entirely with new HERE metadata
    if (segmentMetadata && Object.keys(segmentMetadata).length > 0) {
      segmentData = segmentMetadata; // REPLACE, don't merge
      console.log(`📍 Replaced segment data with ${Object.keys(segmentMetadata).length} HERE segments`);
    }
    
    // Convert to interval format
    const interval = {
      timestamp: timestamp,
    };
    
    trafficData.forEach(data => {
      interval[data.segmentId] = data.ratio;
    });
    
    trafficIntervals.push(interval);
    
    // Keep last 24 hours (288 5-minute intervals)
    if (trafficIntervals.length > 288) {
      trafficIntervals = trafficIntervals.slice(-288);
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
    console.log(`📊 API Request: /api/traffic/today - ${trafficIntervals.length} intervals available`);
    
    // If no data, collect some now
    if (trafficIntervals.length === 0) {
      await collectTrafficData();
    }
    
    const response = {
      intervals: trafficIntervals,
      segments: segmentData,
      totalSegments: Object.keys(segmentData).length,
      currentIntervalIndex: trafficIntervals.length - 1,
      maxInterval: trafficIntervals.length - 1,
      coverage: `North Vancouver comprehensive: ${Object.keys(segmentData).length} segments across ${NORTH_VAN_ROADS.length} major roads`,
      dataSource: 'here-api-synthetic',
      counterFlow: {
        status: counterFlowData.currentStatus,
        lanesOutbound: counterFlowData.lanesOutbound || 1,
        statusSince: counterFlowData.statusSince,
        lastChecked: counterFlowData.lastChecked,
        durationMs: counterFlowData.statusSince ? 
          Date.now() - new Date(counterFlowData.statusSince).getTime() : null
      }
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

// Initialize and start
const startServer = async () => {
  console.log('🚀 Starting North Vancouver Traffic Server (HERE API)...');
  
  // Initialize road segments
  initializeSegments();
  
  // Start data collection
  isCollecting = true;
  await collectTrafficData();
  
  // Collect every 5 minutes
  setInterval(collectTrafficData, 5 * 60 * 1000);
  
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