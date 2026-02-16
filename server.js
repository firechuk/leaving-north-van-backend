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

// TomTom API configuration  
const TOMTOM_API_KEY = process.env.TOMTOM_API_KEY || 'YOUR_TOMTOM_API_KEY_NEEDED';
const TOMTOM_BASE_URL = 'https://api.tomtom.com/traffic/services/4/flowSegmentData/absolute/10/json';

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

// Generate segments from road bboxes
const initializeSegments = () => {
  console.log('Initializing North Vancouver road segments...');
  
  NORTH_VAN_ROADS.forEach((road, roadIndex) => {
    const [minLat, minLng, maxLat, maxLng] = road.bbox.split(',').map(Number);
    
    // Create multiple segments per road for granular visualization
    const segmentCount = road.priority === 'high' ? 8 : road.priority === 'medium' ? 4 : 2;
    
    for (let i = 0; i < segmentCount; i++) {
      const segmentId = `tomtom-${roadIndex}-${i}`;
      
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

// Fetch traffic data from TomTom API
const fetchTomTomTrafficData = async () => {
  if (!TOMTOM_API_KEY || TOMTOM_API_KEY === 'YOUR_TOMTOM_API_KEY_NEEDED') {
    console.log('⚠️ TomTom API key not configured, using synthetic data');
    return generateSyntheticTrafficData();
  }
  
  try {
    console.log('Fetching traffic data from TomTom API...');
    const trafficData = [];
    
    // Query traffic for each road using center points
    for (const [index, road] of NORTH_VAN_ROADS.entries()) {
      try {
        const [minLat, minLng, maxLat, maxLng] = road.bbox.split(',').map(Number);
        const centerLat = (minLat + maxLat) / 2;
        const centerLng = (minLng + maxLng) / 2;
        
        const response = await axios.get(TOMTOM_BASE_URL, {
          params: {
            point: `${centerLat},${centerLng}`,
            key: TOMTOM_API_KEY
          },
          timeout: 5000
        });
        
        // Process TomTom API response
        const segmentCount = road.priority === 'high' ? 8 : road.priority === 'medium' ? 4 : 2;
        
        for (let i = 0; i < segmentCount; i++) {
          const segmentId = `tomtom-${index}-${i}`;
          
          // Extract traffic flow ratio from TomTom response
          let flowRatio = 1.0; // Default free flow
          
          if (response.data && response.data.flowSegmentData) {
            const currentSpeed = response.data.flowSegmentData.currentSpeed || 50;
            const freeFlowSpeed = response.data.flowSegmentData.freeFlowSpeed || 50;
            flowRatio = freeFlowSpeed > 0 ? Math.min(1.0, currentSpeed / freeFlowSpeed) : 1.0;
          }
          
          // Add realistic variation per segment
          const variation = (Math.random() - 0.5) * 0.1;
          const adjustedRatio = Math.max(0.1, Math.min(1.0, flowRatio + variation));
          
          trafficData.push({
            segmentId: segmentId,
            ratio: adjustedRatio
          });
        }
        
        console.log(`✅ Fetched TomTom traffic for ${road.name}`);
        
      } catch (error) {
        console.log(`⚠️ Failed to fetch TomTom traffic for ${road.name}:`, error.message);
        // Use synthetic data for failed requests
        const segmentCount = road.priority === 'high' ? 8 : road.priority === 'medium' ? 4 : 2;
        for (let i = 0; i < segmentCount; i++) {
          const segmentId = `tomtom-${index}-${i}`;
          trafficData.push({
            segmentId: segmentId,
            ratio: generateTimeBasedTrafficRatio(road.type)
          });
        }
      }
    }
    
    return trafficData;
    
  } catch (error) {
    console.log('❌ TomTom API failed, falling back to synthetic data:', error.message);
    return generateSyntheticTrafficData();
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

// Collect traffic data
const collectTrafficData = async () => {
  try {
    const timestamp = new Date().toISOString();
    console.log(`🚗 Collecting traffic data at ${timestamp}`);
    
    const trafficData = await fetchTomTomTrafficData();
    
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
      dataSource: 'tomtom-api-synthetic'
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
  
  app.listen(PORT, () => {
    console.log(`🌐 Server running on port ${PORT}`);
    console.log(`📍 Monitoring ${NORTH_VAN_ROADS.length} major roads with ${Object.keys(segmentData).length} segments`);
    console.log(`🔑 TomTom API: ${TOMTOM_API_KEY !== 'YOUR_TOMTOM_API_KEY_NEEDED' ? 'Configured' : 'Using synthetic data'}`);
  });
};

startServer().catch(console.error);