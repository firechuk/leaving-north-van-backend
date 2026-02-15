const express = require('express');
const axios = require('axios');
const fs = require('fs');
const path = require('path');

const app = express();
const PORT = 3002;

app.use(express.static('public'));
app.use(express.json());

// TomTom API configuration
const TOMTOM_API_KEY = 'BrHg5hRxSoJ6aFC0rlUzqd697SrBwVdu';
const TOMTOM_BASE_URL = 'https://api.tomtom.com/traffic/services/4';

// Key traffic monitoring points across North Vancouver
const TRAFFIC_POINTS = [
  { name: 'Lions Gate Bridge', point: '49.3154,-123.1384', type: 'bridge' },
  { name: 'Ironworkers Memorial Bridge', point: '49.2935,-123.0232', type: 'bridge' },
  { name: 'Upper Levels West', point: '49.3280,-123.0600', type: 'highway' },
  { name: 'Upper Levels East', point: '49.3300,-123.0900', type: 'highway' },
  { name: 'Lonsdale Avenue', point: '49.3200,-123.0736', type: 'arterial' },
  { name: 'Capilano Road', point: '49.3200,-123.1140', type: 'arterial' },
  { name: 'Lynn Valley Road', point: '49.3200,-123.0350', type: 'arterial' },
  { name: 'Marine Drive', point: '49.3250,-123.0800', type: 'arterial' },
  { name: 'Keith Road', point: '49.3120,-123.0700', type: 'arterial' },
  { name: 'Mountain Highway', point: '49.3300,-123.0500', type: 'arterial' }
];

// Fetch real traffic data from TomTom API
const fetchRealTrafficData = async () => {
  try {
    console.log(`Fetching traffic data from TomTom for ${TRAFFIC_POINTS.length} key points...`);
    
    const segments = {};
    const trafficData = [];
    let successCount = 0;
    
    // Query each traffic point
    for (let i = 0; i < TRAFFIC_POINTS.length; i++) {
      const trafficPoint = TRAFFIC_POINTS[i];
      
      try {
        const response = await axios.get(`${TOMTOM_BASE_URL}/flowSegmentData/absolute/10/json`, {
          params: {
            point: trafficPoint.point,
            key: TOMTOM_API_KEY,
            unit: 'KMPH'
          },
          timeout: 5000
        });
        
        if (response.data.flowSegmentData) {
          const segment = response.data.flowSegmentData;
          const coordinates = segment.coordinates?.coordinate || [];
          
          if (coordinates.length > 1) {
            // Convert TomTom coordinates to our format
            const coords = coordinates.map(coord => [coord.longitude, coord.latitude]);
            
            const currentSpeed = segment.currentSpeed || 50;
            const freeFlowSpeed = segment.freeFlowSpeed || 50;
            const congestionRatio = freeFlowSpeed > 0 ? currentSpeed / freeFlowSpeed : 1.0;
            
            // Break into smaller subsegments for granular visualization
            const subsegmentLength = 8;
            const numSubsegments = Math.max(1, Math.floor(coords.length / subsegmentLength));
            
            for (let sub = 0; sub < numSubsegments; sub++) {
              const startIdx = sub * subsegmentLength;
              const endIdx = Math.min(coords.length, (sub + 1) * subsegmentLength + 1);
              const subCoords = coords.slice(startIdx, endIdx);
              
              if (subCoords.length > 1) {
                const segmentId = `tomtom-${i}-${sub}`;
                
                // Add slight variation within segments for realism
                const variation = (Math.random() - 0.5) * 0.05;
                const subCongestionRatio = Math.max(0.1, Math.min(1.0, congestionRatio + variation));
                
                segments[segmentId] = {
                  name: `${trafficPoint.name} (${sub + 1})`,
                  coordinates: subCoords,
                  type: trafficPoint.type,
                  currentSpeed: currentSpeed,
                  freeFlowSpeed: freeFlowSpeed,
                  confidence: segment.confidence || 1.0
                };
                
                trafficData.push({
                  segmentId: segmentId,
                  ratio: subCongestionRatio
                });
              }
            }
            
            successCount++;
          }
        }
        
      } catch (pointError) {
        console.log(`Failed to fetch traffic for ${trafficPoint.name}:`, pointError.message);
      }
      
      // Small delay between API calls
      await new Promise(resolve => setTimeout(resolve, 50));
    }
    
    console.log(`Successfully processed ${successCount}/${TRAFFIC_POINTS.length} traffic points`);
    
    return {
      segments,
      trafficData,
      totalSegments: Object.keys(segments).length,
      coverage: `Real TomTom traffic data for ${successCount} North Van locations`,
      dataSource: 'tomtom-live'
    };
    
  } catch (error) {
    console.error('TomTom API error:', error.message);
    return {
      segments: {},
      trafficData: [],
      totalSegments: 0,
      coverage: 'No traffic data available',
      dataSource: 'offline'
    };
  }
};

// Historical data recording
const recordHistoricalData = (trafficData) => {
  const now = new Date();
  const dateStr = now.toISOString().split('T')[0];
  const dataDir = './historical-data';
  const filePath = path.join(dataDir, `${dateStr}.json`);
  
  // Ensure directory exists
  if (!fs.existsSync(dataDir)) {
    fs.mkdirSync(dataDir, { recursive: true });
  }
  
  // Read existing data or create new
  let dailyData = { date: dateStr, intervals: [] };
  if (fs.existsSync(filePath)) {
    try {
      dailyData = JSON.parse(fs.readFileSync(filePath, 'utf8'));
    } catch (error) {
      console.log('Creating new daily data file');
    }
  }
  
  // Add current interval (5-minute rounded timestamp)
  const roundedTime = new Date(Math.floor(now.getTime() / (5 * 60 * 1000)) * (5 * 60 * 1000));
  const intervalData = { timestamp: roundedTime.toISOString() };
  
  // Add traffic ratios for each segment
  trafficData.forEach(segment => {
    intervalData[segment.segmentId] = segment.ratio;
  });
  
  // Replace or append interval
  const existingIndex = dailyData.intervals.findIndex(interval => interval.timestamp === intervalData.timestamp);
  if (existingIndex >= 0) {
    dailyData.intervals[existingIndex] = intervalData;
  } else {
    dailyData.intervals.push(intervalData);
    dailyData.intervals.sort((a, b) => new Date(a.timestamp) - new Date(b.timestamp));
  }
  
  // Write back to file
  fs.writeFileSync(filePath, JSON.stringify(dailyData, null, 2));
  console.log(`Recorded traffic data: ${roundedTime.toLocaleTimeString()}`);
};

const loadHistoricalData = (dateStr) => {
  const dataDir = './historical-data';
  const filePath = path.join(dataDir, `${dateStr}.json`);
  
  if (fs.existsSync(filePath)) {
    try {
      return JSON.parse(fs.readFileSync(filePath, 'utf8'));
    } catch (error) {
      console.error(`Error loading historical data for ${dateStr}:`, error.message);
    }
  }
  
  return null;
};

// API Routes
app.get('/api/traffic/today', async (req, res) => {
  try {
    const today = new Date().toISOString().split('T')[0];
    
    // Load historical data for today
    let historicalData = loadHistoricalData(today);
    
    // Get current segments structure
    const realData = await fetchRealTrafficData();
    const segments = realData.segments || {};
    
    if (!historicalData) {
      historicalData = {
        date: today,
        intervals: []
      };
    }
    
    // Calculate current time index for limiting scrubber
    const now = new Date();
    const startOfDay = new Date(now.getFullYear(), now.getMonth(), now.getDate(), 4, 0);
    let minutesSinceStart = Math.floor((now - startOfDay) / (1000 * 60));
    if (now.getHours() < 4) minutesSinceStart += (24 * 60);
    const currentIntervalIndex = Math.floor(minutesSinceStart / 5);
    
    res.json({
      date: today,
      intervals: historicalData.intervals,
      segments: segments,
      totalSegments: Object.keys(segments).length,
      currentIntervalIndex: currentIntervalIndex,
      maxInterval: currentIntervalIndex, // Don't allow scrubbing past current time
      coverage: `Historical data: ${historicalData.intervals.length} recorded intervals`,
      dataSource: historicalData.intervals.length > 0 ? 'historical-live' : 'awaiting-data'
    });
    
  } catch (error) {
    console.error('Traffic API error:', error.message);
    res.status(500).json({
      error: 'Traffic data unavailable',
      message: error.message
    });
  }
});

app.get('/api/article/today', (req, res) => {
  const now = new Date();
  
  const articles = [
    {
      title: "North Shore Traffic Intelligence Report",
      content: "Live traffic monitoring is now active across North Vancouver. Data is being recorded every 5 minutes to build a comprehensive traffic database. Check back throughout the day as congestion patterns develop.",
      author: "Traffic Intelligence Division",
      timestamp: now.toISOString(),
      severity: "light"
    }
  ];
  
  res.json(articles[0]);
});

// Start recording traffic data
const startTrafficRecording = async () => {
  console.log('Starting historical traffic data recording every 5 minutes...');
  
  // Record initial data point
  try {
    const realData = await fetchRealTrafficData();
    if (realData.trafficData && realData.trafficData.length > 0) {
      recordHistoricalData(realData.trafficData);
    }
  } catch (error) {
    console.error('Failed to record initial traffic data:', error.message);
  }
  
  // Set up recurring recording every 5 minutes
  setInterval(async () => {
    try {
      const realData = await fetchRealTrafficData();
      if (realData.trafficData && realData.trafficData.length > 0) {
        recordHistoricalData(realData.trafficData);
      }
    } catch (error) {
      console.error('Failed to record traffic data:', error.message);
    }
  }, 5 * 60 * 1000);
};

// Start the server
app.listen(PORT, '0.0.0.0', async () => {
  console.log(`Leaving North Van server running on http://0.0.0.0:${PORT}`);
  console.log(`Access from phone: http://192.168.1.207:${PORT}`);
  
  // Begin historical data recording
  await startTrafficRecording();
});