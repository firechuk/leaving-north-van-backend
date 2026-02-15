const express = require('express');
const axios = require('axios');
const path = require('path');

// North Vancouver bounding box (roughly)
const NORTH_VAN_BBOX = {
  minLon: -123.27,  // West boundary (West Vancouver border)
  minLat: 49.25,    // South boundary (Burrard Inlet)
  maxLon: -122.95,  // East boundary (Burnaby border)
  maxLat: 49.38     // North boundary (mountains)
};

// TomTom API configuration
const TOMTOM_API_KEY = 'BrHg5hRxSoJ6aFC0rlUzqd697SrBwVdu';
const TOMTOM_BASE_URL = 'https://api.tomtom.com/traffic/services/4';

const app = express();
const PORT = 3002;

app.use(express.static('public'));
app.use(express.json());

// Comprehensive North Vancouver road network for mock data
const NORTH_VAN_ROADS = [
  // Major Bridges
  { id: 'lions-gate-sb', name: 'Lions Gate Bridge SB', coords: [[-123.1384,49.3154],[-123.1435,49.3095],[-123.1475,49.3035]], type: 'bridge' },
  { id: 'lions-gate-nb', name: 'Lions Gate Bridge NB', coords: [[-123.1475,49.3035],[-123.1435,49.3095],[-123.1384,49.3154]], type: 'bridge' },
  { id: 'ironworkers-sb', name: 'Ironworkers Memorial SB', coords: [[-123.0232,49.2935],[-123.0270,49.2890],[-123.0320,49.2830]], type: 'bridge' },
  { id: 'ironworkers-nb', name: 'Ironworkers Memorial NB', coords: [[-123.0320,49.2830],[-123.0270,49.2890],[-123.0232,49.2935]], type: 'bridge' },
  
  // Highway 1 (Trans-Canada)
  { id: 'hwy1-west', name: 'Highway 1 Westbound', coords: [[-123.0232,49.2935],[-123.0500,49.3100],[-123.0820,49.3230]], type: 'highway' },
  { id: 'hwy1-east', name: 'Highway 1 Eastbound', coords: [[-123.0820,49.3230],[-123.0500,49.3100],[-123.0232,49.2935]], type: 'highway' },
  { id: 'hwy1-upper-west', name: 'Upper Levels Westbound', coords: [[-123.0600,49.3280],[-123.0900,49.3320],[-123.1200,49.3350]], type: 'highway' },
  { id: 'hwy1-upper-east', name: 'Upper Levels Eastbound', coords: [[-123.1200,49.3350],[-123.0900,49.3320],[-123.0600,49.3280]], type: 'highway' },
  
  // Major North-South Arterials
  { id: 'lonsdale-sb', name: 'Lonsdale Ave Southbound', coords: [[-123.0736,49.3500],[-123.0736,49.3200],[-123.0736,49.2900]], type: 'arterial' },
  { id: 'lonsdale-nb', name: 'Lonsdale Ave Northbound', coords: [[-123.0736,49.2900],[-123.0736,49.3200],[-123.0736,49.3500]], type: 'arterial' },
  { id: 'capilano-sb', name: 'Capilano Rd Southbound', coords: [[-123.1140,49.3500],[-123.1140,49.3200],[-123.1384,49.3154]], type: 'arterial' },
  { id: 'capilano-nb', name: 'Capilano Rd Northbound', coords: [[-123.1384,49.3154],[-123.1140,49.3200],[-123.1140,49.3500]], type: 'arterial' },
  
  // Major East-West Arterials  
  { id: 'marine-dr-wb', name: 'Marine Dr Westbound', coords: [[-123.0400,49.3270],[-123.0800,49.3250],[-123.1200,49.3270],[-123.1384,49.3154]], type: 'arterial' },
  { id: 'marine-dr-eb', name: 'Marine Dr Eastbound', coords: [[-123.1384,49.3154],[-123.1200,49.3270],[-123.0800,49.3250],[-123.0400,49.3270]], type: 'arterial' },
  { id: 'keith-rd-wb', name: 'Keith Rd Westbound', coords: [[-123.0400,49.3150],[-123.0700,49.3120],[-123.1000,49.3100],[-123.1300,49.3080]], type: 'arterial' },
  { id: 'keith-rd-eb', name: 'Keith Rd Eastbound', coords: [[-123.1300,49.3080],[-123.1000,49.3100],[-123.0700,49.3120],[-123.0400,49.3150]], type: 'arterial' },
  { id: '3rd-st-wb', name: '3rd Street Westbound', coords: [[-123.0600,49.3050],[-123.0800,49.3050],[-123.1000,49.3050],[-123.1200,49.3050]], type: 'arterial' },
  { id: '3rd-st-eb', name: '3rd Street Eastbound', coords: [[-123.1200,49.3050],[-123.1000,49.3050],[-123.0800,49.3050],[-123.0600,49.3050]], type: 'arterial' },
  
  // Secondary Roads & Collectors
  { id: 'lynn-valley-sb', name: 'Lynn Valley Rd SB', coords: [[-123.0350,49.3500],[-123.0350,49.3300],[-123.0232,49.2935]], type: 'collector' },
  { id: 'lynn-valley-nb', name: 'Lynn Valley Rd NB', coords: [[-123.0232,49.2935],[-123.0350,49.3300],[-123.0350,49.3500]], type: 'collector' },
  { id: 'mountain-hwy-sb', name: 'Mountain Highway SB', coords: [[-123.0500,49.3500],[-123.0500,49.3300],[-123.0500,49.3100]], type: 'collector' },
  { id: 'mountain-hwy-nb', name: 'Mountain Highway NB', coords: [[-123.0500,49.3100],[-123.0500,49.3300],[-123.0500,49.3500]], type: 'collector' },
  { id: 'dollarton-hwy-wb', name: 'Dollarton Highway WB', coords: [[-122.9800,49.3100],[-123.0200,49.3100],[-123.0500,49.3100]], type: 'collector' },
  { id: 'dollarton-hwy-eb', name: 'Dollarton Highway EB', coords: [[-123.0500,49.3100],[-123.0200,49.3100],[-122.9800,49.3100]], type: 'collector' },
  
  // Local congestion-prone streets
  { id: 'fell-ave-wb', name: 'Fell Ave Westbound', coords: [[-123.0600,49.3000],[-123.0800,49.3000],[-123.1000,49.3000]], type: 'local' },
  { id: 'fell-ave-eb', name: 'Fell Ave Eastbound', coords: [[-123.1000,49.3000],[-123.0800,49.3000],[-123.0600,49.3000]], type: 'local' },
  { id: 'queens-rd-wb', name: 'Queens Rd Westbound', coords: [[-123.0700,49.2950],[-123.0900,49.2950],[-123.1100,49.2950]], type: 'local' },
  { id: 'queens-rd-eb', name: 'Queens Rd Eastbound', coords: [[-123.1100,49.2950],[-123.0900,49.2950],[-123.0700,49.2950]], type: 'local' },
  { id: 'pemberton-ave-sb', name: 'Pemberton Ave SB', coords: [[-123.0900,49.3400],[-123.0900,49.3200],[-123.0900,49.3000]], type: 'local' },
  { id: 'pemberton-ave-nb', name: 'Pemberton Ave NB', coords: [[-123.0900,49.3000],[-123.0900,49.3200],[-123.0900,49.3400]], type: 'local' }
];

// Key traffic chokepoints across North Vancouver (focused on major routes)  
const TRAFFIC_POINTS = [
  // Major Bridges
  { name: 'Lions Gate Bridge', point: '49.3154,-123.1384', type: 'bridge' },
  { name: 'Ironworkers Memorial Bridge', point: '49.2935,-123.0232', type: 'bridge' },
  
  // Highway 1 corridors  
  { name: 'Upper Levels West', point: '49.3280,-123.0600', type: 'highway' },
  { name: 'Upper Levels East', point: '49.3300,-123.0900', type: 'highway' },
  
  // Major arterials
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
            
            // Break long routes into multiple subsegments for granular visualization
            const subsegmentLength = 8; // ~8 coordinates per subsegment
            const numSubsegments = Math.max(1, Math.floor(coords.length / subsegmentLength));
            
            for (let sub = 0; sub < numSubsegments; sub++) {
              const startIdx = sub * subsegmentLength;
              const endIdx = Math.min(coords.length, (sub + 1) * subsegmentLength + 1); // +1 for overlap
              const subCoords = coords.slice(startIdx, endIdx);
              
              if (subCoords.length > 1) {
                const segmentId = `tomtom-${i}-${sub}`;
                
                // Add slight variation in congestion within the segment for realism
                const variation = (Math.random() - 0.5) * 0.1; // ±5% variation
                const subCongestionRatio = Math.max(0.1, Math.min(1.0, congestionRatio + variation));
                
                segments[segmentId] = {
                  name: `${trafficPoint.name} (${sub + 1})`,
                  coordinates: subCoords,
                  type: trafficPoint.type,
                  currentSpeed: currentSpeed,
                  freeFlowSpeed: freeFlowSpeed,
                  confidence: segment.confidence || 1.0,
                  parentSegment: i,
                  subsegment: sub
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
      
      // Small delay between API calls to be nice to TomTom
      await new Promise(resolve => setTimeout(resolve, 50));
    }
    
    console.log(`Successfully processed ${successCount}/${TRAFFIC_POINTS.length} traffic points`);
    
    // If we got real data, return it
    if (Object.keys(segments).length > 0) {
      return { 
        segments, 
        trafficData,
        totalSegments: Object.keys(segments).length,
        coverage: `Real TomTom traffic data for ${successCount} North Van locations`,
        dataSource: 'tomtom-live'
      };
    }
    
  } catch (error) {
    console.error('Error fetching TomTom traffic data:', error.message);
    console.log('No fallback - returning empty data');
    
    // Return minimal structure instead of mock data
    return {
      segments: {},
      trafficData: [],
      totalSegments: 0,
      coverage: 'No traffic data available',
      dataSource: 'offline'
    };
  }
};

// Classify road type based on road name
const classifyRoadType = (roadName = '') => {
  const name = roadName.toLowerCase();
  if (name.includes('bridge') || name.includes('lions gate') || name.includes('ironworkers')) {
    return 'bridge';
  }
  if (name.includes('highway') || name.includes('hwy') || name.includes('trans-canada') || name.includes('upper levels')) {
    return 'highway';
  }
  if (name.includes('marine') || name.includes('lonsdale') || name.includes('capilano') || name.includes('keith')) {
    return 'arterial';
  }
  if (name.includes('avenue') || name.includes('street') || name.includes('road')) {
    return 'collector';
  }
  return 'local';
};

// Historical data recording
const fs = require('fs');
const path = require('path');

const recordHistoricalData = (trafficData) => {
  const now = new Date();
  const dateStr = now.toISOString().split('T')[0]; // YYYY-MM-DD
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

// Generate basic structure when no data available 
const generateEmptyTrafficData = () => {
  const now = new Date();
  const startOfDay = new Date(now.getFullYear(), now.getMonth(), now.getDate(), 4, 0); // 4 AM
  const intervals = [];
  
  // Generate 5-minute intervals from 4 AM today to 4 AM tomorrow (24 hours)
  for (let i = 0; i < 288; i++) { // 24 hours * 12 intervals per hour
    const timestamp = new Date(startOfDay.getTime() + i * 5 * 60 * 1000);
    const hour = timestamp.getHours();
    const intervalData = { timestamp: timestamp.toISOString() };
    
    // Generate traffic for each road segment
    NORTH_VAN_ROADS.forEach(road => {
      let baseCongestion = 1.0; // Start with free flow
      
      // Different congestion patterns by road type
      if (road.type === 'bridge') {
        // Bridges get most congested
        if ((hour >= 7 && hour <= 9) || (hour >= 16 && hour <= 18)) {
          baseCongestion = 0.2 + Math.random() * 0.3; // Very heavy
        } else if ((hour >= 6 && hour <= 7) || (hour >= 15 && hour <= 16) || (hour >= 18 && hour <= 19)) {
          baseCongestion = 0.4 + Math.random() * 0.3; // Heavy
        } else {
          baseCongestion = 0.7 + Math.random() * 0.3; // Light
        }
      } else if (road.type === 'highway') {
        // Highways second most congested  
        if ((hour >= 7 && hour <= 9) || (hour >= 16 && hour <= 18)) {
          baseCongestion = 0.3 + Math.random() * 0.3; // Heavy
        } else if ((hour >= 6 && hour <= 7) || (hour >= 15 && hour <= 16) || (hour >= 18 && hour <= 19)) {
          baseCongestion = 0.5 + Math.random() * 0.3; // Moderate
        } else {
          baseCongestion = 0.8 + Math.random() * 0.2; // Light
        }
      } else if (road.type === 'arterial') {
        // Arterials moderately congested
        if ((hour >= 7 && hour <= 9) || (hour >= 16 && hour <= 18)) {
          baseCongestion = 0.4 + Math.random() * 0.4; // Moderate to heavy
        } else if ((hour >= 6 && hour <= 7) || (hour >= 15 && hour <= 16) || (hour >= 18 && hour <= 19)) {
          baseCongestion = 0.6 + Math.random() * 0.3; // Light to moderate
        } else {
          baseCongestion = 0.8 + Math.random() * 0.2; // Mostly free flow
        }
      } else { // collector and local roads
        // Only show congestion during peak rush hour
        if ((hour >= 7.5 && hour <= 8.5) || (hour >= 16.5 && hour <= 17.5)) {
          baseCongestion = 0.5 + Math.random() * 0.4; // Some congestion during peak
        } else {
          baseCongestion = 0.8 + Math.random() * 0.2; // Usually free flow
        }
      }
      
      intervalData[road.id] = Math.max(0.1, Math.min(1.0, baseCongestion));
    });
    
    intervals.push(intervalData);
  }
  
  return intervals;
};

// Start recording traffic data immediately  
const startTrafficRecording = async () => {
  console.log('Starting historical traffic data recording...');
  
  setInterval(async () => {
    try {
      const realData = await fetchRealTrafficData();
      if (realData.segments && realData.trafficData.length > 0) {
        recordHistoricalData(realData.trafficData);
      }
    } catch (error) {
      console.error('Failed to record traffic data:', error.message);
    }
  }, 5 * 60 * 1000); // Every 5 minutes
  
  // Record initial data point
  try {
    const realData = await fetchRealTrafficData();
    if (realData.segments && realData.trafficData.length > 0) {
      recordHistoricalData(realData.trafficData);
    }
  } catch (error) {
    console.error('Failed to record initial traffic data:', error.message);
  }
};

// API Routes
app.get('/api/traffic/today', async (req, res) => {
  try {
    const today = new Date().toISOString().split('T')[0];
    
    // Load historical data for today
    let historicalData = loadHistoricalData(today);
    
    if (!historicalData || historicalData.intervals.length === 0) {
      // No historical data yet - get current segments structure
      const realData = await fetchRealTrafficData();
      
      if (!realData.segments || Object.keys(realData.segments).length === 0) {
        throw new Error('No traffic data available');
      }
      
      historicalData = {
        date: today,
        intervals: [],
        segments: realData.segments
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
      segments: historicalData.segments || {},
      totalSegments: NORTH_VAN_ROADS.length,
      coverage: 'Mock traffic data (TomTom API unavailable)',
      dataSource: 'mock-fallback'
    });
  }
});

app.get('/api/article/today', (req, res) => {
  const now = new Date();
  const hour = now.getHours();
  
  // If it's before 4 AM, we're still showing yesterday's article
  // After 4 AM, we generate today's article based on current conditions
  let articleDate = new Date();
  let timeOfDay = 'morning';
  
  if (hour < 4) {
    // Before 4 AM - show yesterday's final article
    articleDate.setDate(articleDate.getDate() - 1);
    timeOfDay = 'evening'; // Yesterday evening recap
  } else if (hour >= 4 && hour < 12) {
    timeOfDay = 'morning';
  } else if (hour >= 12 && hour < 17) {
    timeOfDay = 'afternoon';
  } else {
    timeOfDay = 'evening';
  }
  
  // Mock Fallout-style broadcaster article
  const articles = {
    morning: {
      headline: "Morning Exodus from the North Shore Wasteland",
      content: `Good morning, survivors. This is your faithful correspondent reporting from the post-apocalyptic traffic conditions plaguing our fair North Shore settlements.

At 0730 hours, scouts observed the usual gathering of commuter tribes at the Marine Drive staging area. The Lions Gate checkpoint experienced moderate to heavy congestion, with vehicular flow dropping to just 40% of optimal capacity. 

Meanwhile, our intelligence network reports that the Ironworkers Memorial crossing maintained better throughput, though mechanical failures near the Lower Lynn junction caused brief delays for eastbound travelers.

Weather conditions remain stable with light precipitation - nothing our hardy North Shore dwellers can't handle. Remember, citizens: merge like your life depends on it, because in this traffic wasteland, it just might.

This has been your traffic correspondent. Stay vigilant, stay caffeinated, and may the merge lanes be ever in your favor.`,
      timestamp: new Date().toISOString(),
      severity: 'moderate'
    },
    afternoon: {
      headline: "Afternoon Siege: The Return Journey Begins",
      content: `Attention all North Shore refugees preparing for the afternoon exodus from the Downtown Core.

Intelligence suggests heavy resistance building at both primary crossing points. The Lions Gate bottleneck is experiencing severe congestion, with flow ratios dropping below 30% - that's entering gridlock territory, folks.

The Ironworkers route shows more promise, though our traffic reconnaissance indicates growing pressure at the Mountain Highway confluence. 

A word of caution: avoid the Stanley Park approach between 1600 and 1800 hours unless absolutely necessary. Tourist activity has reached critical mass, creating additional friction in an already volatile situation.

Remember the golden rule of the wasteland: patience preserves fuel, and fuel preserves sanity.`,
      timestamp: new Date().toISOString(),
      severity: 'heavy'
    },
    evening: {
      headline: "Evening Calm: The Bridges Rest",
      content: `Evening, North Shore survivors. The day's great migration has concluded, and our bridge infrastructure enters its recovery phase.

Traffic flow has normalized across both primary corridors, with the Lions Gate and Ironworkers crossings returning to optimal capacity. The daily siege has lifted, and our hardy commuters have successfully completed another day's journey through the urban wilderness.

Tonight's forecast calls for clear conditions with light winds from the southeast - perfect weather for those late-night supply runs to the mainland settlements.

Until tomorrow's dawn brings another chapter in our ongoing traffic saga, this is your correspondent signing off. Sleep well, North Shore - you've earned it.`,
      timestamp: new Date().toISOString(),
      severity: 'light'
    }
  };
  
  res.json(articles[timeOfDay]);
});

app.listen(PORT, '0.0.0.0', () => {
  console.log(`Leaving North Van server running on http://0.0.0.0:${PORT}`);
  console.log(`Access from phone: http://192.168.1.207:${PORT}`);
});