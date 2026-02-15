# Leaving North Van - Backend API

Traffic data collection and API service for North Vancouver traffic monitoring.

## Features

- Real-time TomTom traffic data collection
- Historical data storage (5-minute intervals)
- RESTful API for frontend consumption
- Granular traffic segment analysis (109+ segments)
- CORS enabled for external frontends

## API Endpoints

### GET /health
Health check endpoint

### GET /api/traffic/today
Returns today's traffic data with historical intervals

### GET /api/article/today
Returns daily traffic article

## Environment Variables

- `TOMTOM_API_KEY`: Your TomTom API key
- `PORT`: Server port (Railway sets automatically)

## Deployment

### Railway (Recommended)
1. Push to GitHub
2. Connect to Railway
3. Set environment variables
4. Deploy automatically

### Manual
```bash
npm install
npm start
```

## Data Collection

Automatically collects traffic data every 5 minutes from:
- Lions Gate Bridge
- Ironworkers Memorial Bridge  
- Major arterials (Lonsdale, Marine Dr, etc.)
- Highway 1 Upper Levels

Data is stored in `./historical-data/YYYY-MM-DD.json` files.