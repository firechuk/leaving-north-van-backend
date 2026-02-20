// PostgreSQL integration for traffic data persistence
const { Pool } = require('pg');

const SERVICE_TIME_ZONE = 'America/Vancouver';
const SERVICE_DAY_START_HOUR = 4;
const SNAPSHOT_INTERVAL_MINUTES = 2;

const serviceTimePartsFormatter = new Intl.DateTimeFormat('en-CA', {
    timeZone: SERVICE_TIME_ZONE,
    year: 'numeric',
    month: '2-digit',
    day: '2-digit',
    hour: '2-digit',
    minute: '2-digit',
    hour12: false,
    hourCycle: 'h23'
});

function getTimeZoneOffsetMs(date, timeZone) {
    if (!(date instanceof Date) || Number.isNaN(date.getTime())) return 0;

    const dtf = new Intl.DateTimeFormat('en-US', {
        timeZone,
        year: 'numeric',
        month: '2-digit',
        day: '2-digit',
        hour: '2-digit',
        minute: '2-digit',
        second: '2-digit',
        hour12: false,
        hourCycle: 'h23'
    });

    const parts = dtf.formatToParts(date);
    const values = {};
    parts.forEach((part) => {
        if (part.type !== 'literal') {
            values[part.type] = part.value;
        }
    });

    const asUTC = Date.UTC(
        Number(values.year),
        Number(values.month) - 1,
        Number(values.day),
        Number(values.hour),
        Number(values.minute),
        Number(values.second)
    );

    return asUTC - date.getTime();
}

function getDatePartsInServiceTimeZone(date) {
    const parts = serviceTimePartsFormatter.formatToParts(date);
    const values = {};
    parts.forEach((part) => {
        if (part.type !== 'literal') {
            values[part.type] = part.value;
        }
    });

    const parsedHour = Number(values.hour);
    return {
        year: Number(values.year),
        month: Number(values.month),
        day: Number(values.day),
        hour: Number.isFinite(parsedHour) ? parsedHour % 24 : 0,
        minute: Number(values.minute)
    };
}

function formatDayKeyFromParts(year, month, day) {
    return `${String(year).padStart(4, '0')}-${String(month).padStart(2, '0')}-${String(day).padStart(2, '0')}`;
}

function getUtcForTimeZoneDateTime(year, month, day, hour = 0, minute = 0, second = 0, millisecond = 0, timeZone = SERVICE_TIME_ZONE) {
    let utcMs = Date.UTC(year, month - 1, day, hour, minute, second, millisecond);
    for (let i = 0; i < 3; i++) {
        const offset = getTimeZoneOffsetMs(new Date(utcMs), timeZone);
        const nextUtc = Date.UTC(year, month - 1, day, hour, minute, second, millisecond) - offset;
        if (Math.abs(nextUtc - utcMs) < 1000) {
            utcMs = nextUtc;
            break;
        }
        utcMs = nextUtc;
    }
    return new Date(utcMs);
}

function getServiceDayKey(date = new Date()) {
    const shifted = new Date(date.getTime() - (SERVICE_DAY_START_HOUR * 60 * 60 * 1000));
    const parts = getDatePartsInServiceTimeZone(shifted);
    return formatDayKeyFromParts(parts.year, parts.month, parts.day);
}

function getCurrentServiceDayWindow(now = new Date()) {
    const currentParts = getDatePartsInServiceTimeZone(now);
    const serviceDayStartDate = new Date(Date.UTC(currentParts.year, currentParts.month - 1, currentParts.day));
    if (currentParts.hour < SERVICE_DAY_START_HOUR) {
        serviceDayStartDate.setUTCDate(serviceDayStartDate.getUTCDate() - 1);
    }

    const serviceDayKey = formatDayKeyFromParts(
        serviceDayStartDate.getUTCFullYear(),
        serviceDayStartDate.getUTCMonth() + 1,
        serviceDayStartDate.getUTCDate()
    );

    const startUtc = getUtcForTimeZoneDateTime(
        serviceDayStartDate.getUTCFullYear(),
        serviceDayStartDate.getUTCMonth() + 1,
        serviceDayStartDate.getUTCDate(),
        SERVICE_DAY_START_HOUR,
        0,
        0,
        0,
        SERVICE_TIME_ZONE
    );
    const endUtc = new Date(startUtc.getTime() + (24 * 60 * 60 * 1000));

    return {
        serviceDayKey,
        startUtc,
        endUtc
    };
}

function getServiceIntervalIndex(date = new Date()) {
    const parts = getDatePartsInServiceTimeZone(date);
    const totalMinutes = (parts.hour * 60) + parts.minute;
    const startMinutes = SERVICE_DAY_START_HOUR * 60;
    const minutesSinceStart = (totalMinutes - startMinutes + (24 * 60)) % (24 * 60);
    return Math.floor(minutesSinceStart / SNAPSHOT_INTERVAL_MINUTES);
}

class TrafficDatabase {
    constructor() {
        this.pool = new Pool({
            connectionString: process.env.DATABASE_URL,
            ssl: process.env.NODE_ENV === 'production' ? { rejectUnauthorized: false } : false
        });
        
        this.initDatabase();
    }
    
    async initDatabase() {
        try {
            // Create table if it doesn't exist (Railway creates manually via UI)
            console.log('✅ Database connection established');
        } catch (error) {
            console.error('❌ Database initialization failed:', error);
        }
    }
    
    // Save complete traffic snapshot (replaces in-memory storage)
    async saveTrafficSnapshot(intervalData, segmentData, counterFlowData) {
        try {
            const now = new Date();
            const dateKey = getServiceDayKey(now);
            const intervalIndex = this.calculateIntervalIndex(now);
            
            const snapshotData = {
                intervalData,
                segmentData,
                counterFlowData,
                timestamp: now.toISOString()
            };
            
            const query = `
                INSERT INTO traffic_snapshots (observed_at, date_key, interval_index, raw_data)
                VALUES ($1, $2, $3, $4)
                RETURNING id;
            `;
            
            const values = [
                now.toISOString(), // Convert Date to ISO string for TEXT column
                dateKey,
                intervalIndex, 
                JSON.stringify(snapshotData)
            ];
            
            const result = await this.pool.query(query, values);
            console.log(`✅ Saved traffic snapshot ${dateKey}-${intervalIndex} (id: ${result.rows[0].id})`);
            return result.rows[0].id;
        } catch (error) {
            console.error('❌ Failed to save traffic snapshot:', error.message);
            console.error('❌ Full error details:', error);
            throw error;
        }
    }
    
    // Get the current service day's traffic data for API endpoint.
    async getTodayTrafficData() {
        try {
            const { serviceDayKey, startUtc, endUtc } = getCurrentServiceDayWindow(new Date());
            const startIso = startUtc.toISOString();
            const endIso = endUtc.toISOString();
            console.log(`🔍 DB READ: Querying for service day ${serviceDayKey} (${startIso} → ${endIso}, ${SERVICE_TIME_ZONE}, ${SERVICE_DAY_START_HOUR}:00 start)`);
            
            const query = `
                SELECT interval_index, raw_data, observed_at
                FROM traffic_snapshots
                WHERE observed_at >= $1 AND observed_at < $2
                ORDER BY observed_at ASC;
            `;
            
            const result = await this.pool.query(query, [startIso, endIso]);
            console.log(`🔍 DB read result: Found ${result.rows.length} rows for service day ${serviceDayKey}`);
            
            if (result.rows.length === 0) {
                // DEBUG: Check what dates are actually in the database
                const allDatesQuery = `
                    SELECT DISTINCT date_key, COUNT(*) as count 
                    FROM traffic_snapshots 
                    GROUP BY date_key 
                    ORDER BY date_key DESC 
                    LIMIT 5;
                `;
                const allDatesResult = await this.pool.query(allDatesQuery);
                console.log(`🔍 DB dates available:`, allDatesResult.rows);
                
                return {
                    intervals: [],
                    segments: {},
                    counterFlow: {}
                };
            }
            
            // Reconstruct data from database
            const intervals = [];
            const segments = {};
            let counterFlow = {};
            
            result.rows.forEach((row, index) => {
                try {
                    const snapshot = JSON.parse(row.raw_data);
                    intervals.push(snapshot.intervalData);
                    if (snapshot.segmentData && typeof snapshot.segmentData === 'object') {
                        // Keep a union of segment metadata so historical intervals still resolve
                        // even when segment keys change between snapshots.
                        Object.assign(segments, snapshot.segmentData);
                    }
                    counterFlow = snapshot.counterFlowData; // Latest state
                } catch (parseError) {
                    console.error(`❌ JSON parse error for row ${index} (id: ${row.id || 'unknown'}):`, parseError.message);
                    console.error(`❌ Raw data preview:`, row.raw_data?.substring(0, 100) + '...');
                    // Skip corrupted records but continue processing others
                }
            });
            
            console.log(`✅ DB read success: Reconstructed ${intervals.length} intervals, ${Object.keys(segments).length} segments from ${result.rows.length} total rows`);
            
            // Return data even if some records were corrupted (better than complete failure)
            return {
                intervals,
                segments,
                counterFlow,
                fromDatabase: true,
                recordCount: result.rows.length,
                validRecords: intervals.length,
                corruptedRecords: result.rows.length - intervals.length
            };
        } catch (error) {
            console.error('❌ Failed to get traffic data from database:', error);
            return null;
        }
    }
    
    // Calculate 2-minute interval index within the service day (4am->4am, 0-719).
    calculateIntervalIndex(date) {
        return getServiceIntervalIndex(date);
    }
    
    // Get database statistics
    async getStats() {
        try {
            const query = `
                SELECT 
                    date_key,
                    COUNT(*) as interval_count,
                    MIN(observed_at) as first_snapshot,
                    MAX(observed_at) as last_snapshot
                FROM traffic_snapshots
                GROUP BY date_key
                ORDER BY date_key DESC
                LIMIT 7;
            `;
            
            const result = await this.pool.query(query);
            return result.rows;
        } catch (error) {
            console.error('❌ Failed to get database stats:', error);
            return [];
        }
    }
    
    // Clear all traffic data (emergency database cleanup)
    async clearAllTrafficData() {
        try {
            console.log('🗑️  Clearing all traffic data from database...');
            
            const query = `DELETE FROM traffic_snapshots;`;
            const result = await this.pool.query(query);
            
            console.log(`✅ Cleared ${result.rowCount} traffic snapshots from database`);
            console.log('💾 Database space freed up - ready for optimized data collection');
            
            return { success: true, deletedRows: result.rowCount };
            
        } catch (error) {
            console.error('❌ Failed to clear traffic data:', error);
            return { success: false, error: error.message };
        }
    }
}

module.exports = TrafficDatabase;
