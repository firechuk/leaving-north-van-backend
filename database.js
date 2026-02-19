// PostgreSQL integration for traffic data persistence
const { Pool } = require('pg');

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
            const dateKey = now.toISOString().split('T')[0];
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
    
    // Get today's traffic data for API endpoint
    async getTodayTrafficData() {
        try {
            const dateKey = new Date().toISOString().split('T')[0];
            console.log(`🔍 DB READ: Querying for dateKey: ${dateKey}`);
            
            const query = `
                SELECT interval_index, raw_data, observed_at
                FROM traffic_snapshots
                WHERE date_key = $1
                ORDER BY interval_index ASC;
            `;
            
            const result = await this.pool.query(query, [dateKey]);
            console.log(`🔍 DB read result: Found ${result.rows.length} rows for date ${dateKey}`);
            
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
            let segments = {};
            let counterFlow = {};
            
            result.rows.forEach((row, index) => {
                try {
                    const snapshot = JSON.parse(row.raw_data);
                    intervals.push(snapshot.intervalData);
                    segments = snapshot.segmentData; // Same for all intervals
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
    
    // Calculate 2-minute interval index (0-719 for 24 hours)
    calculateIntervalIndex(date) {
        const hours = date.getHours();
        const minutes = date.getMinutes();
        return Math.floor((hours * 60 + minutes) / 2);
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