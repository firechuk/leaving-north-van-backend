// PostgreSQL integration for traffic data persistence
const { Pool } = require('pg');

const SERVICE_TIME_ZONE = 'America/Vancouver';
const SERVICE_DAY_START_HOUR = 0;
const SNAPSHOT_INTERVAL_MINUTES = 2;
// Cutover guard: keep legacy 4am-keyed rows and new midnight-keyed rows from colliding.
const SERVICE_INTERVAL_INDEX_OFFSET = 720;
const MAX_SERVICE_DAYS = 21;

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
    return Math.floor(minutesSinceStart / SNAPSHOT_INTERVAL_MINUTES) + SERVICE_INTERVAL_INDEX_OFFSET;
}

function normalizeObservedAtTimestamp(observedAt) {
    if (observedAt instanceof Date) {
        return Number.isNaN(observedAt.getTime()) ? null : observedAt.toISOString();
    }

    if (typeof observedAt === 'number' && Number.isFinite(observedAt)) {
        const parsed = new Date(observedAt);
        return Number.isNaN(parsed.getTime()) ? null : parsed.toISOString();
    }

    if (typeof observedAt !== 'string') return null;
    const trimmed = observedAt.trim();
    if (!trimmed) return null;

    const parsedMs = Date.parse(trimmed);
    if (!Number.isFinite(parsedMs)) return null;
    return new Date(parsedMs).toISOString();
}

function getIndexedColumnsFromDefinition(definition) {
    if (typeof definition !== 'string') return [];
    const match = definition.match(/\(([^)]+)\)/);
    if (!match) return [];
    return match[1]
        .split(',')
        .map((part) => part.trim().replace(/"/g, '').toLowerCase())
        .filter(Boolean);
}

function isLegacyIntervalIndexOnlyDefinition(definition) {
    const columns = getIndexedColumnsFromDefinition(definition);
    return columns.length === 1 && columns[0] === 'interval_index';
}

function isLegacyIntervalIndexRangeCheck(definition) {
    const normalized = String(definition || '').toLowerCase().replace(/\s+/g, ' ');
    if (!normalized.includes('interval_index')) return false;
    return (
        normalized.includes('interval_index >= 0') &&
        (
            normalized.includes('interval_index < 720') ||
            normalized.includes('interval_index <= 719') ||
            normalized.includes('between 0 and 719')
        )
    );
}

function quoteIdentifier(identifier) {
    return `"${String(identifier).replace(/"/g, '""')}"`;
}

class TrafficDatabase {
    constructor() {
        this.pool = new Pool({
            connectionString: process.env.DATABASE_URL,
            ssl: process.env.NODE_ENV === 'production' ? { rejectUnauthorized: false } : false
        });
        
        this.readyPromise = this.initDatabase();
    }
    
    async initDatabase() {
        try {
            console.log('✅ Database connection established');
            await this.ensureTrafficSnapshotsConstraints();
        } catch (error) {
            console.error('❌ Database initialization failed:', error);
        }
    }

    async ensureTrafficSnapshotsConstraints() {
        const client = await this.pool.connect();
        const droppedLegacyConstraints = [];
        const droppedLegacyCheckConstraints = [];
        const droppedLegacyIndexes = [];

        try {
            await client.query('BEGIN');

            const uniqueConstraintsResult = await client.query(`
                SELECT conname, pg_get_constraintdef(oid) AS definition
                FROM pg_constraint
                WHERE conrelid = 'traffic_snapshots'::regclass
                  AND contype = 'u';
            `);

            for (const row of uniqueConstraintsResult.rows) {
                if (!isLegacyIntervalIndexOnlyDefinition(row.definition)) continue;
                await client.query(`ALTER TABLE traffic_snapshots DROP CONSTRAINT IF EXISTS ${quoteIdentifier(row.conname)};`);
                droppedLegacyConstraints.push(row.conname);
            }

            const hasTargetConstraint = uniqueConstraintsResult.rows.some((row) => {
                if (String(row?.conname || '').toLowerCase() === 'traffic_snapshots_date_key_interval_index_key') {
                    return true;
                }
                const columns = getIndexedColumnsFromDefinition(row?.definition);
                return columns.length === 2 &&
                    columns[0] === 'date_key' &&
                    columns[1] === 'interval_index';
            });

            if (!hasTargetConstraint) {
                await client.query(`
                    ALTER TABLE traffic_snapshots
                    ADD CONSTRAINT traffic_snapshots_date_key_interval_index_key
                    UNIQUE (date_key, interval_index);
                `);
            }

            const checkConstraintsResult = await client.query(`
                SELECT conname, pg_get_constraintdef(oid) AS definition
                FROM pg_constraint
                WHERE conrelid = 'traffic_snapshots'::regclass
                  AND contype = 'c';
            `);
            for (const row of checkConstraintsResult.rows) {
                if (!isLegacyIntervalIndexRangeCheck(row.definition)) continue;
                await client.query(`ALTER TABLE traffic_snapshots DROP CONSTRAINT IF EXISTS ${quoteIdentifier(row.conname)};`);
                droppedLegacyCheckConstraints.push(row.conname);
            }

            const uniqueIndexesResult = await client.query(`
                SELECT indexname, indexdef
                FROM pg_indexes
                WHERE schemaname = current_schema()
                  AND tablename = 'traffic_snapshots';
            `);

            for (const row of uniqueIndexesResult.rows) {
                const indexDef = String(row.indexdef || '');
                if (!/create\s+unique\s+index/i.test(indexDef)) continue;
                if (!isLegacyIntervalIndexOnlyDefinition(indexDef)) continue;
                await client.query(`DROP INDEX IF EXISTS ${quoteIdentifier(row.indexname)};`);
                droppedLegacyIndexes.push(row.indexname);
            }

            await client.query('COMMIT');
            console.log(
                `✅ DB constraint check complete: ` +
                `dropped legacy constraints=${droppedLegacyConstraints.length}, ` +
                `dropped legacy check constraints=${droppedLegacyCheckConstraints.length}, ` +
                `dropped legacy indexes=${droppedLegacyIndexes.length}`
            );
        } catch (error) {
            try {
                await client.query('ROLLBACK');
            } catch (rollbackError) {
                console.error('❌ Failed to rollback DB constraint migration:', rollbackError.message);
            }
            throw error;
        } finally {
            client.release();
        }
    }
    
    // Save complete traffic snapshot (replaces in-memory storage)
    async saveTrafficSnapshot(intervalData, segmentData, counterFlowData) {
        try {
            if (this.readyPromise) {
                await this.readyPromise;
            }
            const now = new Date();
            const dateKey = getServiceDayKey(now);
            const intervalIndex = this.calculateIntervalIndex(now);
            
            const snapshotData = {
                intervalData,
                segmentData,
                counterFlowData,
                timestamp: now.toISOString()
            };

            const values = [
                now.toISOString(), // Convert Date to ISO string for TEXT column
                dateKey,
                intervalIndex, 
                JSON.stringify(snapshotData)
            ];

            // Robust upsert that does not rely on ON CONFLICT index inference.
            // This avoids silent persistence loss when legacy schema drift exists.
            const updateQuery = `
                UPDATE traffic_snapshots
                SET observed_at = $1,
                    raw_data = $4
                WHERE date_key = $2
                  AND interval_index = $3
                RETURNING id;
            `;
            const updateResult = await this.pool.query(updateQuery, values);
            if (updateResult.rows.length > 0) {
                const updatedId = updateResult.rows[0].id;
                console.log(`✅ Saved/upserted traffic snapshot ${dateKey}-${intervalIndex} (id: ${updatedId}, mode:update)`);
                return updatedId;
            }

            const insertQuery = `
                INSERT INTO traffic_snapshots (observed_at, date_key, interval_index, raw_data)
                VALUES ($1, $2, $3, $4)
                RETURNING id;
            `;

            try {
                const insertResult = await this.pool.query(insertQuery, values);
                const insertedId = insertResult.rows[0].id;
                console.log(`✅ Saved/upserted traffic snapshot ${dateKey}-${intervalIndex} (id: ${insertedId}, mode:insert)`);
                return insertedId;
            } catch (insertError) {
                // Race-safe fallback when a concurrent writer inserts between update/insert.
                if (insertError?.code === '23505') {
                    const retryUpdateResult = await this.pool.query(updateQuery, values);
                    if (retryUpdateResult.rows.length > 0) {
                        const retryId = retryUpdateResult.rows[0].id;
                        console.log(`✅ Saved/upserted traffic snapshot ${dateKey}-${intervalIndex} (id: ${retryId}, mode:retry-update)`);
                        return retryId;
                    }
                }
                throw insertError;
            }
        } catch (error) {
            console.error('❌ Failed to save traffic snapshot:', error.message);
            console.error('❌ Full error details:', error);
            throw error;
        }
    }
    
    // Get traffic data for current + previous service days.
    async getTodayTrafficData(serviceDays = 2) {
        try {
            if (this.readyPromise) {
                await this.readyPromise;
            }
            const normalizedServiceDays = Math.max(1, Math.min(MAX_SERVICE_DAYS, Number(serviceDays) || 1));
            const { serviceDayKey, startUtc, endUtc } = getCurrentServiceDayWindow(new Date());
            const windowStartUtc = new Date(startUtc.getTime() - ((normalizedServiceDays - 1) * 24 * 60 * 60 * 1000));
            const windowStartIso = windowStartUtc.toISOString();
            const endIso = endUtc.toISOString();
            console.log(
                `🔍 DB READ: Querying ${normalizedServiceDays} service day(s) ending ${serviceDayKey} (` +
                `${windowStartIso} → ${endIso}, ${SERVICE_TIME_ZONE}, ${SERVICE_DAY_START_HOUR}:00 start)`
            );
            
            const query = `
                SELECT interval_index, raw_data, observed_at
                FROM traffic_snapshots
                WHERE observed_at >= $1 AND observed_at < $2
                ORDER BY observed_at ASC;
            `;
            
            const result = await this.pool.query(query, [windowStartIso, endIso]);
            console.log(`🔍 DB read result: Found ${result.rows.length} rows for service window ending ${serviceDayKey}`);
            
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
            let rewrittenTimestampCount = 0;
            
            result.rows.forEach((row, index) => {
                try {
                    const snapshot = JSON.parse(row.raw_data);
                    const intervalData = snapshot.intervalData && typeof snapshot.intervalData === 'object'
                        ? { ...snapshot.intervalData }
                        : null;

                    if (intervalData) {
                        // Use DB observed_at as canonical interval time. This avoids timeline gaps
                        // when historical raw_data timestamps were saved in mixed formats.
                        const observedAtIso = normalizeObservedAtTimestamp(row.observed_at);
                        if (observedAtIso && intervalData.timestamp !== observedAtIso) {
                            intervalData.timestamp = observedAtIso;
                            rewrittenTimestampCount += 1;
                        }
                        intervals.push(intervalData);
                    }

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
            
            console.log(
                `✅ DB read success: Reconstructed ${intervals.length} intervals, ` +
                `${Object.keys(segments).length} segments from ${result.rows.length} total rows ` +
                `(${rewrittenTimestampCount} interval timestamps normalized from observed_at)`
            );
            
            // Return data even if some records were corrupted (better than complete failure)
            return {
                intervals,
                segments,
                counterFlow,
                serviceDays: normalizedServiceDays,
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
    
    // Calculate 2-minute interval index within the service day (12am->12am, offset range 720-1439).
    calculateIntervalIndex(date) {
        return getServiceIntervalIndex(date);
    }
    
    // Get database statistics
    async getStats() {
        try {
            if (this.readyPromise) {
                await this.readyPromise;
            }
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
            if (this.readyPromise) {
                await this.readyPromise;
            }
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
