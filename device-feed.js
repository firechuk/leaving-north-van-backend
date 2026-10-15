// The desk device's feed (GET /api/device/v1): one small JSON with everything its screen
// draws. Pure functions over the same {intervals, segments} shape /api/traffic/window
// returns, so it can be tested against archived days without a database.
//
// Schema 1 (the same shape as the simulator's fixtures, device/firmware/tools/make_fixture.py):
//   day, weekday, slot_minutes, first_slot_minute
//   bridges[]   {id, name, today[], usual[]}  mean speed / free-flow per slot, null = no data
//   ring        {metric: "slow_fraction", slow_below, today[], usual[]}
//               share of ALL segments below 0.6 of free-flow: the Instagram heat strip
//   usual_basis "the last 3 Tuesdays"
//   map         {version, segments[{bridge, pts[x,y,...]}], levels[]}  pts in 466 px screen
//               space; levels: one char per slot, 0 calm .. 3 heavy, "." no data
//   generated_at, latest_reading (ISO), now_minute (local minute of the latest reading)

const TIME_ZONE = 'America/Vancouver';
const SLOT_MINUTES = 10;
const FIRST_SLOT_MINUTE = 0;
const SLOT_COUNT = (24 * 60) / SLOT_MINUTES;
const SLOW_BELOW = 0.6;                       // same as the reel's time bar (make-timebar.js)
const BRIDGES = [['lions-gate', 'LIONS GATE'], ['ironworkers', 'IRONWORKERS']];

// Map framing: centred between the bridges, tight on the round screen's wide middle band
const MAP_CENTRE = [-123.0775, 49.315];
const PX_PER_DEG = 3680;
const SCREEN = 466;

const localParts = new Intl.DateTimeFormat('en-CA', {
  timeZone: TIME_ZONE, year: 'numeric', month: '2-digit', day: '2-digit',
  hour: '2-digit', minute: '2-digit', hourCycle: 'h23'
});

function localDayAndMinute(timestamp) {
  const parts = {};
  for (const p of localParts.formatToParts(new Date(timestamp))) parts[p.type] = p.value;
  return {
    day: `${parts.year}-${parts.month}-${parts.day}`,
    minute: Number(parts.hour) * 60 + Number(parts.minute)
  };
}

const mean = (xs) => xs.reduce((a, b) => a + b, 0) / xs.length;
const round = (x, places) => Math.round(x * 10 ** places) / 10 ** places;

// Intervals of one local day, bucketed into slots: Map(slot -> [interval, ...])
function bucketBySlot(intervals, day) {
  const buckets = new Map();
  for (const iv of intervals || []) {
    if (!iv || !iv.timestamp) continue;
    const { day: d, minute } = localDayAndMinute(iv.timestamp);
    if (d !== day) continue;
    const slot = Math.floor((minute - FIRST_SLOT_MINUTE) / SLOT_MINUTES);
    if (slot < 0 || slot >= SLOT_COUNT) continue;
    if (!buckets.has(slot)) buckets.set(slot, []);
    buckets.get(slot).push(iv);
  }
  return buckets;
}

const isRatio = (v) => typeof v === 'number' && Number.isFinite(v);

// Per bridge: mean over its segments, then over the slot's readings
function bridgeSeries(buckets, segments, bridgeId) {
  const ids = Object.entries(segments || {})
    .filter(([, s]) => s && s.bridgeHint === bridgeId)
    .map(([id]) => id);
  const out = new Array(SLOT_COUNT).fill(null);
  for (const [slot, ivs] of buckets) {
    const perReading = ivs
      .map((iv) => ids.map((id) => iv[id]).filter(isRatio))
      .filter((vals) => vals.length)
      .map(mean);
    if (perReading.length) out[slot] = round(mean(perReading), 3);
  }
  return out;
}

// The heat strip: share of every reported segment below SLOW_BELOW
function slowFractionSeries(buckets) {
  const out = new Array(SLOT_COUNT).fill(null);
  for (const [slot, ivs] of buckets) {
    const perReading = ivs.map((iv) => {
      const vals = Object.entries(iv).filter(([k, v]) => k !== 'timestamp' && isRatio(v)).map(([, v]) => v);
      return vals.length ? vals.filter((v) => v < SLOW_BELOW).length / vals.length : null;
    }).filter((v) => v !== null);
    if (perReading.length) out[slot] = round(mean(perReading), 4);
  }
  return out;
}

function averageSeries(seriesList, places) {
  const out = new Array(SLOT_COUNT).fill(null);
  for (let i = 0; i < SLOT_COUNT; i++) {
    const vals = seriesList.map((s) => s[i]).filter((v) => v !== null);
    if (vals.length) out[i] = round(mean(vals), places);
  }
  return out;
}

// ------------------------------------------------------------------ map
function project([lon, lat]) {
  const cos = Math.cos((MAP_CENTRE[1] * Math.PI) / 180);
  return [
    Math.round((lon - MAP_CENTRE[0]) * PX_PER_DEG * cos + SCREEN / 2),
    Math.round(SCREEN / 2 - (lat - MAP_CENTRE[1]) * PX_PER_DEG)
  ];
}

function simplify(pts, eps = 1.2) {
  if (pts.length < 3) return pts;
  const [x1, y1] = pts[0];
  const [x2, y2] = pts[pts.length - 1];
  const dx = x2 - x1, dy = y2 - y1, len = Math.hypot(dx, dy) || 1;
  let dmax = 0, idx = 0;
  for (let i = 1; i < pts.length - 1; i++) {
    const [x, y] = pts[i];
    const d = Math.abs(dy * x - dx * y + x2 * y1 - y2 * x1) / len;
    if (d > dmax) { dmax = d; idx = i; }
  }
  if (dmax > eps) return simplify(pts.slice(0, idx + 1), eps).slice(0, -1).concat(simplify(pts.slice(idx), eps));
  return [pts[0], pts[pts.length - 1]];
}

// The site's per-segment thresholds (app.js trafficThresholds)
function segmentLevel(r) {
  if (r === null) return '.';
  if (r < 0.26) return '3';
  if (r < 0.38) return '2';
  if (r < 0.48) return '1';
  return '0';
}

// Geometry depends only on the segment set, so it's computed once and reused
let geometryCache = { key: null, value: null };

function mapGeometry(segments) {
  const ids = Object.keys(segments || {}).sort();
  const key = ids.join(',');
  if (geometryCache.key === key) return geometryCache.value;
  const kept = [];
  for (const id of ids) {
    const s = segments[id];
    const coords = Array.isArray(s?.coordinates) ? s.coordinates : [];
    let pts = coords.map(project).filter((p, i, a) => i === 0 || p[0] !== a[i - 1][0] || p[1] !== a[i - 1][1]);
    if (pts.length < 2) continue;
    if (pts.every(([x, y]) => (x - SCREEN / 2) ** 2 + (y - SCREEN / 2) ** 2 > 240 ** 2)) continue;
    pts = simplify(pts);
    kept.push({ id, bridge: !!s.bridgeHint, pts: pts.flat() });
  }
  geometryCache = { key, value: kept };
  return kept;
}

function mapLevels(buckets, geometry) {
  return geometry.map(({ id }) => {
    let s = '';
    for (let slot = 0; slot < SLOT_COUNT; slot++) {
      const vals = (buckets.get(slot) || []).map((iv) => iv[id]).filter(isRatio);
      s += segmentLevel(vals.length ? mean(vals) : null);
    }
    return s;
  });
}

// ------------------------------------------------------------------ the feed
// usual: what usualFor() returned for this day (cached by the caller, once a day)
function usualFor(previousDays) {
  const days = previousDays.filter((p) => p.intervals && p.intervals.length);
  const bucketsList = days.map((p) => [bucketBySlot(p.intervals, p.day), p.segments]);
  return {
    days: days.map((p) => p.day),
    bridges: Object.fromEntries(BRIDGES.map(([id]) => [
      id, averageSeries(bucketsList.map(([b, segs]) => bridgeSeries(b, segs, id)), 3)
    ])),
    ring: averageSeries(bucketsList.map(([b]) => slowFractionSeries(b)), 4)
  };
}

function buildDeviceFeed({ day, weekday, intervals, segments, usual }) {
  const buckets = bucketBySlot(intervals, day);
  const geometry = mapGeometry(segments);
  const latest = (intervals || [])
    .map((iv) => iv && iv.timestamp)
    .filter((t) => t && localDayAndMinute(t).day === day)
    .sort()
    .pop() || null;
  return {
    schema: 1,
    day,
    weekday,
    slot_minutes: SLOT_MINUTES,
    first_slot_minute: FIRST_SLOT_MINUTE,
    bridges: BRIDGES.map(([id, name]) => ({
      id, name, today: bridgeSeries(buckets, segments, id), usual: usual.bridges[id]
    })),
    ring: { metric: 'slow_fraction', slow_below: SLOW_BELOW, today: slowFractionSeries(buckets), usual: usual.ring },
    usual_basis: `the last ${usual.days.length} ${weekday}s`,
    map: { version: 1, segments: geometry.map(({ bridge, pts }) => ({ bridge, pts })), levels: mapLevels(buckets, geometry) },
    generated_at: new Date().toISOString(),
    latest_reading: latest,
    now_minute: latest ? localDayAndMinute(latest).minute : null
  };
}

// The same weekday, 1-3 weeks back, as YYYY-MM-DD
function previousSameWeekdays(day, count = 3) {
  const [y, m, d] = day.split('-').map(Number);
  return Array.from({ length: count }, (_, i) => {
    const t = new Date(Date.UTC(y, m - 1, d - 7 * (i + 1)));
    return t.toISOString().slice(0, 10);
  });
}

function weekdayName(day) {
  const [y, m, d] = day.split('-').map(Number);
  return new Date(Date.UTC(y, m - 1, d)).toLocaleDateString('en-US', { weekday: 'long', timeZone: 'UTC' });
}

module.exports = {
  buildDeviceFeed, usualFor, previousSameWeekdays, weekdayName, localDayAndMinute,
  SLOT_MINUTES, SLOT_COUNT
};
