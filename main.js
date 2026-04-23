import { Actor } from 'apify';

await Actor.init();

/*
 * ═══════════════════════════════════════════════════════
 * API BEHAVIOR — VERIFIED FROM LIVE LOGS 2026-04-22
 * ═══════════════════════════════════════════════════════
 * Endpoint:    https://www.fsis.usda.gov/fsis/api/establishments/mpi
 * Records:     Returns ALL 7,166 records every call
 * Pagination:  BROKEN — filter client-side
 * State filter: BROKEN — filter client-side
 * Geolocation: Single string "lat, lon" — split on our end
 * Demographic: Pulled from GitHub, joined on establishment_number
 * ═══════════════════════════════════════════════════════
 */

// ── INPUT ──────────────────────────────────────────────
const input = await Actor.getInput() ?? {};
const rawStates = input.states ?? ['AL','AK','AZ','AR','CA','CO','CT','DE','FL','GA','HI','ID','IL','IN','IA','KS','KY','LA','ME','MD','MA','MI','MN','MS','MO','MT','NE','NV','NH','NJ','NM','NY','NC','ND','OH','OK','OR','PA','RI','SC','SD','TN','TX','UT','VT','VA','WA','WV','WI','WY'];
const states = (Array.isArray(rawStates) ? rawStates : rawStates.split(',')).map(s => String(s).trim().toUpperCase());
console.log(`Target states: ${states.join(', ')}`);

// ── REGION MAPS ────────────────────────────────────────
const HUB_MAP = {
    CA: 'LA/Inland Empire', NV: 'LA/Inland Empire',
    IL: 'Chicago', IN: 'Chicago', WI: 'Chicago',
    TX: 'Dallas-FW', GA: 'Atlanta',
    NY: 'NY/NJ', NJ: 'NY/NJ', CT: 'NY/NJ',
    WA: 'Seattle/Tacoma', OR: 'Seattle/Tacoma',
    TN: 'Memphis', AR: 'Memphis', MS: 'Memphis',
    FL: 'Miami/South FL',
};
const FREIGHT_MAP = {
    CA: 'West Coast', NV: 'West Coast',
    IL: 'Midwest', IN: 'Midwest', WI: 'Midwest',
    TX: 'Central', GA: 'Southeast',
    NY: 'Mid-Atlantic', NJ: 'Mid-Atlantic', CT: 'Mid-Atlantic',
    WA: 'Pacific NW', OR: 'Pacific NW',
    TN: 'Southeast', AR: 'Southeast', MS: 'Southeast',
    FL: 'Southeast',
};
const CENSUS_MAP = {
    CA: 'West', NV: 'West', WA: 'West', OR: 'West',
    IL: 'Midwest', IN: 'Midwest', WI: 'Midwest',
    TX: 'South', GA: 'South', TN: 'South',
    AR: 'South', MS: 'South', FL: 'South',
    NY: 'Northeast', NJ: 'Northeast', CT: 'Northeast',
};

const TODAY = new Date().toISOString().split('T')[0];

// ── STEP 1: LOAD DEMOGRAPHIC LOOKUP FROM GITHUB ───────
console.log('Loading demographic lookup from GitHub...');
const DEMO_URL = 'https://raw.githubusercontent.com/nolanmarcouxllc/shippath-data-engine/main/Dataset_Establishment_Demographic_Data.csv';

const demoMap = {};
try {
    const res = await fetch(DEMO_URL);
    if (!res.ok) throw new Error(`HTTP ${res.status}`);
    const text = await res.text();
    const lines = text.trim().split('\n');
    const headers = lines[0].split(',');

    for (let i = 1; i < lines.length; i++) {
        const vals = lines[i].split(',');
        const obj = {};
        headers.forEach((h, idx) => obj[h.trim()] = (vals[idx] ?? '').trim());
        const key = obj['establishment_number'];
        if (key) demoMap[key] = obj;
    }
    console.log(`Demographic lookup loaded: ${Object.keys(demoMap).length} records`);
} catch (err) {
    console.log(`WARNING: Could not load demographic data: ${err.message}`);
}

// ── STEP 2: PULL ALL USDA RECORDS ─────────────────────
console.log('Pulling all records from USDA FSIS API...');
let allRecords = [];
try {
    const res = await fetch('https://www.fsis.usda.gov/fsis/api/establishments/mpi', {
        headers: { 'Accept': 'application/json', 'User-Agent': 'Mozilla/5.0 (compatible; ShipPath/1.0)' }
    });
    if (!res.ok) throw new Error(`HTTP ${res.status}`);
    const data = await res.json();
    allRecords = Array.isArray(data) ? data : (data.data ?? data.results ?? data.establishments ?? []);
    console.log(`STEP 2 COMPLETE — Total records from API: ${allRecords.length}`);
} catch (err) {
    console.log(`API call failed: ${err.message}`);
    await Actor.exit();
}

// ── STEP 3: FILTER BY STATE ───────────────────────────
const stateSet = new Set(states);
const filtered = allRecords.filter(r => stateSet.has((r.state ?? '').toUpperCase()));
console.log(`STEP 3 COMPLETE — Records after filtering: ${filtered.length}`);
for (const s of states) {
    const count = filtered.filter(r => (r.state ?? '').toUpperCase() === s).length;
    if (count > 0) console.log(`  ${s}: ${count} records`);
}

// ── HELPER: get demographic value ─────────────────────
function getDemo(estNumber, field) {
    if (!estNumber) return '';
    // Try direct match first
    if (demoMap[estNumber]) return demoMap[estNumber][field] ?? '';
    // Try each part split by +
    for (const part of estNumber.split('+')) {
        if (demoMap[part]) return demoMap[part][field] ?? '';
    }
    return '';
}

function isYes(val) {
    return ['y', 'yes', '1', 'true'].includes((val ?? '').toLowerCase().trim());
}

function volumeLabel(val) {
    const v = parseFloat(val);
    if (v === 1) return 'Very Small';
    if (v === 2) return 'Small';
    if (v === 3) return 'Medium';
    if (v === 4) return 'Large';
    if (v === 5) return 'Very Large';
    return '';
}

function commodityDetail(estNumber) {
    const products = [];
    if (isYes(getDemo(estNumber, 'beef_processing')))    products.push('Beef');
    if (isYes(getDemo(estNumber, 'pork_processing')))    products.push('Pork');
    if (isYes(getDemo(estNumber, 'chicken_processing'))) products.push('Chicken');
    if (isYes(getDemo(estNumber, 'turkey_processing')))  products.push('Turkey');
    if (isYes(getDemo(estNumber, 'duck_processing')))    products.push('Duck');
    if (isYes(getDemo(estNumber, 'sheep_processing')))   products.push('Lamb/Sheep');
    if (isYes(getDemo(estNumber, 'goat_processing')))    products.push('Goat');
    if (isYes(getDemo(estNumber, 'egg_processing')))     products.push('Egg');
    return products.length > 0 ? products.join(', ') : 'Meat/Poultry/Egg';
}

// ── STEP 4: MAP + ENRICH + OUTPUT ─────────────────────
let enriched = 0;
for (const r of filtered) {
    const stateCode = (r.state ?? '').toUpperCase();
    const estNum = r.establishment_number ?? '';
    const activities = Array.isArray(r.activities) ? r.activities.join('; ') : (r.activities ?? '');
    const dba = Array.isArray(r.dbas) ? r.dbas.join('; ') : (r.dbas ?? r.dba ?? '');

    // Split geolocation
    let latitude = '', longitude = '';
    if (r.geolocation) {
        const parts = String(r.geolocation).split(',');
        latitude  = parts[0]?.trim() ?? '';
        longitude = parts[1]?.trim() ?? '';
    }

    const hasDemo = !!(demoMap[estNum] || estNum.split('+').some(p => demoMap[p]));
    if (hasDemo) enriched++;

    await Actor.pushData({
        'Company Name':     r.establishment_name ?? '',
        'DBA':              dba,
        'Address':          r.address ?? '',
        'City':             r.city ?? '',
        'State':            stateCode,
        'ZIP':              r.zip ?? '',
        'County':           r.county ?? '',
        'Latitude':         latitude,
        'Longitude':        longitude,
        'USDA Est #':       estNum,
        'Hub':              HUB_MAP[stateCode] ?? '',
        'Freight Region':   FREIGHT_MAP[stateCode] ?? '',
        'Census Region':    CENSUS_MAP[stateCode] ?? '',
        'Commodity Detail': commodityDetail(estNum),
        'Equipment Type':   'Reefer',
        'Facility Size':    r.size ?? '',
        'Slaughter?':       isYes(getDemo(estNum, 'slaughter')) ? 'Yes' : (activities.toLowerCase().includes('slaughter') ? 'Yes' : 'No'),
        'Ready-to-Eat?':    isYes(getDemo(estNum, 'rte_processing')) ? 'Yes' : 'No',
        'Beef':             isYes(getDemo(estNum, 'beef_processing')) ? 'Yes' : '',
        'Pork':             isYes(getDemo(estNum, 'pork_processing')) ? 'Yes' : '',
        'Chicken':          isYes(getDemo(estNum, 'chicken_processing')) ? 'Yes' : '',
        'Turkey':           isYes(getDemo(estNum, 'turkey_processing')) ? 'Yes' : '',
        'Processing Vol':   volumeLabel(getDemo(estNum, 'processing_volume_category')),
        'Slaughter Vol':    volumeLabel(getDemo(estNum, 'slaughter_volume_category')),
        'Source':           'USDA FSIS',
    });
}

console.log(`✅ DONE — ${filtered.length} records pushed. ${enriched} enriched with demographic data.`);
await Actor.exit();
