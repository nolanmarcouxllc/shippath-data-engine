import { Actor } from 'apify';

await Actor.init();

/*
 * ═══════════════════════════════════════════════════════
 * API BEHAVIOR — VERIFIED FROM LIVE LOGS 2026-04-22
 * ═══════════════════════════════════════════════════════
 * Endpoint:    https://www.fsis.usda.gov/fsis/api/establishments/mpi
 * Method:      GET, no auth required
 * Records:     Returns ALL 7,166 records every call regardless of filters
 * Pagination:  BROKEN — all pages return same 7,166 records
 * State filter: BROKEN server-side — filter client-side after pull
 * Geolocation: Returned as single string "lat, lon" — must split
 * Strategy:    Pull once. Filter on our end. Split geolocation. Done.
 * ═══════════════════════════════════════════════════════
 * OUTPUT COLUMN ORDER — matches ShipPath Master Warehouse exactly:
 * Company Name, DBA, Address, City, State, ZIP, County,
 * Latitude, Longitude, USDA Est #, Hub, Freight Region,
 * Census Region, Commodity Detail, Equipment Type, Facility Size,
 * Slaughter?, Ready-to-Eat?, Beef, Pork, Chicken, Turkey,
 * Processing Vol, Slaughter Vol, Source
 * ═══════════════════════════════════════════════════════
 */

// ── INPUT ──────────────────────────────────────────────
const input = await Actor.getInput() ?? {};
const states = (input.states ?? ['GA', 'TN', 'AR', 'MS']).map(s => s.toUpperCase());
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

// ── STEP 1: PULL ALL RECORDS ONCE ─────────────────────
console.log('Pulling all records from USDA FSIS API...');

let allRecords = [];
try {
    const response = await fetch('https://www.fsis.usda.gov/fsis/api/establishments/mpi', {
        headers: {
            'Accept': 'application/json',
            'User-Agent': 'Mozilla/5.0 (compatible; ShipPath/1.0)',
        }
    });

    if (!response.ok) throw new Error(`HTTP ${response.status}`);

    const data = await response.json();
    allRecords = Array.isArray(data) ? data : (data.data ?? data.results ?? data.establishments ?? []);
    console.log(`STEP 1 COMPLETE — Total records from API: ${allRecords.length}`);

} catch (err) {
    console.log(`API call failed: ${err.message}`);
    await Actor.exit();
}

// ── STEP 2: FILTER CLIENT-SIDE BY STATE ───────────────
const stateSet = new Set(states);
const filtered = allRecords.filter(r => stateSet.has((r.state ?? '').toUpperCase()));
console.log(`STEP 2 COMPLETE — Records after filtering: ${filtered.length}`);
for (const s of states) {
    const count = filtered.filter(r => (r.state ?? '').toUpperCase() === s).length;
    console.log(`  ${s}: ${count} records`);
}

// ── STEP 3: MAP TO SHIPPATH SCHEMA ────────────────────
// Output columns match Google Sheet Master Warehouse exactly
for (const r of filtered) {
    const stateCode = (r.state ?? '').toUpperCase();
    const activities = Array.isArray(r.activities)
        ? r.activities.join('; ')
        : (r.activities ?? '');
    const dba = Array.isArray(r.dbas)
        ? r.dbas.join('; ')
        : (r.dbas ?? r.dba ?? '');

    // Split geolocation "lat, lon" into two fields
    let latitude = '';
    let longitude = '';
    if (r.geolocation) {
        const parts = String(r.geolocation).split(',');
        latitude  = parts[0]?.trim() ?? '';
        longitude = parts[1]?.trim() ?? '';
    }

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
        'USDA Est #':       r.establishment_number ?? '',
        'Hub':              HUB_MAP[stateCode] ?? '',
        'Freight Region':   FREIGHT_MAP[stateCode] ?? '',
        'Census Region':    CENSUS_MAP[stateCode] ?? '',
        'Commodity Detail': 'Meat/Poultry/Egg',
        'Equipment Type':   'Reefer',
        'Facility Size':    r.size ?? '',
        'Slaughter?':       activities.toLowerCase().includes('slaughter') ? 'Yes' : 'No',
        'Ready-to-Eat?':    '',
        'Beef':             '',
        'Pork':             '',
        'Chicken':          '',
        'Turkey':           '',
        'Processing Vol':   r.processing_volume_category ?? '',
        'Slaughter Vol':    r.slaughter_volume_category ?? '',
        'Source':           'USDA FSIS',
    });
}

console.log(`✅ DONE — ${filtered.length} records pushed.`);
await Actor.exit();
