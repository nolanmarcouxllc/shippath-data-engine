import { Actor } from 'apify';

await Actor.init();

/*
 * ═══════════════════════════════════════════════════════
 * API BEHAVIOR — VERIFIED FROM LIVE LOGS 2026-04-22
 * ═══════════════════════════════════════════════════════
 * Endpoint:    https://www.fsis.usda.gov/fsis/api/establishments/mpi
 * Method:      GET, no auth required
 * Records:     Returns ALL 7,166 records every call regardless of filters
 * Pagination:  BROKEN — page 1, 2, 3 all return same 7,166 records
 * State filter: BROKEN server-side — must filter client-side after pull
 * Strategy:    Pull once. Filter on our end. Done.
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

    console.log(`STEP 1 COMPLETE — Total records received from API: ${allRecords.length}`);

    // Print first record's field names so we always know the schema
    if (allRecords.length > 0) {
        console.log(`API field names: ${Object.keys(allRecords[0]).join(', ')}`);
        console.log(`Sample record: ${JSON.stringify(allRecords[0], null, 2)}`);
    }

} catch (err) {
    console.log(`API call failed: ${err.message}`);
    await Actor.exit();
}

// ── STEP 2: FILTER CLIENT-SIDE BY STATE ───────────────
const stateSet = new Set(states);
const filtered = allRecords.filter(r => stateSet.has((r.state ?? '').toUpperCase()));

console.log(`STEP 2 COMPLETE — Records after filtering for [${states.join(', ')}]: ${filtered.length}`);

// Break down by state
for (const s of states) {
    const count = filtered.filter(r => (r.state ?? '').toUpperCase() === s).length;
    console.log(`  ${s}: ${count} records`);
}

if (filtered.length === 0) {
    console.log('WARNING: 0 records after filter. Check that state field name matches.');
    console.log('Available state values in first 10 records:');
    allRecords.slice(0, 10).forEach(r => console.log(`  state field: "${r.state}"`));
}

// ── STEP 3: MAP TO SHIPPATH SCHEMA AND OUTPUT ─────────
for (const r of filtered) {
    const stateCode = (r.state ?? '').toUpperCase();
    const activities = Array.isArray(r.activities)
        ? r.activities.join('; ')
        : (r.activities ?? '');

    await Actor.pushData({
        company_name:              r.establishment_name ?? r.name ?? '',
        dba:                       r.dba_name ?? r.dba ?? '',
        address_1:                 r.address ?? r.street ?? '',
        city:                      r.city ?? '',
        state:                     stateCode,
        zip:                       r.zip ?? r.postal_code ?? '',
        county:                    r.county ?? '',
        latitude:                  r.latitude ?? '',
        longitude:                 r.longitude ?? '',
        usda_establishment_number: r.establishment_number ?? r.est_number ?? '',
        activities:                activities,
        facility_size:             r.size ?? r.facility_size ?? '',
        phone:                     r.phone ?? '',
        commodity_type:            'Meat/Poultry/Egg',
        equipment_type:            'Reefer',
        does_slaughter:            activities.toLowerCase().includes('slaughter') ? 'Yes' : 'No',
        ready_to_eat:              r.rte ? 'Yes' : 'No',
        processing_volume:         r.processing_volume_category ?? '',
        slaughter_volume:          r.slaughter_volume_category ?? '',
        hub:                       HUB_MAP[stateCode] ?? '',
        freight_region:            FREIGHT_MAP[stateCode] ?? '',
        census_region:             CENSUS_MAP[stateCode] ?? '',
        source:                    'USDA FSIS',
        run_date:                  TODAY,
    });
}

console.log(`✅ DONE — ${filtered.length} records pushed to Apify dataset.`);
await Actor.exit();
