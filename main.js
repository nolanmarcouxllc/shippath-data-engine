import { Actor } from 'apify';

await Actor.init();

// ─────────────────────────────────────────────
// INPUT
// ─────────────────────────────────────────────
const input = await Actor.getInput() ?? {};
const {
    states = ['GA', 'TN', 'AR', 'MS'],
} = input;

// ─────────────────────────────────────────────
// REGION MAPS
// ─────────────────────────────────────────────
const HUB_MAP = {
    CA: 'LA/Inland Empire', NV: 'LA/Inland Empire',
    IL: 'Chicago', IN: 'Chicago', WI: 'Chicago',
    TX: 'Dallas-FW',
    GA: 'Atlanta',
    NY: 'NY/NJ', NJ: 'NY/NJ', CT: 'NY/NJ',
    WA: 'Seattle/Tacoma', OR: 'Seattle/Tacoma',
    TN: 'Memphis', AR: 'Memphis', MS: 'Memphis',
    FL: 'Miami/South FL',
};

const FREIGHT_MAP = {
    CA: 'West Coast', NV: 'West Coast',
    IL: 'Midwest', IN: 'Midwest', WI: 'Midwest',
    TX: 'Central',
    GA: 'Southeast',
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

// ─────────────────────────────────────────────
// SCRAPE EACH STATE
// ─────────────────────────────────────────────
const BASE = 'https://www.fsis.usda.gov/fsis/api/establishments/mpi';
const TODAY = new Date().toISOString().split('T')[0];
let totalRecords = 0;

for (const state of states) {
    console.log(`Scraping state: ${state}`);
    let pageIndex = 1;
    let hasMore = true;

    while (hasMore) {
        const url = `${BASE}?state=${state}&pageIndex=${pageIndex}&pageSize=100`;

        try {
            const response = await fetch(url, {
                headers: {
                    'Accept': 'application/json',
                    'User-Agent': 'Mozilla/5.0 (compatible; ShipPath/1.0)',
                }
            });

            if (!response.ok) {
                console.log(`Error ${response.status} for ${state} page ${pageIndex}`);
                hasMore = false;
                break;
            }

            const data = await response.json();
            const records = Array.isArray(data) ? data : (data.data ?? data.results ?? data.establishments ?? []);

            if (!records.length) {
                hasMore = false;
                break;
            }

            console.log(`${state} page ${pageIndex}: ${records.length} records`);

            for (const r of records) {
                const stateCode = r.state ?? state;
                const activities = Array.isArray(r.activities)
                    ? r.activities.join('; ')
                    : (r.activities ?? '');

                const record = {
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
                };

                await Actor.pushData(record);
                totalRecords++;
            }

            if (records.length < 100) {
                hasMore = false;
            } else {
                pageIndex++;
            }

        } catch (err) {
            console.log(`Failed on ${state} page ${pageIndex}: ${err.message}`);
            hasMore = false;
        }
    }
}

console.log(`✅ Done. Total records: ${totalRecords}`);
await Actor.exit();
