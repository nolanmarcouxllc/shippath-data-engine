import { Actor } from 'apify';
import { CheerioCrawler } from 'crawlee';

await Actor.init();

// ─────────────────────────────────────────────
// INPUT — comes from Apify or City Input List
// ─────────────────────────────────────────────
const input = await Actor.getInput() ?? {};
const {
    states = ['GA', 'TN', 'AR', 'MS'],  // Default: Savannah + Memphis states
    maxRequestsPerCrawl = 500,
} = input;

// ─────────────────────────────────────────────
// USDA FSIS API — public, no login required
// Returns all establishments by state
// ─────────────────────────────────────────────
const BASE_URL = 'https://efts.fsis.usda.gov/efts-web/better_search';

const REGION_MAP = {
    // States → Hub
    CA: 'LA/Inland Empire', NV: 'LA/Inland Empire',
    IL: 'Chicago', IN: 'Chicago', WI: 'Chicago',
    TX: 'Dallas-FW',
    GA: 'Atlanta',
    NY: 'NY/NJ', NJ: 'NY/NJ', CT: 'NY/NJ',
    WA: 'Seattle/Tacoma', OR: 'Seattle/Tacoma',
    TN: 'Memphis', AR: 'Memphis', MS: 'Memphis',
    FL: 'Miami/South FL',
};

const FREIGHT_REGION_MAP = {
    CA: 'West Coast', NV: 'West Coast',
    IL: 'Midwest', IN: 'Midwest', WI: 'Midwest',
    TX: 'Central',
    GA: 'Southeast',
    NY: 'Mid-Atlantic', NJ: 'Mid-Atlantic', CT: 'Mid-Atlantic',
    WA: 'Pacific NW', OR: 'Pacific NW',
    TN: 'Southeast', AR: 'Southeast', MS: 'Southeast',
    FL: 'Southeast',
};

const CENSUS_REGION_MAP = {
    CA: 'West', NV: 'West', WA: 'West', OR: 'West',
    IL: 'Midwest', IN: 'Midwest', WI: 'Midwest',
    TX: 'South', GA: 'South', TN: 'South',
    AR: 'South', MS: 'South', FL: 'South',
    NY: 'Northeast', NJ: 'Northeast', CT: 'Northeast',
};

// ─────────────────────────────────────────────
// BUILD URLS — one per state
// ─────────────────────────────────────────────
const startUrls = states.map(state => ({
    url: `${BASE_URL}?establishmentType=Meat%2C+Poultry%2C+Egg&state=${state}&pageNumber=1&numberOfElements=100`,
    userData: { state, page: 1 },
}));

const allResults = [];

// ─────────────────────────────────────────────
// CRAWLER
// ─────────────────────────────────────────────
const crawler = new CheerioCrawler({
    maxRequestsPerCrawl,

    async requestHandler({ request, $, crawler }) {
        const { state, page } = request.userData;

        // USDA returns JSON embedded in the page — parse it
        let data;
        try {
            const raw = $('body').text().trim();
            data = JSON.parse(raw);
        } catch (e) {
            console.log(`Could not parse JSON for ${state} page ${page}`);
            return;
        }

        const hits = data?.hits?.hits ?? [];
        const total = data?.hits?.total?.value ?? 0;

        console.log(`${state} page ${page}: ${hits.length} records (total: ${total})`);

        // ── PARSE EACH FACILITY ──
        for (const hit of hits) {
            const src = hit._source ?? {};

            const stateCode = src.state ?? state;
            const record = {
                company_name:               src.establishment_name ?? '',
                dba:                        src.dba_name ?? '',
                address_1:                  src.address ?? '',
                city:                       src.city ?? '',
                state:                      stateCode,
                zip:                        src.zip ?? '',
                county:                     src.county ?? '',
                latitude:                   src.latitude ?? '',
                longitude:                  src.longitude ?? '',
                usda_establishment_number:  src.establishment_number ?? '',
                activities:                 (src.activities ?? []).join('; '),
                facility_size:              src.size ?? '',
                phone:                      src.phone ?? '',
                commodity_type:             'Meat/Poultry/Egg',
                equipment_type:             'Reefer',
                does_slaughter:             src.activities?.includes('Slaughter') ? 'Yes' : 'No',
                ready_to_eat:               src.rte ? 'Yes' : 'No',
                processing_volume:          src.processing_volume_category ?? '',
                slaughter_volume:           src.slaughter_volume_category ?? '',
                hub:                        REGION_MAP[stateCode] ?? '',
                freight_region:             FREIGHT_REGION_MAP[stateCode] ?? '',
                census_region:              CENSUS_REGION_MAP[stateCode] ?? '',
                source:                     'USDA FSIS',
                run_date:                   new Date().toISOString().split('T')[0],
            };

            allResults.push(record);
            await Actor.pushData(record);
        }

        // ── PAGINATE — queue next page if more results exist ──
        const perPage = 100;
        const totalPages = Math.ceil(total / perPage);

        if (page < totalPages) {
            const nextPage = page + 1;
            await crawler.addRequests([{
                url: `${BASE_URL}?establishmentType=Meat%2C+Poultry%2C+Egg&state=${state}&pageNumber=${nextPage}&numberOfElements=${perPage}`,
                userData: { state, page: nextPage },
            }]);
        }
    },

    failedRequestHandler({ request }) {
        console.log(`Request failed: ${request.url}`);
    },
});

await crawler.run(startUrls);

console.log(`✅ Done. Total records scraped: ${allResults.length}`);

await Actor.exit();
