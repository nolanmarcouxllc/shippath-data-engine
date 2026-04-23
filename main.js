/**
 * ShipPath Data Engine — USDA Enrichment Actor
 * 
 * WHAT THIS DOES:
 * 1. Pulls MPI Directory (establishment addresses, coords, activities)
 * 2. Pulls Demographic Data (slaughter/processing flags, volume, species)
 * 3. Joins on establishment_number
 * 4. Tags every record with hub + freight_region + census_region
 * 5. Writes enriched records to Google Sheets (Master Warehouse tab)
 * 
 * INPUTS (set in Apify actor input):
 *   - googleSheetsId: your Google Sheet ID
 *   - googleServiceAccountJson: service account credentials (store as Apify secret)
 *   - stateFilter: optional 2-letter state code to filter (e.g. "CA") — leave blank for all
 * 
 * DEPLOY:
 *   apify push  (from repo root)
 */

import { Actor } from 'apify';
import { parse } from 'csv-parse/sync';
import { google } from 'googleapis';

// ─── RAW FILE URLS ────────────────────────────────────────────────────────────

const MPI_URL =
  'https://raw.githubusercontent.com/nolanmarcouxllc/shippath-data-engine/refs/heads/main/MPI_Directory_by_Establishment_Number.csv';

const DEMO_URL =
  'https://raw.githubusercontent.com/nolanmarcouxllc/shippath-data-engine/refs/heads/main/Dataset_Establishment_Demographic_Data.csv';

// ─── HUB + REGION TAGGING ────────────────────────────────────────────────────
// Maps state → hub, freight_region, census_region
// Every record gets all three tags so brokers can search any way they want

const STATE_TAGS = {
  CA: { hub: 'LA/Inland Empire', freight_region: 'West Coast',    census_region: 'West'     },
  NV: { hub: 'LA/Inland Empire', freight_region: 'Mountain',      census_region: 'West'     },
  AZ: { hub: 'LA/Inland Empire', freight_region: 'Mountain',      census_region: 'West'     },
  IL: { hub: 'Chicago',          freight_region: 'Midwest',       census_region: 'Midwest'  },
  IN: { hub: 'Chicago',          freight_region: 'Midwest',       census_region: 'Midwest'  },
  WI: { hub: 'Chicago',          freight_region: 'Midwest',       census_region: 'Midwest'  },
  TX: { hub: 'Dallas-FW',        freight_region: 'Southwest',     census_region: 'South'    },
  OK: { hub: 'Dallas-FW',        freight_region: 'Southwest',     census_region: 'South'    },
  AR: { hub: 'Dallas-FW',        freight_region: 'Southwest',     census_region: 'South'    },
  GA: { hub: 'Atlanta',          freight_region: 'Southeast',     census_region: 'South'    },
  AL: { hub: 'Atlanta',          freight_region: 'Southeast',     census_region: 'South'    },
  SC: { hub: 'Atlanta',          freight_region: 'Southeast',     census_region: 'South'    },
  NJ: { hub: 'NY/NJ',            freight_region: 'Mid-Atlantic',  census_region: 'Northeast'},
  NY: { hub: 'NY/NJ',            freight_region: 'Mid-Atlantic',  census_region: 'Northeast'},
  CT: { hub: 'NY/NJ',            freight_region: 'New England',   census_region: 'Northeast'},
  LA: { hub: 'Houston',          freight_region: 'Southwest',     census_region: 'South'    },
  MS: { hub: 'Houston',          freight_region: 'Southeast',     census_region: 'South'    },
  SC: { hub: 'Savannah',         freight_region: 'Southeast',     census_region: 'South'    },
  NC: { hub: 'Savannah',         freight_region: 'Southeast',     census_region: 'South'    },
  WA: { hub: 'Seattle/Tacoma',   freight_region: 'Pacific NW',    census_region: 'West'     },
  OR: { hub: 'Seattle/Tacoma',   freight_region: 'Pacific NW',    census_region: 'West'     },
  TN: { hub: 'Memphis',          freight_region: 'Southeast',     census_region: 'South'    },
  KY: { hub: 'Memphis',          freight_region: 'Midwest',       census_region: 'South'    },
  FL: { hub: 'Miami/South FL',   freight_region: 'Southeast',     census_region: 'South'    },
  // Remaining states — default tags
  MN: { hub: 'Chicago',          freight_region: 'Midwest',       census_region: 'Midwest'  },
  IA: { hub: 'Chicago',          freight_region: 'Midwest',       census_region: 'Midwest'  },
  MO: { hub: 'Chicago',          freight_region: 'Midwest',       census_region: 'Midwest'  },
  OH: { hub: 'Chicago',          freight_region: 'Midwest',       census_region: 'Midwest'  },
  MI: { hub: 'Chicago',          freight_region: 'Midwest',       census_region: 'Midwest'  },
  KS: { hub: 'Dallas-FW',        freight_region: 'Central',       census_region: 'Midwest'  },
  NE: { hub: 'Dallas-FW',        freight_region: 'Central',       census_region: 'Midwest'  },
  CO: { hub: 'Dallas-FW',        freight_region: 'Mountain',      census_region: 'West'     },
  PA: { hub: 'NY/NJ',            freight_region: 'Mid-Atlantic',  census_region: 'Northeast'},
  MD: { hub: 'NY/NJ',            freight_region: 'Mid-Atlantic',  census_region: 'South'    },
  VA: { hub: 'NY/NJ',            freight_region: 'Mid-Atlantic',  census_region: 'South'    },
  MA: { hub: 'NY/NJ',            freight_region: 'New England',   census_region: 'Northeast'},
  DE: { hub: 'NY/NJ',            freight_region: 'Mid-Atlantic',  census_region: 'South'    },
  WV: { hub: 'NY/NJ',            freight_region: 'Mid-Atlantic',  census_region: 'South'    },
};

function getTags(state) {
  return STATE_TAGS[state] || {
    hub: 'Other',
    freight_region: 'Other',
    census_region: 'Other',
  };
}

// ─── DEMOGRAPHIC COLUMNS TO KEEP ─────────────────────────────────────────────

const DEMO_COLUMNS = [
  'establishment_number',
  'active_meat_grant',
  'active_poultry_grant',
  'active_egg_grant',
  'active_voluntary_grant',
  'slaughter',
  'processing',
  'rte_processing',
  'nrte_processing',
  'raw_intact_processing',
  'beef_cow_slaughter',
  'market_swine_slaughter',
  'young_chicken_slaughter',
  'young_turkey_slaughter',
  'lamb_slaughter',
  'processing_volume_category',
  'slaughter_volume_category',
  'listeria_alternative',
];

// ─── OUTPUT SHEET COLUMNS (in order) ─────────────────────────────────────────

const SHEET_HEADERS = [
  'establishment_number',
  'establishment_name',
  'dba',
  'street',
  'city',
  'state',
  'zip',
  'county',
  'phone',
  'latitude',
  'longitude',
  'activities',
  'size',
  'grant_date',
  'hub',
  'freight_region',
  'census_region',
  'active_meat_grant',
  'active_poultry_grant',
  'active_egg_grant',
  'active_voluntary_grant',
  'slaughter',
  'processing',
  'rte_processing',
  'nrte_processing',
  'raw_intact_processing',
  'beef_cow_slaughter',
  'market_swine_slaughter',
  'young_chicken_slaughter',
  'young_turkey_slaughter',
  'lamb_slaughter',
  'processing_volume_category',
  'slaughter_volume_category',
  'listeria_alternative',
  'run_date',
];

// ─── HELPERS ──────────────────────────────────────────────────────────────────

async function fetchCsv(url, label) {
  console.log(`[FETCH] Pulling ${label}...`);
  const res = await fetch(url);
  if (!res.ok) throw new Error(`Failed to fetch ${label}: ${res.status} ${res.statusText}`);
  const text = await res.text();
  const records = parse(text, { columns: true, skip_empty_lines: true, trim: true });
  console.log(`[FETCH] ${label} — ${records.length} records received`);
  return records;
}

function buildLookup(records, keyField) {
  const map = {};
  for (const r of records) {
    const key = (r[keyField] || '').trim().toUpperCase();
    if (key) map[key] = r;
  }
  return map;
}

function pickDemoFields(demoRow) {
  const out = {};
  for (const col of DEMO_COLUMNS) {
    out[col] = demoRow ? (demoRow[col] || '') : '';
  }
  return out;
}

function buildOutputRow(mpi, demo, tags) {
  const d = pickDemoFields(demo);
  const today = new Date().toISOString().split('T')[0];
  return [
    mpi.establishment_number || '',
    mpi.establishment_name   || '',
    mpi.dbas                 || '',
    mpi.street               || '',
    mpi.city                 || '',
    mpi.state                || '',
    mpi.zip                  || '',
    mpi.county               || '',
    mpi.phone                || '',
    mpi.latitude             || '',
    mpi.longitude            || '',
    mpi.activities           || '',
    mpi.size                 || '',
    mpi.grant_date           || '',
    tags.hub,
    tags.freight_region,
    tags.census_region,
    d.active_meat_grant,
    d.active_poultry_grant,
    d.active_egg_grant,
    d.active_voluntary_grant,
    d.slaughter,
    d.processing,
    d.rte_processing,
    d.nrte_processing,
    d.raw_intact_processing,
    d.beef_cow_slaughter,
    d.market_swine_slaughter,
    d.young_chicken_slaughter,
    d.young_turkey_slaughter,
    d.lamb_slaughter,
    d.processing_volume_category,
    d.slaughter_volume_category,
    d.listeria_alternative,
    today,
  ];
}

async function writeToSheets(auth, sheetId, rows) {
  const sheets = google.sheets({ version: 'v4', auth });
  const sheetName = 'Master Warehouse';

  // Clear existing data first
  console.log('[SHEETS] Clearing existing data...');
  await sheets.spreadsheets.values.clear({
    spreadsheetId: sheetId,
    range: `${sheetName}!A:AM`,
  });

  // Write headers + data in one call
  const values = [SHEET_HEADERS, ...rows];
  console.log(`[SHEETS] Writing ${rows.length} records + header row...`);
  await sheets.spreadsheets.values.update({
    spreadsheetId: sheetId,
    range: `${sheetName}!A1`,
    valueInputOption: 'RAW',
    requestBody: { values },
  });

  console.log(`[SHEETS] Done — ${rows.length} records written to "${sheetName}"`);
}

// ─── MAIN ─────────────────────────────────────────────────────────────────────

Actor.main(async () => {
  const input = await Actor.getInput();
  const {
    googleSheetsId,
    googleServiceAccountJson,
    stateFilter = '',   // optional: "CA", "TX", etc. — blank = all states
  } = input || {};

  if (!googleSheetsId)          throw new Error('INPUT ERROR: googleSheetsId is required');
  if (!googleServiceAccountJson) throw new Error('INPUT ERROR: googleServiceAccountJson is required');

  // ── STEP 1: Fetch both files ────────────────────────────────────────────────
  const [mpiRecords, demoRecords] = await Promise.all([
    fetchCsv(MPI_URL,  'MPI Directory'),
    fetchCsv(DEMO_URL, 'Demographic Data'),
  ]);

  // ── STEP 2: Build demographic lookup by establishment_number ────────────────
  console.log('[JOIN] Building demographic lookup...');
  const demoLookup = buildLookup(demoRecords, 'establishment_number');
  console.log(`[JOIN] Lookup built — ${Object.keys(demoLookup).length} unique establishment numbers`);

  // ── STEP 3: Filter MPI by state if requested ────────────────────────────────
  const filtered = stateFilter
    ? mpiRecords.filter(r => (r.state || '').trim().toUpperCase() === stateFilter.toUpperCase())
    : mpiRecords;

  console.log(
    stateFilter
      ? `[FILTER] State filter "${stateFilter}" applied — ${filtered.length} records remaining`
      : `[FILTER] No state filter — processing all ${filtered.length} records`
  );

  // ── STEP 4: Join + tag every record ─────────────────────────────────────────
  console.log('[JOIN] Joining MPI + demographic data...');
  let matchCount = 0;
  let noMatchCount = 0;
  const outputRows = [];

  for (const mpi of filtered) {
    const estNum = (mpi.establishment_number || '').trim().toUpperCase();
    const demo   = demoLookup[estNum];
    const tags   = getTags((mpi.state || '').trim().toUpperCase());

    if (demo) matchCount++;
    else noMatchCount++;

    outputRows.push(buildOutputRow(mpi, demo, tags));
  }

  console.log(`[JOIN] Complete — ${matchCount} matched, ${noMatchCount} no demographic match`);
  console.log(`[JOIN] Total output rows: ${outputRows.length}`);

  // ── STEP 5: Auth + write to Google Sheets ───────────────────────────────────
  console.log('[SHEETS] Authenticating with Google...');
  const serviceAccount = typeof googleServiceAccountJson === 'string'
    ? JSON.parse(googleServiceAccountJson)
    : googleServiceAccountJson;

  const auth = new google.auth.GoogleAuth({
    credentials: serviceAccount,
    scopes: ['https://www.googleapis.com/auth/spreadsheets'],
  });

  await writeToSheets(await auth.getClient(), googleSheetsId, outputRows);

  // ── STEP 6: Push summary to Apify dataset for logging ───────────────────────
  await Actor.pushData({
    run_date: new Date().toISOString(),
    state_filter: stateFilter || 'ALL',
    mpi_records_fetched: mpiRecords.length,
    demo_records_fetched: demoRecords.length,
    records_after_filter: filtered.length,
    demographic_matches: matchCount,
    demographic_no_match: noMatchCount,
    rows_written_to_sheets: outputRows.length,
  });

  console.log('[DONE] ShipPath enrichment actor complete.');
});
