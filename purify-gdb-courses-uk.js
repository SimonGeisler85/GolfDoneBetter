/**
 * GDB Purity Pass v1
 * Purpose: classify and filter UK dataset so only true golf courses remain
 * Input:  ../data/gdb_courses_uk.json
 * Output: ../data/gdb_courses_uk.pure.json
 * Reports: ../enrichment/reports/*.json
 *
 * Notes:
 * - No external calls
 * - Deterministic rules
 * - Keeps ids unchanged
 */

const fs = require("fs");
const path = require("path");

const ROOT = path.resolve(__dirname, ".."); // Golf/
const IN_FILE = path.join(ROOT, "data", "gdb_courses_uk.json");
const OUT_FILE = path.join(ROOT, "data", "gdb_courses_uk.pure.json");

const REPORT_DIR = path.join(ROOT, "enrichment", "reports");
const REPORT_PURITY = path.join(REPORT_DIR, "purity_report.json");
const REPORT_EXCLUDED = path.join(REPORT_DIR, "purity_excluded.json");
const REPORT_MANUAL = path.join(REPORT_DIR, "purity_manual_review.json");

function ensureDir(p) {
  if (!fs.existsSync(p)) fs.mkdirSync(p, { recursive: true });
}

function normStr(s) {
  return String(s || "").trim().toLowerCase();
}

function hasDigits(s) {
  return /\d/.test(String(s || ""));
}

function isFiniteNumber(n) {
  return typeof n === "number" && Number.isFinite(n);
}

function ukPostcodeLooksValid(pc) {
  const s = String(pc || "").trim().toUpperCase();
  // Simple but effective UK postcode pattern
  // Examples: SW1A 1AA, M1 1AE, B33 8TH, CR2 6XH
  return /^[A-Z]{1,2}\d[A-Z\d]?\s?\d[A-Z]{2}$/.test(s);
}

function cleanExtras(extras) {
  const arr = Array.isArray(extras) ? extras.slice() : [];
  const drop = new Set(["drinking_unknown", "smoking_unknown"]);
  return arr.filter((x) => x && !drop.has(String(x)));
}

/**
 * Purity rules
 * Returns:
 * {
 *   entity_type: "course"|"not_course"|"closed_course",
 *   needs_manual_review: boolean,
 *   reason: string
 * }
 */
function classify(rec) {
  const name = String(rec?.name || "").trim();
  const n = normStr(name);

  // Geo sanity
  const lat = rec?.geo?.lat;
  const lng = rec?.geo?.lng;
  if (!isFiniteNumber(lat) || !isFiniteNumber(lng) || (lat === 0 && lng === 0)) {
    return { entity_type: "not_course", needs_manual_review: false, reason: "missing_or_bad_geo" };
  }

  // Closed markers in name
  if (
    /\bpermanently closed\b/i.test(name) ||
    /\bclosed\b/i.test(name) ||
    /\(closed\b/i.test(name)
  ) {
    return { entity_type: "closed_course", needs_manual_review: false, reason: "name_indicates_closed" };
  }

  // Hard excludes: clearly not a course
  const HARD_EXCLUDE_TOKENS = [
    "driving range",
    "topgolf",
    "simulator",
    "indoor golf",
    "golf studio",
    "trackman",
    "virtual golf",
    "adventure golf",
    "mini golf",
    "crazy golf",
    "footgolf",
    "disc golf",
  ];

  for (const tok of HARD_EXCLUDE_TOKENS) {
    if (n.includes(tok)) {
      return { entity_type: "not_course", needs_manual_review: false, reason: `hard_exclude_token:${tok}` };
    }
  }

  // Non venue heuristics
  // If it contains these and does not also contain strong golf markers, send to manual review
  const NON_VENUE_TOKENS = ["school", "college", "university"];
  const STRONG_MARKERS = ["golf club", "golf course", "golf links", "country club", "golf resort"];

  const hasStrongMarker = STRONG_MARKERS.some((m) => n.includes(m));
  const hasGolfWord = n.includes("golf");

  for (const tok of NON_VENUE_TOKENS) {
    if (n.includes(tok) && !hasStrongMarker) {
      return { entity_type: "not_course", needs_manual_review: false, reason: `non_venue_token:${tok}` };
    }
  }

  // Hard include if strong markers
  if (hasStrongMarker) {
    return { entity_type: "course", needs_manual_review: false, reason: "strong_name_marker" };
  }

  // Hard include if structured facts exist
  const holes = Array.isArray(rec?.holes) ? rec.holes : [];
  const par = String(rec?.par || "unknown");
  if ((holes && holes.length > 0) || (par !== "unknown" && hasDigits(par))) {
    return { entity_type: "course", needs_manual_review: false, reason: "has_course_facts" };
  }

  // Generic ambiguous course names: Main Course, Short Course, Academy Course, etc
  const GENERIC_AMBIGUOUS = [
    "main course",
    "short course",
    "academy course",
    "practice course",
    "pitch and putt",
    "par 3",
    "par3",
  ];
  if (GENERIC_AMBIGUOUS.some((g) => n === g || n.includes(g))) {
    // Might be part of a larger club or a small public course, mark for review
    return { entity_type: "course", needs_manual_review: true, reason: "generic_course_name" };
  }

  // If it contains golf but no strong marker, default to course but manual review
  if (hasGolfWord) {
    return { entity_type: "course", needs_manual_review: true, reason: "golf_word_without_strong_marker" };
  }

  // If no golf word at all, likely not a course
  return { entity_type: "not_course", needs_manual_review: false, reason: "no_golf_signal" };
}

function main() {
  if (!fs.existsSync(IN_FILE)) {
    console.error("Input file missing:", IN_FILE);
    process.exit(1);
  }

  ensureDir(REPORT_DIR);

  const raw = JSON.parse(fs.readFileSync(IN_FILE, "utf8"));

  const courses = Array.isArray(raw?.courses) ? raw.courses : [];
  const kept = [];
  const excluded = [];
  const manual = [];
  const closed = [];

  const breakdown = {};
  const inc = (k) => (breakdown[k] = (breakdown[k] || 0) + 1);

  for (const rec of courses) {
    const result = classify(rec);

    // Apply light sanitation (extras cleanup)
    const outRec = { ...rec };
    outRec.extras = cleanExtras(outRec.extras);

    // Add purity fields for auditability
    outRec.entity_type = result.entity_type;
    outRec.needs_manual_review = !!result.needs_manual_review;
    outRec.purity_reason = result.reason;

    if (result.entity_type === "course") {
      kept.push(outRec);
      if (result.needs_manual_review) manual.push({ id: outRec.id, name: outRec.name, reason: result.reason });
      inc(result.needs_manual_review ? "course_manual_review" : "course_kept");
    } else if (result.entity_type === "closed_course") {
      closed.push({ id: outRec.id, name: outRec.name, reason: result.reason });
      excluded.push({ id: outRec.id, name: outRec.name, entity_type: result.entity_type, reason: result.reason });
      inc("closed_course");
    } else {
      excluded.push({ id: outRec.id, name: outRec.name, entity_type: result.entity_type, reason: result.reason });
      inc(`excluded:${result.reason}`);
    }
  }

  // Optional: light validation stats for key identity fields
  let badPostcode = 0;
  let missingCity = 0;
  for (const c of kept) {
    const pc = c?.address?.postcode || "";
    const city = c?.address?.city || "";
    if (pc !== "unknown" && pc && !ukPostcodeLooksValid(pc)) badPostcode++;
    if (!city || city === "unknown") missingCity++;
  }

  const report = {
    summary: {
      total_records: courses.length,
      courses_kept: kept.length,
      excluded_total: excluded.length,
      closed_courses: closed.length,
      manual_review: manual.length,
    },
    quality_flags: {
      kept_bad_postcode_count: badPostcode,
      kept_missing_city_count: missingCity,
    },
    breakdown,
    sample: {
      excluded_first_25: excluded.slice(0, 25),
      manual_first_25: manual.slice(0, 25),
    },
  };

  // Build output meta
  const outMeta = {
    ...raw,
    schema_version: raw?.schema_version || "gdb_courses_uk_v1",
    purity_version: "gdb_purity_v1",
    purified_utc: new Date().toISOString(),
    counts: {
      ...(raw?.counts || {}),
      courses_total_original: courses.length,
      courses_total_pure: kept.length,
      excluded_total: excluded.length,
      closed_total: closed.length,
      manual_review_total: manual.length,
    },
    notes: String(raw?.notes || "") + " | Purity pass applied: driving ranges and non courses removed from site output.",
    courses: kept,
  };

  fs.writeFileSync(OUT_FILE, JSON.stringify(outMeta, null, 2), "utf8");
  fs.writeFileSync(REPORT_PURITY, JSON.stringify(report, null, 2), "utf8");
  fs.writeFileSync(REPORT_EXCLUDED, JSON.stringify(excluded, null, 2), "utf8");
  fs.writeFileSync(REPORT_MANUAL, JSON.stringify(manual, null, 2), "utf8");

  console.log("Purity pass complete");
  console.log("Input records:", courses.length);
  console.log("Kept courses:", kept.length);
  console.log("Excluded:", excluded.length);
  console.log("Closed:", closed.length);
  console.log("Manual review:", manual.length);
  console.log("Wrote:", OUT_FILE);
  console.log("Report:", REPORT_PURITY);
}

main();
