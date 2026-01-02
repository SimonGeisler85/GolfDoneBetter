/* GDB UK Golf Data Builder
Static site friendly builder that outputs JSON files for UK golf courses and driving ranges.

Run:
  node build-gdb-uk.js

Outputs in ./dist:
  - gdb_courses_uk.json (courses only)
  - gdb_driving_ranges_uk.json (driving ranges only, minimal fields)
  - gdb_courses_uk.england.json
  - gdb_courses_uk.scotland.json
  - gdb_courses_uk.wales.json
  - gdb_courses_uk.northern_ireland.json
  - gdb_courses_uk_index.json (counts, metadata)

Notes:
- Uses Overpass for OSM POI fetch
- Uses Nominatim reverse geocode to fill city, county, postcode
- Adds GDB opinion tags via deterministic heuristics
- Use overrides.json for manual fixes and additions
*/

import fs from "fs";
import path from "path";

const OUT_DIR = path.resolve("./dist");
if (!fs.existsSync(OUT_DIR)) fs.mkdirSync(OUT_DIR, { recursive: true });

const sleep = (ms) => new Promise((r) => setTimeout(r, ms));

const OVERPASS_ENDPOINTS = [
  "https://overpass-api.de/api/interpreter",
  "https://overpass.kumi.systems/api/interpreter",
  "https://overpass.openstreetmap.ru/api/interpreter",
];

async function overpass(query) {
  let lastErr = null;
  for (const ep of OVERPASS_ENDPOINTS) {
    try {
      const res = await fetch(ep, {
        method: "POST",
        headers: { "Content-Type": "application/x-www-form-urlencoded; charset=UTF-8" },
        body: "data=" + encodeURIComponent(query),
      });
      if (res.status === 429) { lastErr = new Error("Rate limited"); continue; }
      if (!res.ok) { lastErr = new Error("Overpass " + res.status); continue; }
      return await res.json();
    } catch (e) {
      lastErr = e;
    }
  }
  throw lastErr || new Error("Overpass failed");
}

function normStr(s) {
  return String(s || "")
    .toLowerCase()
    .replace(/&/g, " and ")
    .replace(/[^a-z0-9 ]+/g, " ")
    .replace(/\b(the|golf|club|course|links|park)\b/g, " ")
    .replace(/\s+/g, " ")
    .trim();
}

function havKm(aLat, aLng, bLat, bLng) {
  const R = 6371;
  const toRad = (d) => (d * Math.PI) / 180;
  const dLat = toRad(bLat - aLat);
  const dLng = toRad(bLng - aLng);
  const sLat1 = toRad(aLat);
  const sLat2 = toRad(bLat);
  const a =
    Math.sin(dLat / 2) ** 2 +
    Math.cos(sLat1) * Math.cos(sLat2) * Math.sin(dLng / 2) ** 2;
  return 2 * R * Math.asin(Math.min(1, Math.sqrt(a)));
}

function prefScore(it) {
  const t = it.tags || {};
  const base = it.osmType === "relation" ? 30 : it.osmType === "way" ? 20 : 10;
  const holesB = it.holes ? 3 : 0;
  const parB = it.par ? 1 : 0;
  const webB = (t.website || t["contact:website"]) ? 2 : 0;
  const phoneB = (t.phone || t["contact:phone"]) ? 1 : 0;
  const addrB = t["addr:city"] ? 2 : 0;
  return base + holesB + parB + webB + phoneB + addrB;
}

function dedupe(items) {
  const groups = new Map();
  for (const it of items) {
    const k = normStr(it.name);
    const arr = groups.get(k) || [];
    arr.push(it);
    groups.set(k, arr);
  }

  const out = [];
  for (const arr of groups.values()) {
    arr.sort((a, b) => prefScore(b) - prefScore(a));
    const kept = [];
    for (const it of arr) {
      const near = kept.some((k) => havKm(k.lat, k.lng, it.lat, it.lng) < 0.75);
      if (!near) kept.push(it);
      else {
        let bestIdx = -1, bestDist = 999;
        for (let i = 0; i < kept.length; i++) {
          const d = havKm(kept[i].lat, kept[i].lng, it.lat, it.lng);
          if (d < bestDist) { bestDist = d; bestIdx = i; }
        }
        if (bestIdx >= 0 && prefScore(it) > prefScore(kept[bestIdx]) + 2) kept[bestIdx] = it;
      }
    }
    out.push(...kept);
  }
  return out;
}

function pickTag(tags, keys) {
  for (const k of keys) {
    const v = tags?.[k];
    if (v) return String(v);
  }
  return "";
}

function computeFacilities(tags) {
  const fac = new Set();
  const golf = String(tags?.golf || "").toLowerCase();

  if (golf.includes("driving_range")) fac.add("driving_range");
  if (tags?.shop === "golf" || tags?.shop === "sports") fac.add("pro_shop");
  if (tags?.amenity === "restaurant") fac.add("restaurant");
  if (tags?.amenity === "bar" || tags?.amenity === "pub") fac.add("bar");
  if (tags?.amenity === "cafe") fac.add("cafe");

  if (tags?.buggy_rental || tags?.["golf:buggy"]) fac.add("buggy_hire");
  if (tags?.["golf:trolley"] || tags?.trolley_rental) fac.add("trolley_hire");
  if (tags?.["golf:club_rental"] || tags?.club_rental) fac.add("club_hire");

  if (tags?.["golf:practice"] || tags?.leisure === "pitch") fac.add("practice_area");

  return [...fac];
}

function classifyCourse(tags, name) {
  const t = tags || {};
  const nameL = String(name || "").toLowerCase();
  const golf = String(t.golf || "").toLowerCase();
  const leisure = String(t.leisure || "").toLowerCase();

  const isDrivingRange =
    golf === "driving_range" ||
    leisure === "driving_range" ||
    golf === "practice";

  const isCourse =
    t.leisure === "golf_course" ||
    t.golf === "course" ||
    (t.sport === "golf" && (t.leisure === "pitch" || t.landuse === "recreation_ground"));

  const isMini = ["miniature_golf", "adventure_golf", "disc_golf", "footgolf", "pitch_and_putt"].includes(golf);
  if (isMini) return { kind: "exclude" };
  if (isDrivingRange) return { kind: "driving_range" };
  if (isCourse) return { kind: "course" };

  if (nameL.includes("golf") && (nameL.includes("club") || nameL.includes("course"))) return { kind: "course" };
  return { kind: "unknown" };
}

function classifyType(tags, name) {
  const t = tags || {};
  const nameL = String(name || "").toLowerCase();
  const s = (String(t.surface || "") + " " + String(t["golf:type"] || "") + " " + String(t.description || "")).toLowerCase();

  const out = new Set();
  if (s.includes("links") || nameL.includes("links")) out.add("links");
  if (s.includes("heath")) out.add("heathland");
  if (s.includes("park")) out.add("parkland");
  if (s.includes("moor")) out.add("moorland");
  if (s.includes("down")) out.add("downland");

  if (nameL.includes("resort") || nameL.includes("hotel") || nameL.includes("country club") || t.tourism === "hotel") out.add("resort");

  if (!out.size) out.add("standard");
  return [...out];
}

function classifyAccess(tags, name) {
  const t = tags || {};
  const s = (
    String(t.access || "") + " " +
    String(t.membership || "") + " " +
    String(t.description || "") + " " +
    String(t.note || "") + " " +
    String(name || "")
  ).toLowerCase();

  if (s.includes("members only") || s.includes("private")) return ["members_only"];
  if (s.includes("visitors welcome") || s.includes("visitor") || s.includes("pay and play") || s.includes("public")) return ["visitors_welcome"];
  return ["unknown"];
}

function classifyDressCode(tags, name) {
  const s = (
    String(tags?.dress_code || "") + " " +
    String(tags?.["golf:dress_code"] || "") + " " +
    String(tags?.description || "") + " " +
    String(tags?.note || "") + " " +
    String(name || "")
  ).toLowerCase();

  if (s.includes("strict") || s.includes("jacket") || s.includes("tie")) return ["strict_golf_attire"];
  if (s.includes("smart") || s.includes("collar") || s.includes("tailored")) return ["smart_golf_attire"];
  if (s.includes("casual") || s.includes("relaxed")) return ["casual"];
  return ["smart_casual"];
}

function classifyPrice(tags, name) {
  const s = (
    String(tags?.greenfee || "") + " " +
    String(tags?.fee || "") + " " +
    String(tags?.description || "") + " " +
    String(name || "")
  ).toLowerCase();

  const m = s.match(/£\s*([0-9]{1,3})/);
  if (m) {
    const n = Number(m[1]);
    if (n <= 25) return ["value"];
    if (n <= 50) return ["mid"];
    if (n <= 90) return ["premium"];
    return ["luxury"];
  }

  if (s.includes("affordable") || s.includes("municipal") || s.includes("public")) return ["value", "mid"];
  if (s.includes("resort") || s.includes("championship")) return ["premium", "luxury"];
  return ["unknown"];
}

function classifyDifficulty(tags, name, holes) {
  const s = (
    String(tags?.handicap || "") + " " +
    String(tags?.["golf:handicap"] || "") + " " +
    String(tags?.description || "") + " " +
    String(tags?.note || "") + " " +
    String(name || "")
  ).toLowerCase();

  if (s.includes("handicap") || s.includes("hcp")) {
    const mm = s.match(/(?:max(?:imum)?\s*)?(?:handicap|hcp)\s*[:=]?\s*([0-9]{1,2})/);
    if (mm) {
      const h = Number(mm[1]);
      if (h <= 18) return ["hard", "low_handicap_friendly"];
      if (h <= 28) return ["medium", "intermediate_friendly"];
      return ["easy", "beginner_friendly"];
    }
    return ["medium", "hard"];
  }

  const nameL = String(name || "").toLowerCase();
  if (nameL.includes("championship")) return ["hard", "championship"];
  if ((holes || 18) >= 27) return ["medium", "hard"];
  return ["medium"];
}

function buildItem(el) {
  const tags = el.tags || {};
  const name = tags.name || tags["name:en"] || "unknown";
  const lat = el.lat ?? el.center?.lat ?? null;
  const lng = el.lon ?? el.center?.lon ?? null;

  const holesRaw = pickTag(tags, ["golf:holes", "holes", "golf_holes"]);
  const holesNum = holesRaw && /^\d+$/.test(holesRaw) ? Number(holesRaw) : null;

  const parRaw = pickTag(tags, ["golf:par", "par"]);
  const website = pickTag(tags, ["website", "contact:website", "url"]);
  const phone = pickTag(tags, ["phone", "contact:phone"]);

  return { osm: { id: el.id, type: el.type }, osmType: el.type, name, lat, lng, tags, holes: holesNum, par: parRaw || "", website, phone };
}

async function reverseGeocode(lat, lng) {
  const url = "https://nominatim.openstreetmap.org/reverse?format=jsonv2&zoom=18&addressdetails=1&lat=" +
    encodeURIComponent(lat) + "&lon=" + encodeURIComponent(lng);
  const res = await fetch(url, { headers: { "User-Agent": "GolfDoneBetterDataBuilder/1.0 (static-site project)" } });
  if (!res.ok) return null;
  return await res.json();
}

function buildAddress(tags, nominatim) {
  const a = {};
  const t = tags || {}; a.state = pickTag(t, ["addr:state"]);
  a.street = pickTag(t, ["addr:housenumber", "addr:street"]) ? `${pickTag(t,["addr:housenumber"])} ${pickTag(t,["addr:street"])}`.trim() : pickTag(t, ["addr:street"]);
  a.city = pickTag(t, ["addr:city", "addr:town", "addr:village"]);
  a.county = pickTag(t, ["addr:county"]);
  a.postcode = pickTag(t, ["addr:postcode"]);
  a.country = pickTag(t, ["addr:country"]) || "UK";

  if ((!a.city || !a.postcode || !a.county) && nominatim?.address) {
    const na = nominatim.address; a.state = a.state || na.state || na.state_district || "";
    a.city = a.city || na.city || na.town || na.village || na.hamlet || "";
    a.county = a.county || na.county || na.state_district || na.state || "";
    a.postcode = a.postcode || na.postcode || "";
    a.country = a.country || na.country || "UK";
    if (!a.street) a.street = na.road ? `${na.house_number || ""} ${na.road}`.trim() : "";
  }

  if (!a.city) a.city = "unknown";
  if (!a.county) a.county = "unknown";
  if (!a.postcode) a.postcode = "unknown";
  if (!a.street) a.street = "unknown";
  return a;
}

function nationFromAddress(addr) {
  const state = String(addr?.state || "").toLowerCase();
  const county = String(addr?.county || "").toLowerCase();
  const country = String(addr?.country || "").toLowerCase();

  if (state.includes("scotland") || county.includes("scotland") || country.includes("scotland")) return "scotland";
  if (state.includes("wales") || county.includes("wales") || country.includes("wales")) return "wales";
  if (state.includes("northern ireland") || county.includes("northern ireland") || country.includes("northern ireland")) return "northern_ireland";
  if (state.includes("england") || county.includes("england") || country.includes("england")) return "england";

  return "england";
}

function slugId(name, city, county) {
  const s = (name + " " + city + " " + county).toLowerCase()
    .replace(/&/g, " and ")
    .replace(/[^a-z0-9]+/g, "_")
    .replace(/^_+|_+$/g, "");
  return "uk_" + s.slice(0, 120);
}

function applyOverrides(item, overrides) {
  const k = item.id;
  const ov = overrides?.[k];
  if (!ov) return item;
  const out = { ...item, ...ov };
  if (ov.address) out.address = { ...item.address, ...ov.address };
  if (ov.links) out.links = { ...item.links, ...ov.links };
  return out;
}

async function main() {
  const query = `
[out:json][timeout:180];
area["ISO3166-1"="GB"][admin_level=2]->.uk;
(
  nwr["leisure"="golf_course"]["leisure"!="miniature_golf"]["golf"!="pitch_and_putt"]["golf"!="miniature_golf"]["golf"!="practice"]["golf"!="driving_range"]["golf"!="adventure_golf"]["golf"!="disc_golf"]["golf"!="footgolf"](area.uk);
  nwr["golf"="course"]["golf"!="pitch_and_putt"]["golf"!="miniature_golf"]["golf"!="practice"]["golf"!="driving_range"]["golf"!="adventure_golf"]["golf"!="disc_golf"]["golf"!="footgolf"](area.uk);
  nwr["golf"="driving_range"](area.uk);
  nwr["leisure"="driving_range"](area.uk);
);
out center tags;
`;

  console.log("Fetching UK golf features via Overpass...");
  const data = await overpass(query);
  const raw = (data.elements || []).map(buildItem).filter(x => x.lat && x.lng && x.name && x.name !== "unknown");
  console.log("Raw items:", raw.length);

  const deduped = dedupe(raw);
  console.log("After dedupe:", deduped.length);

  const overridesPath = path.resolve("./overrides.json");
  const overrides = fs.existsSync(overridesPath) ? JSON.parse(fs.readFileSync(overridesPath, "utf8")) : null;

  const cachePath = path.resolve("./nominatim_cache.json");
  const nomCache = fs.existsSync(cachePath) ? JSON.parse(fs.readFileSync(cachePath, "utf8")) : {};
  let cacheWrites = 0;

  const courses = [];
  const ranges = [];

  for (let i = 0; i < deduped.length; i++) {
    const it = deduped[i];
    const kind = classifyCourse(it.tags, it.name);
    if (kind.kind === "exclude" || kind.kind === "unknown") continue;

    const cacheKey = `${it.lat.toFixed(5)},${it.lng.toFixed(5)}`;
    let nom = nomCache[cacheKey] || null;

    if (!nom) {
      try {
        nom = await reverseGeocode(it.lat, it.lng);
        nomCache[cacheKey] = nom;
        cacheWrites++;
        if (cacheWrites % 25 === 0) fs.writeFileSync(cachePath, JSON.stringify(nomCache, null, 2), "utf8");
      } catch (e) {
        nom = null;
      }
      await sleep(1100);
    }

    const address = buildAddress(it.tags, nom);
    const nation = nationFromAddress(address);
    const id = slugId(it.name, address.city, address.county);

    const base = {
      id,
      name: it.name,
      kind: kind.kind,
      nation,
      address,
      links: {
        official: it.website || "unknown",
        affiliate: { provider: "unknown", url: "unknown" }
      },
      geo: { lat: it.lat, lng: it.lng },
      holes: it.holes ? [{ count: it.holes, label: "Main" }] : [],
      par: it.par || "unknown",
      course_type: kind.kind === "course" ? classifyType(it.tags, it.name) : [],
      access: kind.kind === "course" ? classifyAccess(it.tags, it.name) : ["unknown"],
      vibe: kind.kind === "course" ? ["friendly"] : [],
      dress_code: kind.kind === "course" ? classifyDressCode(it.tags, it.name) : [],
      difficulty: kind.kind === "course" ? classifyDifficulty(it.tags, it.name, it.holes) : [],
      facilities: kind.kind === "course" ? computeFacilities(it.tags) : [],
      extras: kind.kind === "course" ? ["drinking_unknown", "smoking_unknown"] : [],
      price_band: kind.kind === "course" ? classifyPrice(it.tags, it.name) : [],
      source: { osm: it.osm, has_addr_tags: Boolean(it.tags?.["addr:city"] || it.tags?.["addr:town"] || it.tags?.["addr:village"]) }
    };

    if (kind.kind === "course") {
      const nameL = it.name.toLowerCase();
      if (nameL.includes("municipal") || nameL.includes("public")) base.vibe.push("beginner_friendly", "relaxed");
      if (base.course_type.includes("resort")) base.vibe.push("premium");
      if (base.access.includes("members_only")) base.vibe.push("traditional");
      base.vibe = [...new Set(base.vibe)];
    }

    const final = applyOverrides(base, overrides);

    if (final.kind === "driving_range") {
      ranges.push({ id: final.id, name: final.name, nation: final.nation, address: final.address, links: final.links, geo: final.geo, kind: "driving_range" });
    } else {
      courses.push(final);
    }
  }

  fs.writeFileSync(cachePath, JSON.stringify(nomCache, null, 2), "utf8");

  const byNation = { england: [], scotland: [], wales: [], northern_ireland: [] };
  for (const c of courses) {
    const n = byNation[c.nation] ? c.nation : "england";
    byNation[n].push(c);
  }

  const meta = {
    schema_version: "gdb_courses_uk_v1",
    generated_utc: new Date().toISOString(),
    counts: {
      courses_total: courses.length,
      driving_ranges_total: ranges.length,
      by_nation: Object.fromEntries(Object.entries(byNation).map(([k,v]) => [k, v.length]))
    },
    notes: "Facts from OSM plus GDB opinion tags via heuristics. Use overrides.json for manual fixes."
  };

  fs.writeFileSync(path.join(OUT_DIR, "gdb_courses_uk.json"), JSON.stringify({ ...meta, courses }, null, 2), "utf8");
  fs.writeFileSync(path.join(OUT_DIR, "gdb_driving_ranges_uk.json"), JSON.stringify({ ...meta, driving_ranges: ranges }, null, 2), "utf8");

  for (const [nation, arr] of Object.entries(byNation)) {
    fs.writeFileSync(path.join(OUT_DIR, `gdb_courses_uk.${nation}.json`), JSON.stringify({ ...meta, nation, courses: arr }, null, 2), "utf8");
  }

  fs.writeFileSync(path.join(OUT_DIR, "gdb_courses_uk_index.json"), JSON.stringify(meta, null, 2), "utf8");

  console.log("Done.");
  console.log(meta.counts);
}

main().catch((e) => {
  console.error("Build failed:", e);
  process.exit(1);
});
