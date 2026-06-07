#!/usr/bin/env bash
# Read-only probes for the A2 investigation: can FDA recall press-release URLs (or a
# presence flag) be obtained in BULK from a source OTHER than the per-event iRES GET
# /search/pressreleaseurls/{eventid}?  All commands below are public, unauthenticated,
# read-only GETs (no FDA iRES API calls, no DB, no side effects). Run them yourself to
# reproduce the findings in the A2 report.
#
# Verdict (see report): NO bulk source carries the PR URL joinable to RECALLEVENTID.
# openFDA enforcement IS bulk + joinable on event_id/recall_number but has NO PR URL.
# The fda.gov recalls JSON feed HAS PR URLs but NO join key and only ~3yr of history.
#
# Usage:  bash scripts/fda_press_releases/alt_source_probes/probe_alt_pr_sources.sh
set -euo pipefail
UA='Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0 Safari/537.36'
OUT="${TMPDIR:-/tmp}/a2_probes"
mkdir -p "$OUT"

echo "== 1. openFDA enforcement record shape (does it carry a PR URL?) =="
for noun in food drug device; do
  echo "--- ${noun}/enforcement (1 record) ---"
  curl -s -A "$UA" "https://api.fda.gov/${noun}/enforcement.json?limit=1" \
    | python3 -c 'import sys,json;r=json.load(sys.stdin)["results"][0];print(json.dumps(r,indent=2))'
done

echo "== 2. openFDA field list — confirm NO url/press field (food enforcement YAML) =="
curl -s -A "$UA" "https://open.fda.gov/fields/foodenforcement.yaml" -o "$OUT/foodenforcement.yaml"
python3 - "$OUT/foodenforcement.yaml" <<'PY'
import sys,yaml
d=yaml.safe_load(open(sys.argv[1]))
print("top-level fields:", sorted(d["properties"]))
print("openfda sub-fields:", sorted(d["properties"]["openfda"]["properties"]))
t=open(sys.argv[1]).read().lower()
print("contains 'url'  field:", 'url:' in t)
print("contains 'press':", 'press' in t)
PY

echo "== 3. openFDA enforcement corpus sizes (product-grain, group by event_id) =="
for noun in food drug device; do
  total=$(curl -s -A "$UA" "https://api.fda.gov/${noun}/enforcement.json?limit=1" \
    | python3 -c 'import sys,json;print(json.load(sys.stdin)["meta"]["results"]["total"])')
  echo "  ${noun}/enforcement total: ${total}"
done
echo "  (no /animalandveterinary/enforcement endpoint exists — only adverse-event reports)"

echo "== 4. FDA Data Dashboard API schema — confirm NO recalls endpoint =="
curl -s -A "$UA" "https://datadashboard.fda.gov/oii/api/ddapi.json" -o "$OUT/ddapi.json"
python3 -c 'import json;print("DDAPI paths:",list(json.load(open("'"$OUT"'/ddapi.json"))["paths"]))'

echo "== 5. fda.gov recalls datatables JSON feed — HAS PR url (path), NO join key =="
curl -s -A "$UA" "https://www.fda.gov/datatables-json/recalls-market-withdrawals.json" -o "$OUT/recalls_feed.json"
python3 - "$OUT/recalls_feed.json" <<'PY'
import sys,json,re
rows=json.load(open(sys.argv[1]))
print("rows:",len(rows),"| keys:",list(rows[0]))
blob=json.dumps(rows)
print("has recall_number (X-NNNN-YYYY):", bool(re.search(r'[A-Z]-\d{3,4}-20\d\d',blob)))
print("has event_id key:", 'event_id' in blob.lower())
from collections import Counter
yc=Counter(r["field_change_date_2"].split("/")[-1] for r in rows if r.get("field_change_date_2"))
print("rows per year:",dict(sorted(yc.items())))
PY

echo "== 6. xlsx export of same feed — confirm columns carry NO url, NO recall number =="
curl -s -A "$UA" "https://www.fda.gov/safety/recalls-market-withdrawals-safety-alerts/datatables-data?_format=xlsx" -o "$OUT/recalls_dt.xlsx"
python3 - "$OUT/recalls_dt.xlsx" <<'PY'
import sys,zipfile,xml.etree.ElementTree as ET
z=zipfile.ZipFile(sys.argv[1]); ns={'m':'http://schemas.openxmlformats.org/spreadsheetml/2006/main'}
ss=[ ''.join(t.text or '' for t in si.iter('{http://schemas.openxmlformats.org/spreadsheetml/2006/main}t'))
     for si in ET.fromstring(z.read('xl/sharedStrings.xml')).findall('m:si',ns)]
sh=ET.fromstring(z.read('xl/worksheets/sheet1.xml')).find('m:sheetData',ns)
hdr=sh.findall('m:row',ns)[0]
def val(c):
    v=c.find('m:v',ns)
    return ss[int(v.text)] if (v is not None and c.get('t')=='s') else (v.text if v is not None else '')
print("xlsx header columns:", [val(c) for c in hdr.findall('m:c',ns)])
print("hyperlink tags in sheet:", z.read('xl/worksheets/sheet1.xml').decode().count('hyperlink'))
PY

echo "== done — artifacts in $OUT =="
