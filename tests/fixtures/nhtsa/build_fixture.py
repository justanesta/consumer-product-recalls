"""Build the deterministic NHTSA test fixture ZIP.

Generates ``sample_recalls.zip`` next to this script with:

- 10 representative TSV rows covering: ordinary record with embedded
  HTML anchor (Damon-style), ODATE ``19010101`` sentinel, null DATEA,
  ``do_not_drive="Yes"``, ``do_not_drive="No"`` / ``park_outside="Yes"``,
  pre-2007 record (empty NOTES/RCL_CMPT_ID/MFR_*/DO_NOT_DRIVE/PARK_OUTSIDE),
  pre-2008 record (NOTES populated but RCL_CMPT_ID empty), and three
  generic rows for batch shape.
- 29 tab-delimited fields per row, CRLF line termination, no header,
  matching the live shape per Finding E.
- A separate ``drift_30col.tsv`` byte payload (NOT in the ZIP) used by
  the field-count drift unit test — exposes the right-edge column
  addition pattern from Finding F.

Reproducible: zipfile inner mtime fixed to 2026-05-05 12:00 UTC so
re-running the script produces byte-identical output. Run after editing
this file or the field layout in ``src/extractors/nhtsa.py:_FIELD_NAMES``.

Usage (from repo root):
    python tests/fixtures/nhtsa/build_fixture.py
"""

from __future__ import annotations

import zipfile
from pathlib import Path

_HERE = Path(__file__).parent
_TSV_NAME = "FLAT_RCL_FIXTURE.txt"
_ZIP_OUT = _HERE / "sample_recalls.zip"
_DRIFT_OUT = _HERE / "drift_30col.tsv"

# 10 representative rows. Each is a tuple of 29 fields matching
# src/extractors/nhtsa.py:_FIELD_NAMES order.
#
# Field positions (lowercase RCL.txt names):
#   record_id, campno, maketxt, modeltxt, yeartxt, mfgcampno, compname,
#   mfgname, bgman, endman, rcltype, potaff, odate, influenced_by,
#   mfgtxt, rcdate, datea, rpno, fmvss, desc_defect, conequence_defect,
#   corrective_action, notes, rcl_cmpt_id, mfr_comp_name, mfr_comp_desc,
#   mfr_comp_ptno, do_not_drive, park_outside

_HTML_DEFECT = (
    "DAMON SAFETY RECALL NO. RC000018. OWNERS MAY ALSO CONTACT "
    "<A HREF=HTTP://WWW.SAFERCAR.GOV>HTTP://WWW.SAFERCAR.GOV</A> ."
)

_ROWS: list[tuple[str, ...]] = [
    # Row 1 — ordinary modern recall with embedded HTML in narrative fields.
    (
        "200001",  # record_id
        "23V123000",  # campno
        "DAMON",  # maketxt
        "INTRUDER",  # modeltxt
        "2024",  # yeartxt
        "RC000018",  # mfgcampno
        "EQUIPMENT:RV:LPG SYSTEM",  # compname
        "THOR MOTOR COACH",  # mfgname
        "20230101",  # bgman
        "20231231",  # endman
        "V",  # rcltype
        "1500",  # potaff
        "20240115",  # odate
        "MFR",  # influenced_by
        "THOR MOTOR COACH",  # mfgtxt
        "20240120",  # rcdate
        "20240122",  # datea
        "23V-001",  # rpno
        "208",  # fmvss
        _HTML_DEFECT,  # desc_defect — embedded HTML
        "MAY CAUSE LPG LEAK",  # conequence_defect
        "DEALERS WILL REPLACE TANK",  # corrective_action
        "Owner outreach 2024-02-01",  # notes
        "000037237000216701000000332",  # rcl_cmpt_id
        "Acme Tank Co",  # mfr_comp_name
        "LPG storage tank",  # mfr_comp_desc
        "TANK-2024-A",  # mfr_comp_ptno
        "No",  # do_not_drive
        "No",  # park_outside
    ),
    # Row 2 — ODATE 19010101 sentinel (unknown notification date).
    (
        "200002",
        "20V456000",
        "FORD",
        "EXPLORER",
        "2018",
        "20S15",
        "AIRBAGS:FRONTAL",
        "FORD MOTOR COMPANY",
        "20180101",
        "20181231",
        "V",
        "82000",
        "19010101",  # odate sentinel
        "ODI",
        "FORD MOTOR COMPANY",
        "20200615",
        "20200616",
        "20V-031",
        "208",
        "Frontal airbag inflator may rupture.",
        "Increased risk of injury.",
        "Replace inflator.",
        "Owner notification 2020-07-15",
        "00000409700016000",
        "Takata",
        "Inflator",
        "TK-PSAN-2020",
        "Yes",
        "No",
    ),
    # Row 3 — null DATEA (empty between tabs).
    (
        "200003",
        "21V789000",
        "TOYOTA",
        "RAV4",
        "2020",
        "21A001",
        "BRAKES:HYDRAULIC",
        "TOYOTA MOTOR ENGINEERING",
        "20200301",
        "20200430",
        "V",
        "12345",
        "20210210",
        "MFR",
        "TOYOTA MOTOR ENGINEERING",
        "20210215",
        "",  # datea null
        "21V-014",
        "135",
        "Brake fluid may leak from master cylinder.",
        "Reduced braking performance.",
        "Replace master cylinder seal.",
        "",
        "",
        "",
        "",
        "",
        "No",
        "No",
    ),
    # Row 4 — do_not_drive=Yes (the dangerous-recall flag).
    (
        "200004",
        "22V200000",
        "HONDA",
        "ACCORD",
        "2019",
        "22Q010",
        "AIRBAGS:FRONTAL:DRIVER SIDE",
        "AMERICAN HONDA MOTOR CO",
        "20190101",
        "20191231",
        "V",
        "44000",
        "20220301",
        "ODI",
        "AMERICAN HONDA MOTOR CO",
        "20220305",
        "20220306",
        "22V-100",
        "208",
        "Driver airbag inflator may explode.",
        "Severe injury or death.",
        "Replace inflator immediately.",
        "Park-outside-and-do-not-drive recall.",
        "0000040970",
        "Takata",
        "PSAN driver inflator",
        "PSAN-D-2022",
        "Yes",
        "Yes",
    ),
    # Row 5 — pre-2007 record. NOTES/RCL_CMPT_ID/MFR_* are empty
    # because those columns didn't exist until the 2007/2008/2020/2025
    # drift events. DO_NOT_DRIVE / PARK_OUTSIDE empty (post-May-2025).
    (
        "200005",
        "06V123000",
        "GMC",
        "SIERRA",
        "2003",
        "06017",
        "POWER TRAIN:AUTOMATIC TRANSMISSION",
        "GENERAL MOTORS CORPORATION",
        "20020101",
        "20031231",
        "V",
        "55000",
        "20060501",
        "MFR",
        "GENERAL MOTORS CORPORATION",
        "20060510",
        "20060520",
        "06V-018",
        "",
        "Transmission may shift unexpectedly.",
        "Vehicle may roll.",
        "Reprogram transmission control module.",
        "",  # notes — pre-2007
        "",  # rcl_cmpt_id — pre-2008
        "",
        "",
        "",  # mfr_comp_* — pre-2020
        "",
        "",  # do_not_drive / park_outside — pre-May-2025
    ),
    # Row 6 — pre-2008 record. NOTES populated (post-2007), RCL_CMPT_ID empty.
    (
        "200006",
        "07V234000",
        "CHEVROLET",
        "MALIBU",
        "2005",
        "07024",
        "STEERING:LINKAGES",
        "GENERAL MOTORS CORPORATION",
        "20040101",
        "20051231",
        "V",
        "21000",
        "20070801",
        "MFR",
        "GENERAL MOTORS CORPORATION",
        "20070810",
        "20070815",
        "07V-024",
        "",
        "Tie rod end may corrode and separate.",
        "Loss of steering.",
        "Replace tie rod ends.",
        "Owner mailings began 2007-09-15",  # notes
        "",  # rcl_cmpt_id — still pre-2008
        "",
        "",
        "",
        "",
        "",
    ),
    # Row 7 — generic Yes/No park_outside-only.
    (
        "200007",
        "23V300000",
        "VOLKSWAGEN",
        "JETTA",
        "2021",
        "23W001",
        "FUEL SYSTEM:DIESEL",
        "VOLKSWAGEN GROUP OF AMERICA",
        "20210101",
        "20211231",
        "V",
        "8500",
        "20230501",
        "MFR",
        "VOLKSWAGEN GROUP OF AMERICA",
        "20230502",
        "20230503",
        "23V-205",
        "301",
        "Fuel pump seal may leak.",
        "Fire risk while parked.",
        "Replace fuel pump seal.",
        "Owner notification June 2023",
        "0000050100",
        "Bosch",
        "Fuel pump assembly",
        "FP-VW-2023",
        "No",
        "Yes",
    ),
    # Row 8 — null FMVSS (allowed; field 19 is optional).
    (
        "200008",
        "24V400000",
        "TESLA",
        "MODEL 3",
        "2022",
        "24S005",
        "ELECTRICAL SYSTEM:BATTERY",
        "TESLA, INC.",
        "20220101",
        "20221231",
        "V",
        "4200",
        "20240310",
        "MFR",
        "TESLA, INC.",
        "20240312",
        "20240313",
        "24V-205",
        "",  # fmvss null
        "Battery management software may overheat cells.",
        "Increased thermal-event risk.",
        "Over-the-air software update.",
        "OTA pushed 2024-04-01",
        "0000060200",
        "Tesla",
        "Battery pack assembly",
        "BP-M3-2022",
        "No",
        "No",
    ),
    # Row 9 — minimal record with mostly empty optional fields.
    (
        "200009",
        "20V001000",
        "RAM",
        "1500",
        "2017",
        "20A001",
        "EQUIPMENT",
        "FCA US LLC",
        "20160101",
        "20171231",
        "V",
        "200",
        "20200101",
        "ODI",
        "FCA US LLC",
        "20200201",
        "20200202",
        "20V-001",
        "126",
        "Tonneau cover latch may release.",
        "Cover may detach.",
        "Replace latch.",
        "",
        "",
        "",
        "",
        "",
        "No",
        "No",
    ),
    # Row 10 — long narrative with multiple HTML anchors (cassette stress test).
    (
        "200010",
        "25V500000",
        "SUBARU",
        "OUTBACK",
        "2023",
        "25S010",
        "VISIBILITY:WINDSHIELD WIPER/WASHER",
        "SUBARU OF AMERICA",
        "20230101",
        "20231231",
        "V",
        "31000",
        "20250101",
        "MFR",
        "SUBARU OF AMERICA",
        "20250105",
        "20250110",
        "25V-050",
        "104",
        (
            "Windshield wiper motor may fail. See "
            "<A HREF=HTTP://WWW.NHTSA.GOV/RECALLS>NHTSA.GOV/RECALLS</A> "
            "for details and "
            "<A HREF=HTTP://WWW.SAFERCAR.GOV>SAFERCAR.GOV</A>."
        ),
        "Reduced visibility in rain.",
        "Replace wiper motor.",
        "Mailings began January 2025",
        "0000070300",
        "Denso",
        "Wiper motor unit",
        "WIPER-OB-2025",
        "No",
        "No",
    ),
]


def _row_to_line(row: tuple[str, ...]) -> str:
    if len(row) != 29:
        raise ValueError(f"Row has {len(row)} fields; expected 29: {row[:3]!r}")
    return "\t".join(row)


def main() -> None:
    # Build TSV body — 29 fields per row, tab-delimited, CRLF terminated,
    # no header (matches Finding E).
    lines = [_row_to_line(row) for row in _ROWS]
    tsv_bytes = ("\r\n".join(lines) + "\r\n").encode("utf-8")

    # Build the ZIP with a deterministic inner mtime so the fixture is
    # byte-reproducible across re-runs.
    info = zipfile.ZipInfo(_TSV_NAME)
    info.date_time = (2026, 5, 5, 12, 0, 0)
    info.compress_type = zipfile.ZIP_DEFLATED

    with zipfile.ZipFile(_ZIP_OUT, "w") as zf:
        zf.writestr(info, tsv_bytes)

    # Drift fixture — 30 fields (an extra column at the right edge).
    drift_row: list[str] = list(_ROWS[0]) + ["EXTRA_FIELD_VALUE"]
    drift_line = "\t".join(drift_row)
    _DRIFT_OUT.write_bytes((drift_line + "\r\n").encode("utf-8"))

    print(f"Wrote {_ZIP_OUT} ({_ZIP_OUT.stat().st_size} bytes, {len(_ROWS)} rows)")
    print(f"Wrote {_DRIFT_OUT} ({_DRIFT_OUT.stat().st_size} bytes, 30-field drift row)")


if __name__ == "__main__":
    main()
