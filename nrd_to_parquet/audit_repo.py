r"""audit_repo.py — integrity audit of a NinjaTrader data repo, headers only.

Checks the three trees the weekly pipeline maintains, without decoding a
single event:

    RawData/NRD/<YEAR>/<SYM MM-YY>/<YYYYMMDD>.nrd              per-contract recordings
    RawData/Continuous/<YEAR>/<SYM> <YEAR> Continuous/<date>.nrd   roll-selected, one per day
    RawData/Parquet/<SEASON>/<SYM>-<SEASON>_L1|L2/<date>.parquet   converted from Continuous

Everything it needs is in the .nrd header table (per-type event counts, trade
count and volume, see nrd_to_parquet.py) and the Parquet footer (row count and
the nrd2parquet.* source metadata). Validated: a faithful conversion has
L1 rows == sum of header slots 0-9 and L2 rows == slots 10+11, exactly.

Per (symbol, day) it reports which contract the Continuous file really is
(matched by size + header against the per-contract folders), whether that
contract was the day's volume leader, whether the day is thin for its symbol,
and whether the Parquet pair is a faithful conversion of today's Continuous
file.

Flags (a day can carry several):
  CONT_NOT_LEADER  Continuous file's contract traded < --leader-share of the
                   day's busiest contract (wrong roll choice)
  CONT_THIN        trade prints < --thin-share of the symbol-year median for
                   that weekday class (Sunday vs Mon-Fri); Saturdays exempt
  CONT_PARTIAL     header time span misses > 60 min at the start or end of the
                   day's Globex session (recording gap; abbreviated holiday
                   sessions show up here too)
  CONT_UNMATCHED   Continuous file matches no per-contract file (can't tell
                   which contract it is; informational)
  PQ_STALE         Parquet converted from a different file than the archive
                   holds now: footer source size != archive file (or, without
                   that metadata, row count != the archive header)
  CONT_NO_L2       archive file has no depth events but the Parquet L2 does
                   (the archive lost depth; keep the Parquet L2, don't re-convert it)
  PQ_MISSING       Continuous day with no Parquet L1 (inside the symbol's
                   Parquet date range)
  PQ_L2_MISSING    L1 present, L2 missing
  PQ_ORPHAN        Parquet day with no Continuous file
  PQ_EXTERNAL      Parquet day filled from an outside vendor (footer
                   replay_importer.source_name "databento ...") because the
                   recording was missing or unusable; informational
  CAL_GAP          Mon-Fri with no Continuous file inside the symbol's range
                   (full-closure holidays are noted, not flagged)

Usage:
    python audit_repo.py --repo /path/to/NinjaTraderRepo --out audit_dir
Writes audit_dir/days.csv (every day), flagged.csv, summary.md.
"""
import argparse
import csv
import os
import re
import statistics
import struct
import sys
from collections import Counter, defaultdict
from datetime import date, datetime, timedelta, timezone
from pathlib import Path
from zoneinfo import ZoneInfo

import pyarrow.parquet as pq

HDR = struct.Struct("<diddddd i qq q")   # same 80-byte slot as nrd_to_parquet.py
N_SLOTS = 44
CONTRACT_DIR_RE = re.compile(r"^(\S+) (\d{2}-\d{2})$")
CONT_DIR_RE = re.compile(r"^(\S+) (\d{4}) Continuous$")
PQ_DIR_RE = re.compile(r"^(\S+)-(\d{4})_(L1|L2)$")


def nrd_header(path: Path) -> dict:
    with open(path, "rb") as f:
        head = f.read(N_SLOTS * 80)
    if len(head) < N_SLOTS * 80:
        return {"size": path.stat().st_size, "bad": True}
    s = [HDR.unpack_from(head, i * 80) for i in range(N_SLOTS)]
    live = [x for x in s if x[1]]
    return {"size": path.stat().st_size, "bad": False,
            "last_n": s[2][1], "last_vol": s[2][10],
            "l1": sum(x[1] for x in s[:10]), "l2": s[10][1] + s[11][1],
            # .NET ticks (100 ns since 0001-01-01), UTC
            "t0": min(x[8] for x in live) if live else 0,
            "t1": max(x[9] for x in live) if live else 0}


_NET_EPOCH = datetime(1, 1, 1, tzinfo=timezone.utc)
_ET = ZoneInfo("America/New_York")


def _net_to_et(ticks):
    return (_NET_EPOCH + timedelta(microseconds=ticks // 10)).astimezone(_ET)


def coverage_gaps(d: str, h: dict):
    """(minutes missing at the start, minutes missing at the end) of an ET
    calendar-day file vs. the CME Globex session: Sunday opens 18:00 ET,
    Friday closes 17:00 ET, other weekdays run the whole calendar day."""
    day = ymd(d)
    wd = day.weekday()
    start = datetime(day.year, day.month, day.day, 18 if wd == 6 else 0, tzinfo=_ET)
    end = datetime(day.year, day.month, day.day, 17 if wd == 4 else 23,
                   0 if wd == 4 else 59, tzinfo=_ET)
    a, b = _net_to_et(h["t0"]), _net_to_et(h["t1"])
    return max(0, (a - start).total_seconds() / 60), max(0, (end - b).total_seconds() / 60)


def _easter(y):
    a, b, c = y % 19, y // 100, y % 100
    d, e = b // 4, b % 4
    g = (8 * b + 13) // 25
    h = (19 * a + b - d - g + 15) % 30
    j, k = c // 4, c % 4
    m = (a + 11 * h) // 319
    r = (2 * e + 2 * j - k - h + m + 32) % 7
    n = (h - m + r + 90) // 25
    return date(y, n, (h - m + r + n + 19) % 32)


def full_closures(y):
    """CME Globex full-day closures (no session file expected). Other US
    holidays are abbreviated sessions, which DO produce files."""
    out = {_easter(y) - timedelta(days=2): "Good Friday"}
    for md, name in (((1, 1), "New Year"), ((12, 25), "Christmas")):
        d = date(y, *md)
        if d.weekday() == 5:
            d -= timedelta(days=1)
        elif d.weekday() == 6:
            d += timedelta(days=1)
        out[d] = name
    return out


def ymd(s):
    return date(int(s[:4]), int(s[4:6]), int(s[6:8]))


def scan(repo: Path, symbols, progress=True):
    raw = repo / "RawData"
    per = defaultdict(list)      # (sym, date) -> [(contract, hdr)]
    cont = {}                    # (sym, date) -> hdr
    pqd = {}                     # (sym, date, level) -> (rows, meta)
    n = 0

    def tick():
        nonlocal n
        n += 1
        if progress and n % 5000 == 0:
            print(f"  ... {n} files", file=sys.stderr, flush=True)

    for ydir in sorted((raw / "NRD").iterdir()):
        if not (ydir.is_dir() and ydir.name.isdigit()):
            continue
        for cdir in sorted(ydir.iterdir()):
            m = CONTRACT_DIR_RE.fullmatch(cdir.name)
            if not m or (symbols and m.group(1).upper() not in symbols):
                continue
            for f in cdir.glob("*.nrd"):
                if re.fullmatch(r"\d{8}", f.stem):
                    per[(m.group(1).upper(), f.stem)].append((m.group(2), nrd_header(f)))
                    tick()
    for ydir in sorted((raw / "Continuous").iterdir()):
        if not (ydir.is_dir() and ydir.name.isdigit()):
            continue
        for sdir in sorted(ydir.iterdir()):
            m = CONT_DIR_RE.fullmatch(sdir.name)
            if not m or (symbols and m.group(1).upper() not in symbols):
                continue
            for f in sdir.glob("*.nrd"):
                if re.fullmatch(r"\d{8}", f.stem):
                    cont[(m.group(1).upper(), f.stem)] = nrd_header(f)
                    tick()
    for ydir in sorted((raw / "Parquet").iterdir()):
        if not (ydir.is_dir() and ydir.name.isdigit()):
            continue
        for ldir in sorted(ydir.iterdir()):
            m = PQ_DIR_RE.fullmatch(ldir.name)
            if not m or (symbols and m.group(1).upper() not in symbols):
                continue
            for f in ldir.glob("*.parquet"):
                if not re.fullmatch(r"\d{8}", f.stem):
                    continue
                try:
                    md = pq.read_metadata(f)
                    meta = {k.decode(): v.decode() for k, v in (md.metadata or {}).items()
                            if k.startswith(b"nrd2parquet") or k == b"replay_importer.source_name"}
                    pqd[(m.group(1).upper(), f.stem, m.group(3))] = (md.num_rows, meta)
                except Exception as e:   # unreadable footer is itself a finding
                    pqd[(m.group(1).upper(), f.stem, m.group(3))] = (-1, {"error": str(e)})
                tick()
    return per, cont, pqd


def audit(per, cont, pqd, leader_share, thin_share):
    syms = sorted({k[0] for k in cont} | {k[0] for k in pqd})
    # Thin-day baseline: median trade prints per (symbol, year, weekday class)
    base = defaultdict(list)
    for (sym, d), h in cont.items():
        wd = ymd(d).weekday()
        if wd != 5 and not h["bad"]:
            base[(sym, d[:4], wd == 6)].append(h["last_n"])
    med = {k: statistics.median(v) for k, v in base.items() if v}
    pq_range = {}
    for (sym, d, lvl) in pqd:
        lo, hi = pq_range.get(sym, (d, d))
        pq_range[sym] = (min(lo, d), max(hi, d))

    rows = []
    for sym in syms:
        dates = sorted({d for s, d in cont if s == sym} | {d for s, d, _ in pqd if s == sym})
        if not dates:
            continue
        closures = {}
        for y in range(int(dates[0][:4]), int(dates[-1][:4]) + 1):
            closures.update(full_closures(y))
        # calendar gaps
        d0, d1 = ymd(dates[0]), ymd(dates[-1])
        have = set(dates)
        x = d0
        while x <= d1:
            k = x.strftime("%Y%m%d")
            if x.weekday() < 5 and k not in have:
                rows.append({"symbol": sym, "date": k, "dow": x.strftime("%a"),
                             "flags": "" if x in closures else "CAL_GAP",
                             "note": closures.get(x, "no Continuous file")})
            x += timedelta(days=1)
        for d in dates:
            wd = ymd(d).weekday()
            r = {"symbol": sym, "date": d, "dow": ymd(d).strftime("%a")}
            flags, notes = [], []
            h = cont.get((sym, d))
            cands = per.get((sym, d), [])
            leader = max(cands, key=lambda c: c[1].get("last_vol", 0), default=None)
            if leader:
                r["leader"] = leader[0]
                r["leader_vol"] = leader[1].get("last_vol", 0)
            if h:
                r.update(cont_size=h["size"], cont_last_n=h.get("last_n"),
                         cont_vol=h.get("last_vol"), cont_l1=h.get("l1"), cont_l2=h.get("l2"))
                match = [c for c, ch in cands if ch["size"] == h["size"]
                         and ch.get("l1") == h.get("l1") and ch.get("last_n") == h.get("last_n")]
                r["cont_contract"] = match[0] if match else ""
                if h["bad"]:
                    flags.append("CONT_UNREADABLE")
                elif wd != 5:
                    if not match and cands:
                        flags.append("CONT_UNMATCHED")
                    lv = r.get("leader_vol", 0)
                    if lv and h["last_vol"] < leader_share * lv:
                        flags.append("CONT_NOT_LEADER")
                        notes.append(f"leader {leader[0]} vol {lv:,} vs {h['last_vol']:,}")
                    m = med.get((sym, d[:4], wd == 6))
                    if m and h["last_n"] < thin_share * m:
                        flags.append("CONT_THIN")
                        notes.append(f"{h['last_n']:,} prints vs median {m:,.0f}")
                    if h["t0"] and h["t1"]:
                        g0, g1 = coverage_gaps(d, h)
                        r["late_start_min"], r["early_end_min"] = round(g0), round(g1)
                        if g0 > 60 or g1 > 60:
                            flags.append("CONT_PARTIAL")
                            notes.append(f"covers {_net_to_et(h['t0']):%H:%M}-"
                                         f"{_net_to_et(h['t1']):%H:%M} ET")
            l1 = pqd.get((sym, d, "L1"))
            l2 = pqd.get((sym, d, "L2"))
            external = False
            if l1:
                r["pq_l1_rows"], meta = l1
                external = meta.get("replay_importer.source_name", "").startswith("databento")
                r["pq_src_size"] = meta.get("nrd2parquet.source_size", "")
                src = [c for c, ch in cands if str(ch["size"]) == r["pq_src_size"]]
                r["pq_src_contract"] = src[0] if src else ""
            if l2:
                r["pq_l2_rows"] = l2[0]
            in_pq = sym in pq_range and pq_range[sym][0] <= d <= pq_range[sym][1]
            if external:
                # Filled from an outside vendor because the recording was missing
                # or unusable: not comparable to the archive file by design.
                flags.append("PQ_EXTERNAL")
                notes.append(meta.get("replay_importer.source_name", ""))
            elif h and not h["bad"]:
                # Provenance first: a Parquet whose footer names this exact
                # source file (same size) IS a conversion of it, even when the
                # header under-reports its event counts. Row counts vs the
                # header only decide for files without that metadata.
                same_src = bool(l1) and r.get("pq_src_size") == str(h["size"])
                if l1 and not same_src and l1[0] != h["l1"]:
                    flags.append("PQ_STALE")
                    notes.append(f"L1 rows {l1[0]:,} vs archive {h['l1']:,}"
                                 + (f" (parquet is {r['pq_src_contract']})" if r.get("pq_src_contract") else ""))
                if l2 and h["l2"] == 0 and l2[0] > 0:
                    # Archive file has no depth at all (verified by decoding, not
                    # just the header); the Parquet L2 came from a copy that did.
                    # The Parquet is the better copy here: do NOT re-convert L2.
                    flags.append("CONT_NO_L2")
                    notes.append(f"archive has no depth; parquet L2 {l2[0]:,} rows")
                elif l2 and not same_src and l2[0] != h["l2"] and "PQ_STALE" not in flags:
                    flags.append("PQ_STALE")
                    notes.append(f"L2 rows {l2[0]:,} vs archive {h['l2']:,}")
                if not l1 and in_pq and wd != 5:
                    flags.append("PQ_MISSING")
                if l1 and not l2:
                    flags.append("PQ_L2_MISSING")
            if not external and (l1 or l2) and not h:
                flags.append("PQ_ORPHAN")
            r["flags"] = " ".join(flags)
            r["note"] = "; ".join(notes)
            rows.append(r)
    rows.sort(key=lambda r: (r["symbol"], r["date"]))
    return rows


FIELDS = ["symbol", "date", "dow", "flags", "note", "cont_contract", "leader",
          "cont_last_n", "cont_vol", "leader_vol", "cont_l1", "cont_l2", "cont_size",
          "late_start_min", "early_end_min",
          "pq_l1_rows", "pq_l2_rows", "pq_src_size", "pq_src_contract"]


def write(rows, out: Path, args):
    out.mkdir(parents=True, exist_ok=True)
    for name, rs in (("days.csv", rows), ("flagged.csv", [r for r in rows if r.get("flags")])):
        with open(out / name, "w", newline="") as f:
            w = csv.DictWriter(f, FIELDS, extrasaction="ignore")
            w.writeheader()
            w.writerows(rs)
    by = defaultdict(Counter)
    span = {}
    for r in rows:
        s = r["symbol"]
        lo, hi = span.get(s, (r["date"], r["date"]))
        span[s] = (min(lo, r["date"]), max(hi, r["date"]))
        for fl in r.get("flags", "").split():
            by[s][fl] += 1
    kinds = ["CONT_NOT_LEADER", "CONT_THIN", "CONT_PARTIAL", "CONT_UNMATCHED", "CONT_UNREADABLE",
             "CONT_NO_L2", "PQ_STALE", "PQ_EXTERNAL",
             "PQ_MISSING", "PQ_L2_MISSING", "PQ_ORPHAN", "CAL_GAP"]
    lines = ["# Repo audit summary", "",
             f"leader share < {args.leader_share:.0%}, thin < {args.thin_share:.0%} of median", "",
             "| symbol | range | " + " | ".join(kinds) + " |",
             "|---|---|" + "---|" * len(kinds)]
    for s in sorted(span):
        lines.append(f"| {s} | {span[s][0]}..{span[s][1]} | "
                     + " | ".join(str(by[s].get(k, "")) for k in kinds) + " |")
    (out / "summary.md").write_text("\n".join(lines) + "\n")
    print("\n".join(lines))


def main():
    ap = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    ap.add_argument("--repo", required=True, help="NinjaTraderRepo root (contains RawData/)")
    ap.add_argument("--out", required=True)
    ap.add_argument("--symbols", nargs="*")
    ap.add_argument("--leader-share", type=float, default=0.5)
    ap.add_argument("--thin-share", type=float, default=0.2)
    args = ap.parse_args()
    syms = {s.upper() for s in args.symbols} if args.symbols else None
    per, cont, pqd = scan(Path(args.repo), syms)
    print(f"scanned: {sum(len(v) for v in per.values())} per-contract, {len(cont)} continuous, "
          f"{len(pqd)} parquet", file=sys.stderr)
    write(audit(per, cont, pqd, args.leader_share, args.thin_share), Path(args.out), args)


if __name__ == "__main__":
    main()
