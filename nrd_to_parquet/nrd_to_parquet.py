r"""nrd_to_parquet.py — one-step NinjaTrader 8 Market Replay (.nrd) -> Parquet.

Converts NT8 replay day files directly to the L1 and L2 Parquet layout used by
the backtester repo (no NinjaTrader, no CSV intermediate):

    <out-root>\<SEASON>\<SYMBOL>-<SEASON>_L1\<YYYYMMDD>.parquet
        Timestamp (ts[ns,UTC]), MarketDataType (int8), Price (f64), Volume (i64)
    <out-root>\<SEASON>\<SYMBOL>-<SEASON>_L2\<YYYYMMDD>.parquet
        Timestamp, MarketDataType (int8: 0 ask, 1 bid), Operation (int8: 0 add,
        1 update, 2 remove), Position (int32), MarketMaker (str, always null),
        Price (f64), Volume (i64)

    <SEASON> is the roll-season year (see season_year): quarterly financials
    roll into the next season on the Monday before December's 3rd Friday; all
    other symbols use the calendar year. This mirrors the repo's Parquet/<YEAR>/
    folder layout built by Build-ContinuousContracts.

Both levels match NT8's own MarketReplay.DumpMarketDepth stream event-for-event,
validated exactly against the gbNRDtoCSV -> replay_csv_to_parquet v2 pipeline
output across every traded symbol. Files are tagged replay_importer.timestamps=
UTC so the backtester's data.py handshake skips its legacy ET->UTC correction.

Usage (PowerShell):
    python nrd_to_parquet.py                    # convert all missing days (L1+L2)
    python nrd_to_parquet.py --levels L1        # one level only
    python nrd_to_parquet.py --symbols MNQ MYM  # limit symbols
    python nrd_to_parquet.py --start 20260704   # date filter (ET calendar day)
    python nrd_to_parquet.py --validate         # compare vs existing parquet
    python nrd_to_parquet.py --force            # re-convert existing days

.NRD FORMAT (reverse-engineered 2026-07-16, verified byte-exact on full days)
=============================================================================
Header: 44 slots x 80 bytes. Slot k < 10 = L1 MarketDataType k (0 Ask, 1 Bid,
2 Last, 3 DailyHigh, 4 DailyLow, 5 DailyVolume, 6 LastClose, 7 Opening,
8 OpenInterest, 9 Settlement); slots 10/11 = L2 ask/bid depth. Slot layout
(little-endian): f64 last_price, i32 count, f64 max, f64 min, f64 first_price,
f64 (1.0), f64 tick_size, i32 flag, i64 t0_ticks, i64 t1_ticks, i64 volume_sum.
Ticks are .NET DateTime ticks (100 ns since 0001-01-01), ALREADY UTC.

Data: one merged stream of variable-length records, time-ordered:
    [m1][m2][info][ts?][price?][volume?]      (all multi-byte fields big-endian)
  info: 0xC0 flag set -> L1 event; else L2 depth op (bit 0x40 update, 0x80
        remove, none add/insert; low 6 bits = book position; side = m2 & 0x80).
  m1 & 0x03 (+0x04): timestamp delta, advances a single global clock cursor:
        code 0: none | 1: u8 | 2: u16 | 3: u32, units 100 ns.
        With 0x04: code 0: u64 in 100 ns; codes 1-3: u8/u16/u32 in SECONDS.
  price (L1): per-type cursor in ticks, initialized from the slot first_price.
        m2 & 0x1F = 5-bit nibble: 0 none; 0x01-0x1E -> delta n-15 (n<=14) /
        n-14 (n>=15) i.e. -14..+16; 0x1F = TYPE ESCAPE: type = (m2>>5)+8,
        no nibble. When nibble is 0, m1 bits 0x18 give: 0x08 constant -15,
        0x10 u8 delta (offset 0x80), 0x18 u32 delta (offset 0x80000000).
  price (L2): per-SIDE cursor (0 ask = slot 10, 1 bid = slot 11), no nibble;
        m1 0x08 u8 (offset 0x80) / 0x10 u16 (offset 0x8000) / 0x18 u32 (offset
        0x80000000); 0x00 = unchanged. (0x18 is the one code never seen in the
        validation corpus; it mirrors the validated L1 0x18 rule.)
  volume: m1 bits 5-7 code -> (bytes, multiplier): 0 none, 1 (1,1), 2 (1,100),
        5 (2,1), 6 (4,1). Others unobserved -> hard error (extend if hit).
  L1 type: m2 >> 5 (Ask..Opening), or escaped +8 (OpenInterest, Settlement).
  Prices are rounded to the tick's decimal precision (see _price_decimals) so a
  0.1 tick reconstructs 4342.9, not 4342.900000000001 (matches the CSV pipeline).
"""
import argparse
import os
import re
import struct
import sys
import time
from datetime import datetime, timedelta, timezone
from decimal import Decimal
from pathlib import Path

import numpy as np
import pyarrow as pa
import pyarrow.parquet as pq

EPOCH_TICKS = 621355968000000000        # .NET ticks at 1970-01-01T00:00Z
_ONE_DAY = timedelta(days=1)
HDR = struct.Struct("<diddddd i qq q")  # 80 bytes
N_SLOTS = 44
TSSIZE = (0, 1, 2, 4)
VOLMAP = {0: (0, 1), 1: (1, 1), 2: (1, 100), 3: (1, 500), 4: (1, 1000),
          5: (2, 1), 6: (4, 1)}

L1_TYPES = ("Ask", "Bid", "Last", "DailyHigh", "DailyLow", "DailyVolume",
            "LastClose", "Opening", "OpenInterest", "Settlement")

# Symbols that roll on the December quarterly cycle; their Parquet "season" year
# starts on the Monday before the 3rd Friday of December (matching the repo's
# Build-ContinuousContracts Get-YearRange). Everything else = plain calendar year.
QUARTERLY_FINANCIALS = frozenset(
    ("ES", "MES", "NQ", "MNQ", "YM", "MYM", "RTY", "M2K"))


def _third_friday(year: int, month: int) -> "datetime":
    d = datetime(year, month, 1)
    # 0=Mon..4=Fri; days until first Friday, then +14 for the third.
    return datetime(year, month, 1 + ((4 - d.weekday()) % 7) + 14)


def season_year(symbol: str, date: str) -> str:
    """Roll-season year (as a 4-char string) for a YYYYMMDD file date, matching
    the repo's Parquet/<YEAR>/ folder assignment. Quarterly financials roll into
    the next season on the Monday before December's 3rd Friday; all other symbols
    use the calendar year."""
    y = int(date[:4])
    if symbol.upper() in QUARTERLY_FINANCIALS:
        boundary = _third_friday(y, 12).replace(hour=0) - _ONE_DAY * 4  # Mon before 3rd Fri
        if datetime(y, int(date[4:6]), int(date[6:8])) >= boundary:
            return str(y + 1)
    return str(y)


class FormatError(RuntimeError):
    pass


def parse_headers(head: bytes):
    slots = []
    for s in range(N_SLOTS):
        (last, cnt, pmax, pmin, pfirst, _one, ticksz, _flag,
         t0, t1, volsum) = HDR.unpack_from(head, s * 80)
        slots.append(dict(count=cnt, first=pfirst, tick=ticksz, t0=t0, t1=t1))
    return slots


def _price_decimals(tick: float) -> int:
    """Decimal places implied by the tick size, so integer-tick prices round to
    the same double the CSV pipeline got by parsing decimal strings. Without this
    a 0.1 tick (GC, MGC, RTY, M2K) yields e.g. 4342.900000000001 instead of
    4342.9; exact-in-binary ticks (0.25, 1.0) are unaffected (round is a no-op)."""
    return max(0, -Decimal(str(tick)).as_tuple().exponent)


def _grow(a: np.ndarray, cap: int) -> np.ndarray:
    b = np.empty(cap, a.dtype)
    b[:len(a)] = a
    return b


def decode(buf: bytes, slots, want_l1: bool = True, want_l2: bool = True) -> dict:
    """Decode the merged event stream in a single pass, returning a dict with the
    requested levels:
        "L1": (ts_ns, mdt, price, vol)
        "L2": (ts_ns, side, op, pos, price, vol)
    L1 events (info 0xC0 set) carry a per-MarketDataType price cursor; L2 depth
    ops carry a per-side (0=ask slot 10, 1=bid slot 11) cursor. Operation is
    0 Add / 1 Update (info 0x40) / 2 Remove (info 0x80); Position is info & 0x3F.
    Prices are rounded to the tick's decimal precision (see _price_decimals)."""
    tick = next((s["tick"] for s in slots if s["count"]), None)
    if tick is None:
        out = {}
        if want_l1:
            out["L1"] = (np.empty(0, "int64"), np.empty(0, "int8"),
                         np.empty(0, "float64"), np.empty(0, "int64"))
        if want_l2:
            out["L2"] = (np.empty(0, "int64"), np.empty(0, "int8"), np.empty(0, "int8"),
                         np.empty(0, "int32"), np.empty(0, "float64"), np.empty(0, "int64"))
        return out

    # Header slot counts are a size HINT, not gospel: some files under-report
    # (e.g. M2K ##-## 20260610 header 5,916,892 vs 6,885,655 real events). Seed
    # capacity from them and grow if the stream turns out to hold more.
    cap1 = n_l1 = sum(slots[k]["count"] for k in range(10))
    cap2 = n_l2 = slots[10]["count"] + slots[11]["count"]
    a_ts = np.empty(cap1, "int64"); a_mdt = np.empty(cap1, "int8")
    a_price = np.empty(cap1, "float64"); a_vol = np.empty(cap1, "int64")
    b_ts = np.empty(cap2, "int64"); b_side = np.empty(cap2, "int8")
    b_op = np.empty(cap2, "int8"); b_pos = np.empty(cap2, "int32")
    b_price = np.empty(cap2, "float64"); b_vol = np.empty(cap2, "int64")

    # price cursors in integer ticks: per-type for L1, per-side for L2 depth
    cur = [round(slots[k]["first"] / tick) for k in range(10)]
    l2cur = [round(slots[10]["first"] / tick), round(slots[11]["first"] / tick)]
    t = min(s["t0"] for s in slots if s["count"])   # global clock cursor
    n = len(buf)
    i = 0
    k1 = 0
    k2 = 0
    while i < n:
        if i + 3 > n:
            raise FormatError(f"truncated record header at offset {i}")
        m1 = buf[i]; m2 = buf[i + 1]; info = buf[i + 2]
        j = i + 3
        tscode = m1 & 3
        if m1 & 4:
            if tscode == 0:
                t += int.from_bytes(buf[j:j + 8], "big"); j += 8
            else:
                sz = TSSIZE[tscode]
                t += int.from_bytes(buf[j:j + sz], "big") * 10_000_000
                j += sz
        elif tscode == 1:
            t += buf[j]; j += 1
        elif tscode == 2:
            t += (buf[j] << 8) | buf[j + 1]; j += 2
        elif tscode == 3:
            t += int.from_bytes(buf[j:j + 4], "big"); j += 4

        pbits = m1 & 0x18
        if info & 0xC0 == 0xC0:                     # ---- L1 event
            mdt = m2 >> 5
            nib = m2 & 0x1F
            if nib == 0x1F:                         # type escape (8, 9)
                mdt += 8
                nib = 0
            c = cur[mdt]
            if nib:
                c += nib - 15 if nib <= 14 else nib - 14
            elif pbits == 0x08:
                c -= 15
            elif pbits == 0x10:
                c += buf[j] - 0x80; j += 1
            elif pbits == 0x18:
                c += int.from_bytes(buf[j:j + 4], "big") - 0x80000000; j += 4
            cur[mdt] = c
            volcode = (m1 >> 5) & 7
            try:
                sz, mult = VOLMAP[volcode]
            except KeyError:
                raise FormatError(
                    f"unknown volume code {volcode} at offset {i}: "
                    f"bytes {buf[i:i+16].hex(' ')}") from None
            vol = int.from_bytes(buf[j:j + sz], "big") * mult if sz else 0
            j += sz
            if k1 >= cap1:
                cap1 = max(cap1 * 2, 1024)
                a_ts = _grow(a_ts, cap1); a_mdt = _grow(a_mdt, cap1)
                a_price = _grow(a_price, cap1); a_vol = _grow(a_vol, cap1)
            a_ts[k1] = (t - EPOCH_TICKS) * 100
            a_mdt[k1] = mdt
            a_price[k1] = c * tick
            a_vol[k1] = vol
            k1 += 1
        else:                                       # ---- L2 depth op
            side = 1 if (m2 & 0x80) else 0          # 0 = ask (slot 10), 1 = bid (11)
            c = l2cur[side]
            if pbits == 0x08:
                c += buf[j] - 0x80; j += 1
            elif pbits == 0x10:
                c += int.from_bytes(buf[j:j + 2], "big") - 0x8000; j += 2
            elif pbits == 0x18:
                c += int.from_bytes(buf[j:j + 4], "big") - 0x80000000; j += 4
            l2cur[side] = c
            volcode = (m1 >> 5) & 7
            try:
                sz, mult = VOLMAP[volcode]
            except KeyError:
                raise FormatError(
                    f"unknown L2 volume code {volcode} at offset {i}: "
                    f"bytes {buf[i:i+16].hex(' ')}") from None
            vol = int.from_bytes(buf[j:j + sz], "big") * mult if sz else 0
            j += sz
            if k2 >= cap2:
                cap2 = max(cap2 * 2, 1024)
                b_ts = _grow(b_ts, cap2); b_side = _grow(b_side, cap2); b_op = _grow(b_op, cap2)
                b_pos = _grow(b_pos, cap2); b_price = _grow(b_price, cap2); b_vol = _grow(b_vol, cap2)
            b_ts[k2] = (t - EPOCH_TICKS) * 100
            b_side[k2] = side
            b_op[k2] = 2 if (info & 0x80) else (1 if (info & 0x40) else 0)
            b_pos[k2] = info & 0x3F
            b_price[k2] = c * tick
            b_vol[k2] = vol
            k2 += 1
        i = j
    if i != n:
        # A clean parse lands exactly on the final byte. Anything else means a
        # record length was misread (corruption / unknown encoding variant) --
        # fail loudly rather than emit misaligned data. This replaces the old
        # k1==n_l1 / k2==n_l2 checks, which false-failed on files whose header
        # under-reports the true event count.
        raise FormatError(f"stream not fully consumed: stopped at offset {i} of {n}")

    ndp = _price_decimals(tick)
    out = {}
    if want_l1:
        a_ts = a_ts[:k1]; a_mdt = a_mdt[:k1]; a_price = a_price[:k1]; a_vol = a_vol[:k1]
        np.round(a_price, ndp, out=a_price)
        out["L1"] = (a_ts, a_mdt, a_price, a_vol)
    if want_l2:
        b_ts = b_ts[:k2]; b_side = b_side[:k2]; b_op = b_op[:k2]
        b_pos = b_pos[:k2]; b_price = b_price[:k2]; b_vol = b_vol[:k2]
        np.round(b_price, ndp, out=b_price)
        out["L2"] = (b_ts, b_side, b_op, b_pos, b_price, b_vol)
    return out


def convert_file(nrd_path: Path, want_l1: bool = True, want_l2: bool = True) -> dict:
    raw = nrd_path.read_bytes()
    if len(raw) < N_SLOTS * 80:
        raise FormatError(f"{nrd_path}: too small for header table")
    slots = parse_headers(raw[:N_SLOTS * 80])
    return decode(raw[N_SLOTS * 80:], slots, want_l1, want_l2)


def _meta(nrd_path: Path) -> dict:
    st = nrd_path.stat()
    return {
        b"replay_importer.timestamps": b"UTC",
        b"nrd2parquet.version": b"2",
        b"nrd2parquet.source_name": str(nrd_path.name).encode(),
        b"nrd2parquet.source_contract": nrd_path.parent.name.encode(),
        b"nrd2parquet.source_size": str(st.st_size).encode(),
        b"nrd2parquet.source_mtime_ns": str(st.st_mtime_ns).encode(),
    }


def build_table(arrays, nrd_path: Path) -> pa.Table:
    ts, mdt, price, vol = arrays
    return pa.table({
        "Timestamp": pa.array(ts, pa.timestamp("ns", tz="UTC")),
        "MarketDataType": pa.array(mdt, pa.int8()),
        "Price": pa.array(price, pa.float64()),
        "Volume": pa.array(vol, pa.int64()),
    }).replace_schema_metadata(_meta(nrd_path))


def build_table_l2(arrays, nrd_path: Path) -> pa.Table:
    ts, side, op, pos, price, vol = arrays
    return pa.table({
        "Timestamp": pa.array(ts, pa.timestamp("ns", tz="UTC")),
        "MarketDataType": pa.array(side, pa.int8()),
        "Operation": pa.array(op, pa.int8()),
        "Position": pa.array(pos, pa.int32()),
        "MarketMaker": pa.nulls(len(ts), pa.string()),
        "Price": pa.array(price, pa.float64()),
        "Volume": pa.array(vol, pa.int64()),
    }).replace_schema_metadata(_meta(nrd_path))


BUILDERS = {"L1": build_table, "L2": build_table_l2}
# Columns compared in --validate (MarketMaker is always null, so it is skipped);
# order matches the decode() tuple for each level.
VALIDATE_COLS = {
    "L1": ["Timestamp", "MarketDataType", "Price", "Volume"],
    "L2": ["Timestamp", "MarketDataType", "Operation", "Position", "Price", "Volume"],
}


# A valid replay contract folder is "<SYM> MM-YY" (front month) or "<SYM> ##-##"
# (continuous). Anything else (e.g. a stray, half-renamed "NQ ##-26") is skipped
# so its files never get mis-attributed to a real symbol's dataset.
CONTRACT_DIR_RE = re.compile(r"^\S+ (\d{2}-\d{2}|##-##)$")


CONTINUOUS_DIR_RE = re.compile(r"^(\S+) \d{4} Continuous$")


def discover_continuous(cont_root: Path, symbols, years):
    """Map (symbol, date) -> .nrd from the Continuous archive, laid out as
    <cont_root>\\<year>\\<SYM> <year> Continuous\\<YYYYMMDD>.nrd. Each day already
    has exactly one roll-selected file per symbol, so there is no size contention.
    The folder <year> equals season_year(sym, date), so output nests correctly."""
    work = {}
    for year_dir in sorted(p for p in cont_root.iterdir() if p.is_dir()):
        if not re.fullmatch(r"\d{4}", year_dir.name):
            continue
        if years and year_dir.name not in years:
            continue
        for sym_dir in sorted(p for p in year_dir.iterdir() if p.is_dir()):
            m = CONTINUOUS_DIR_RE.fullmatch(sym_dir.name)
            if not m:
                continue
            sym = m.group(1).upper()
            if symbols and sym not in symbols:
                continue
            for f in sym_dir.glob("*.nrd"):
                if re.fullmatch(r"\d{8}", f.stem):
                    work[(sym, f.stem)] = f
    return work


def discover(replay_root: Path, symbols):
    """Map (symbol, date) -> chosen .nrd path (largest file = front contract)."""
    work = {}
    for contract_dir in sorted(replay_root.iterdir()):
        if not contract_dir.is_dir():
            continue
        if not CONTRACT_DIR_RE.fullmatch(contract_dir.name):
            continue
        sym = contract_dir.name.split(" ")[0].upper()
        if symbols and sym not in symbols:
            continue
        for f in contract_dir.glob("*.nrd"):
            if not re.fullmatch(r"\d{8}", f.stem):
                continue
            key = (sym, f.stem)
            if key not in work or f.stat().st_size > work[key].stat().st_size:
                work[key] = f
    return work


def validate_against(out_path: Path, level: str, arrays) -> str:
    t = pq.read_table(out_path)
    cols = VALIDATE_COLS[level]
    refs = []
    for c in cols:
        col = t.column(c).to_numpy(zero_copy_only=False)
        refs.append(col.astype("int64") if c == "Timestamp" else col)
    if len(refs[0]) != len(arrays[0]):
        return f"ROW COUNT differs: decoded {len(arrays[0]):,} vs existing {len(refs[0]):,}"
    bad = []
    for name, a, b in zip(cols, arrays, refs):
        neq = np.nonzero(a != b)[0]
        if len(neq):
            i0 = int(neq[0])
            bad.append(f"{name}: {len(neq):,} diffs, first at row {i0} "
                       f"(decoded {a[i0]} vs existing {b[i0]})")
    return "; ".join(bad) if bad else ""


def main():
    ap = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    ap.add_argument("--replay-dir", default=os.path.expandvars(
        r"%USERPROFILE%\Documents\NinjaTrader 8\db\replay"))
    ap.add_argument("--out-root", default=r"M:\NinjaTrader_DataRepo\RawData\Parquet",
                    help="Parquet repo root; output nests as <out-root>\\<SEASON>\\<SYM>-<SEASON>_<LEVEL>")
    ap.add_argument("--symbols", nargs="*", default=None,
                    help="limit to these symbols (default: all found)")
    ap.add_argument("--continuous-root", default=None,
                    help="rebuild from the Continuous archive "
                         "(<root>\\<year>\\<SYM> <year> Continuous\\<date>.nrd) "
                         "instead of --replay-dir")
    ap.add_argument("--years", nargs="*", default=None,
                    help="season years to include in continuous mode, e.g. 2025 2026")
    ap.add_argument("--levels", nargs="+", choices=("L1", "L2"), default=["L1", "L2"],
                    help="record levels to export (default: both L1 and L2)")
    ap.add_argument("--start", help="first date YYYYMMDD (ET calendar day)")
    ap.add_argument("--end", help="last date YYYYMMDD")
    ap.add_argument("--force", action="store_true", help="re-convert existing days")
    ap.add_argument("--validate", action="store_true",
                    help="decode and compare against existing parquet; write nothing")
    args = ap.parse_args()

    replay_root = Path(args.replay_dir)
    out_root = Path(args.out_root)
    symbols = {s.upper() for s in args.symbols} if args.symbols else None
    levels = list(dict.fromkeys(args.levels))    # de-dupe, keep order

    if args.continuous_root:
        cont_root = Path(args.continuous_root)
        if not cont_root.is_dir():
            sys.exit(f"continuous root not found: {cont_root}")
        work = discover_continuous(cont_root, symbols,
                                   set(args.years) if args.years else None)
    else:
        if not replay_root.is_dir():
            sys.exit(f"replay dir not found: {replay_root}")
        work = discover(replay_root, symbols)
    if not work:
        sys.exit("no .nrd day files found")

    done = skipped = failed = 0
    for (sym, date), nrd_path in sorted(work.items()):
        if args.start and date < args.start:
            continue
        if args.end and date > args.end:
            continue
        season = season_year(sym, date)

        # Per level, the output path; keep only the ones this run must (re)build.
        targets = {}
        for lvl in levels:
            out_path = out_root / season / f"{sym}-{season}_{lvl}" / f"{date}.parquet"
            if args.validate:
                if out_path.exists():
                    targets[lvl] = out_path
            elif args.force or not out_path.exists():
                targets[lvl] = out_path
        if not targets:
            skipped += 1
            continue

        t0 = time.time()
        try:
            dec = convert_file(nrd_path, want_l1="L1" in targets, want_l2="L2" in targets)
        except (FormatError, OSError) as e:
            # One unreadable/corrupt/vanished day must not abort the whole run.
            print(f"{sym} {date}: DECODE FAILED ({nrd_path.parent.name}): {e}")
            failed += 1
            continue
        secs = time.time() - t0

        for lvl, out_path in targets.items():
            arrays = dec[lvl]
            n = len(arrays[0])
            if args.validate:
                diff = validate_against(out_path, lvl, arrays)
                tag = "EXACT MATCH" if not diff else f"DIFFERS -> {diff}"
                print(f"{sym} {date} {lvl}: {n:,} events vs {out_path.name}: {tag} "
                      f"({nrd_path.parent.name}, {secs:.0f}s)")
                failed += 1 if diff else 0
                done += 0 if diff else 1
            else:
                out_path.parent.mkdir(parents=True, exist_ok=True)
                tmp = out_path.with_suffix(f".tmp{os.getpid()}")
                pq.write_table(BUILDERS[lvl](arrays, nrd_path), tmp, compression="zstd")
                os.replace(tmp, out_path)
                print(f"{sym} {date} {lvl}: {n:,} events -> {out_path} "
                      f"({nrd_path.parent.name}, {secs:.0f}s)")
                done += 1
    print(f"\n{'validated' if args.validate else 'converted'}: {done}, "
          f"skipped: {skipped}, failed: {failed}")
    if failed:
        sys.exit(1)


if __name__ == "__main__":
    main()
