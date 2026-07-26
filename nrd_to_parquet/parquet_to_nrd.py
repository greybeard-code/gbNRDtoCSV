r"""parquet_to_nrd.py — rebuild NinjaTrader 8 Market Replay (.nrd) files from the
L1 + L2 Parquet pairs written by nrd_to_parquet.py.

The inverse of nrd_to_parquet: use it to replay an archived day in NT8 when the
original .nrd is gone but the Parquet survives.

    python parquet_to_nrd.py --symbols YM --start 20251214 --end 20251214
    python parquet_to_nrd.py --verify --symbols YM        # rebuild + self-check, write nothing

Output layout mirrors the continuous replay bin so nrd_to_parquet.py can read it
straight back for verification:

    <out-root>\<SYM> ##-##\<YYYYMMDD>.nrd

FIDELITY — read this before trusting the output
===============================================
The rebuild is *semantically* faithful, not byte-identical. Two things are lost
when the single .nrd stream is split into two Parquet files, and neither can be
recovered from the Parquet alone:

1. THE L1/L2 INTERLEAVE.  The .nrd is one merged stream; ~81% of distinct
   timestamps host both an L1 and an L2 event, and the Parquet does not record
   which came first. This module reconstructs it with the `top` rule (see
   merge_order): NT8 emits an L1 quote immediately before the depth op that
   changes the top of that side's book (position 0), together with the shift
   partner that op pairs with (rem@>=9 + add@0, or add@0 + rem@>=9); depth ops
   at positions 1..8 are independent deep-level updates and float between units.
   Measured against real .nrd files this places ~97% of records correctly
   (vs ~33% for a naive L1-before-L2 merge). The residual misordering is always
   *within a single timestamp* -- the L1 and L2 subsequences are each reproduced
   exactly and in order, so the depth book always reconstructs correctly.

2. THE ENCODING CHOICE.  The format admits several encodings of the same value
   (volume 500 as 1 byte x500 or 2 bytes x1; a 1s gap as u32 ticks or u8
   seconds). NT8's chooser is not reverse-engineered, so this writer uses a
   safe canonical subset. Files are therefore slightly larger than NT8's own.

Everything else round-trips exactly: timestamps, prices, volumes, market data
types, depth operations and positions, and all 11 header fields per slot.
Use --verify to confirm: it re-decodes the rebuilt bytes with nrd_to_parquet's
validated decoder and compares against the source Parquet event for event.

The tick size is not stored in the Parquet; it is recovered as the GCD of the
observed prices (exact for futures, whose prices are integer multiples of the
tick). Override with --tick if a thin day never moves a single tick.
"""
import argparse
import os
import re
import struct
import sys
import time
from datetime import datetime, timezone
from math import gcd
from pathlib import Path

import numpy as np
import pyarrow.parquet as pq

from nrd_to_parquet import EPOCH_TICKS, N_SLOTS, decode, parse_headers

HDR = struct.Struct("<diddddd i qq q")   # 80 bytes, same as nrd_to_parquet.HDR
DBL_MAX = 1.7976931348623157e308
ASK, BID = 0, 1


def to_ticks(ns: int) -> int:
    """UTC nanoseconds -> .NET ticks, rounded to the nearest 100 ns.

    .nrd stores .NET DateTime ticks, so 100 ns is the finest resolution the
    format can express. Parquet written by nrd_to_parquet is always an exact
    multiple of 100 and converts losslessly. Parquet written by the older
    CSV pipeline is not -- float64 cannot hold 19 significant digits, so those
    timestamps carry corrupted sub-100 ns tails. Rounding (not truncating)
    recovers the intended grid value in that case; either way the shift is
    under 100 ns. Rounding is monotonic, so stream ordering is preserved."""
    return (ns + 50) // 100 + EPOCH_TICKS


# ---------------------------------------------------------------- tick recovery

def infer_tick(*price_arrays) -> float:
    """Recover the tick size as the GCD of the observed prices. Futures prices are
    exact integer multiples of the tick, so the GCD is the tick (or a multiple of
    it on a day that never moved one tick -- harmless for the round trip, since
    every price is still an exact multiple, but --tick overrides it)."""
    prices = np.concatenate([p for p in price_arrays if len(p)]) if any(
        len(p) for p in price_arrays) else np.empty(0)
    prices = prices[prices != 0]
    if not len(prices):
        return 1.0
    # smallest decimal precision that represents every price exactly
    for ndp in range(9):
        scaled = prices * (10 ** ndp)
        if np.allclose(scaled, np.round(scaled), rtol=0, atol=1e-6):
            break
    ints = np.unique(np.round(scaled).astype(np.int64))
    g = 0
    for v in ints:
        g = gcd(g, int(abs(v)))
        if g == 1:
            break
    return g / (10 ** ndp)


# ---------------------------------------------------------------- merge order

def merge_order(ts1, mdt1, ts2, side2, op2, pos2):
    """Rebuild the merged stream order as (level, index) pairs, level 1 = L1.

    Within a timestamp the `top` rule applies (see module docstring): each
    top-of-book depth op -- extended backwards over a rem@>=9/add@0 shift pair --
    starts a unit, and the next unconsumed quote of that side is placed just
    before it. Non-quote L1 events (Last, DailyVolume, ...) are flushed just
    ahead of the quote they precede in the L1 stream, preserving L1 order.
    Anything left over is appended in L1 order."""
    n1, n2 = len(ts1), len(ts2)
    out = []
    i = j = 0
    while i < n1 or j < n2:
        # ---- collect one timestamp group from each side
        if j >= n2 or (i < n1 and ts1[i] < ts2[j]):
            t = ts1[i]
        else:
            t = ts2[j]
        a0 = i
        while i < n1 and ts1[i] == t:
            i += 1
        b0 = j
        while j < n2 and ts2[j] == t:
            j += 1
        g1 = range(a0, i)
        g2 = range(b0, j)
        if not g2:
            out.extend((1, k) for k in g1)
            continue
        if not g1:
            out.extend((2, k) for k in g2)
            continue

        # ---- unit starts: top-of-book ops, extended over a shift pair
        starts = {}
        for k in g2:
            if pos2[k] != 0:
                continue
            s = k
            if k > b0 and side2[k - 1] == side2[k] and pos2[k - 1] >= 9 \
                    and (op2[k - 1], op2[k]) in ((2, 0), (0, 2)):
                s = k - 1
            starts.setdefault(s, side2[k])

        # Place quotes at unit starts, but only ever moving FORWARD through the L1
        # group: both levels must come back out in exactly their Parquet order or
        # the round trip breaks, so a unit that would need an out-of-order quote
        # simply goes unattached.
        p = a0
        for k in g2:
            side = starts.get(k)
            if side is not None:
                q = p
                while q < i and mdt1[q] not in (ASK, BID):
                    q += 1                       # skip Last/DailyVolume/...
                if q < i and mdt1[q] == side:
                    for x in range(p, q + 1):    # pending non-quotes, then the quote
                        out.append((1, x))
                    p = q + 1
            out.append((2, k))
        out.extend((1, x) for x in range(p, i))
    return out


# ---------------------------------------------------------------- encoding

def _put_ts(rec, delta):
    """Timestamp delta (in 100 ns .NET ticks) -> (m1 bits, appended bytes).
    Canonical subset: raw-tick codes while they fit, else the u64 escape."""
    if delta == 0:
        return 0
    if delta < 0x100:
        rec.append(delta); return 1
    if delta < 0x10000:
        rec += delta.to_bytes(2, "big"); return 2
    if delta < 0x100000000:
        rec += delta.to_bytes(4, "big"); return 3
    rec += delta.to_bytes(8, "big"); return 4          # m1 bit 0x04, tscode 0


def _put_vol(rec, vol):
    """Volume -> volcode. Canonical subset: 0 / u8 / u16 / u32, never the
    x100/x500/x1000 multiplier codes (semantically identical, just smaller)."""
    if vol == 0:
        return 0
    if vol < 0x100:
        rec.append(vol); return 1
    if vol < 0x10000:
        rec += vol.to_bytes(2, "big"); return 5
    if vol < 0x100000000:
        rec += vol.to_bytes(4, "big"); return 6
    raise ValueError(f"volume {vol} exceeds the 32-bit field")


def _put_price_l1(rec, d, escaped):
    """L1 price delta in ticks -> (m2 nibble, m1 pbits). Types 8/9 reach the
    decoder through the 0x1F type escape, which consumes the nibble, so they
    must use the pbits forms only."""
    if d == 0:
        return 0, 0
    if not escaped:
        if -14 <= d <= -1:
            return d + 15, 0
        if 1 <= d <= 16:
            return d + 14, 0
    if d == -15:
        return 0, 0x08
    if -0x80 <= d <= 0x7F:
        rec.append(d + 0x80); return 0, 0x10
    rec += (d + 0x80000000).to_bytes(4, "big"); return 0, 0x18


def _put_price_l2(rec, d):
    """L2 price delta in ticks -> m1 pbits. No nibble on the depth path."""
    if d == 0:
        return 0
    if -0x80 <= d <= 0x7F:
        rec.append(d + 0x80); return 0x08
    if -0x8000 <= d <= 0x7FFF:
        rec += (d + 0x8000).to_bytes(2, "big"); return 0x10
    rec += (d + 0x80000000).to_bytes(4, "big"); return 0x18


def encode_stream(order, ts1, mdt1, c1, vol1, ts2, side2, op2, pos2, c2, vol2,
                  first1, first2):
    """Encode the merged record stream. Prices arrive as integer tick counts;
    cursors start at the header `first` values, exactly as decode() seeds them."""
    buf = bytearray()
    cur = list(first1)              # per-MarketDataType price cursor
    l2cur = list(first2)            # per-side depth cursor
    t = None
    for level, k in order:
        rec = bytearray()
        tick_ts = to_ticks(ts1[k] if level == 1 else ts2[k])
        if t is None:
            t = tick_ts             # first record: cursor starts here, delta 0
        delta = tick_ts - t
        if delta < 0:
            raise ValueError(f"non-monotonic timestamp at {level}/{k}")
        tscode = _put_ts(rec, delta)
        t = tick_ts
        m1 = (tscode & 3) | (0x04 if tscode == 4 else 0)

        if level == 1:
            mdt = int(mdt1[k])
            d = int(c1[k]) - cur[mdt]
            cur[mdt] = int(c1[k])
            escaped = mdt >= 8
            nib, pbits = _put_price_l1(rec, d, escaped)
            m2 = ((mdt - 8) << 5) | 0x1F if escaped else (mdt << 5) | nib
            info = 0xC0
        else:
            side = int(side2[k])
            d = int(c2[k]) - l2cur[side]
            l2cur[side] = int(c2[k])
            pbits = _put_price_l2(rec, d)
            m2 = 0x80 if side == BID else 0x00
            o = int(op2[k])
            info = (0x80 if o == 2 else 0x40 if o == 1 else 0) | int(pos2[k])

        volcode = _put_vol(rec, int(vol1[k] if level == 1 else vol2[k]))
        m1 |= pbits | (volcode << 5)
        buf += bytes((m1, m2, info))
        buf += rec
    return bytes(buf)


# ---------------------------------------------------------------- header

def build_header(date, tick, ts1, mdt1, price1, vol1, ts2, side2, price2, vol2):
    """44 slots x 80 bytes. Slots 0-9 are the L1 MarketDataTypes, 10/11 the L2
    ask/bid depth. Empty slots 0-11 carry NT8's uninitialised-accumulator
    sentinels (last/max = -DBL_MAX, min = +DBL_MAX, t0 = t1 = midnight UTC of the
    file date); slots 12-43 are all zero."""
    midnight = int(datetime.strptime(date, "%Y%m%d")
                   .replace(tzinfo=timezone.utc).timestamp()) * 10_000_000 + EPOCH_TICKS
    head = bytearray(N_SLOTS * 80)
    for slot in range(12):
        if slot < 10:
            sel = mdt1 == slot
            ts, price, vol = ts1[sel], price1[sel], vol1[sel]
        else:
            sel = side2 == (slot - 10)
            ts, price, vol = ts2[sel], price2[sel], vol2[sel]
        if len(ts):
            HDR.pack_into(
                head, slot * 80,
                float(price[-1]), len(ts), float(price.max()), float(price.min()),
                float(price[0]), 1.0, tick, 1,
                to_ticks(int(ts[0])), to_ticks(int(ts[-1])),
                int(vol.sum()))
        else:
            HDR.pack_into(head, slot * 80,
                          -DBL_MAX, 0, -DBL_MAX, DBL_MAX, 0.0, 1.0, tick, 1,
                          midnight, midnight, 0)
    return bytes(head)


# ---------------------------------------------------------------- driver

def read_pair(l1_path, l2_path):
    def rd(p, cols):
        if not p.exists():
            return None
        t = pq.read_table(p)
        out = []
        for c in cols:
            a = t.column(c).to_numpy(zero_copy_only=False)
            out.append(a.astype("int64") if c == "Timestamp" else a)
        return out
    a = rd(l1_path, ["Timestamp", "MarketDataType", "Price", "Volume"])
    b = rd(l2_path, ["Timestamp", "MarketDataType", "Operation", "Position",
                     "Price", "Volume"])
    if a is None:
        a = [np.empty(0, "int64"), np.empty(0, "int8"),
             np.empty(0, "float64"), np.empty(0, "int64")]
    if b is None:
        b = [np.empty(0, "int64"), np.empty(0, "int8"), np.empty(0, "int8"),
             np.empty(0, "int32"), np.empty(0, "float64"), np.empty(0, "int64")]
    return a, b


def build_nrd(l1, l2, date, tick=None):
    ts1, mdt1_, price1, vol1 = l1
    ts2, side2, op2, pos2, price2, vol2 = l2
    if tick is None:
        tick = infer_tick(price1, price2)
    if tick <= 0:
        raise ValueError(f"could not infer a tick size for {date}")
    c1 = np.round(price1 / tick).astype("int64")
    c2 = np.round(price2 / tick).astype("int64")

    order = merge_order(ts1.tolist(), mdt1_.astype("int64").tolist(),
                        ts2.tolist(),
                        side2.astype("int64").tolist(),
                        op2.astype("int64").tolist(),
                        pos2.astype("int64").tolist())

    # cursor seeds = the header `first` values decode() will read back
    first1 = [0] * 10
    for k in range(10):
        sel = mdt1_ == k
        if sel.any():
            first1[k] = int(c1[sel][0])
    first2 = [0, 0]
    for s in (ASK, BID):
        sel = side2 == s
        if sel.any():
            first2[s] = int(c2[sel][0])

    # plain Python lists: the encode loop is per-record, and numpy scalar
    # indexing there costs more than the conversion
    body = encode_stream(order,
                         ts1.tolist(), mdt1_.astype("int64").tolist(),
                         c1.tolist(), vol1.astype("int64").tolist(),
                         ts2.tolist(), side2.astype("int64").tolist(),
                         op2.astype("int64").tolist(), pos2.astype("int64").tolist(),
                         c2.tolist(), vol2.astype("int64").tolist(),
                         first1, first2)
    head = build_header(date, tick, ts1, mdt1_, price1, vol1, ts2, side2, price2, vol2)
    return head + body, tick


def verify(raw, l1, l2):
    """Re-decode the rebuilt bytes with nrd_to_parquet's validated decoder and
    compare against the source Parquet, event for event.

    Returns (errors, max_ts_shift_ns). Timestamp differences below 100 ns are
    not errors: they are the format's .NET-tick resolution (see to_ticks), and
    are reported separately as a quantisation shift."""
    slots = parse_headers(raw[:N_SLOTS * 80])
    dec = decode(raw[N_SLOTS * 80:], slots, True, True)
    bad = []
    shift = 0
    for lvl, src, names in (("L1", l1, ("Timestamp", "MarketDataType", "Price", "Volume")),
                            ("L2", l2, ("Timestamp", "MarketDataType", "Operation",
                                        "Position", "Price", "Volume"))):
        got = dec[lvl]
        if len(got[0]) != len(src[0]):
            bad.append(f"{lvl} row count {len(got[0]):,} != {len(src[0]):,}")
            continue
        for name, a, b in zip(names, got, src):
            neq = np.nonzero(a != b)[0]
            if not len(neq):
                continue
            if name == "Timestamp":
                d = np.abs(a[neq].astype("int64") - b[neq].astype("int64"))
                shift = max(shift, int(d.max()))
                if d.max() < 100:
                    continue                     # sub-tick quantisation, not a defect
            i0 = int(neq[0])
            bad.append(f"{lvl}.{name}: {len(neq):,} diffs, first row {i0} "
                       f"({a[i0]} != {b[i0]})")
    return "; ".join(bad), shift


def main():
    ap = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    ap.add_argument("--parquet-root", default=r"M:\NinjaTrader_DataRepo\RawData\Parquet")
    ap.add_argument("--out-root", default=r"M:\NinjaTrader_DataRepo\RawData\RebuiltNRD",
                    help=r"output root; files land in <out-root>\<SYM> ##-##\<date>.nrd")
    ap.add_argument("--symbols", nargs="*", default=None)
    ap.add_argument("--seasons", nargs="*", default=None,
                    help="season folders under --parquet-root, e.g. 2025 2026")
    ap.add_argument("--start")
    ap.add_argument("--end")
    ap.add_argument("--tick", type=float, default=None,
                    help="override the inferred tick size")
    ap.add_argument("--force", action="store_true")
    ap.add_argument("--verify", action="store_true",
                    help="rebuild and re-decode to check fidelity; write nothing")
    args = ap.parse_args()

    root = Path(args.parquet_root)
    if not root.is_dir():
        sys.exit(f"parquet root not found: {root}")
    symbols = {s.upper() for s in args.symbols} if args.symbols else None

    # discover (symbol, season, date) from the L1 folders
    work = {}
    for season_dir in sorted(p for p in root.iterdir() if p.is_dir()):
        if not re.fullmatch(r"\d{4}", season_dir.name):
            continue
        if args.seasons and season_dir.name not in args.seasons:
            continue
        for lvl_dir in sorted(p for p in season_dir.iterdir() if p.is_dir()):
            m = re.fullmatch(r"(\S+)-(\d{4})_L1", lvl_dir.name)
            if not m:
                continue
            sym = m.group(1).upper()
            if symbols and sym not in symbols:
                continue
            for f in lvl_dir.glob("*.parquet"):
                if re.fullmatch(r"\d{8}", f.stem):
                    work[(sym, f.stem)] = (season_dir, m.group(2))
    if not work:
        sys.exit("no source parquet found")

    done = skipped = failed = 0
    for (sym, date), (season_dir, season) in sorted(work.items()):
        if args.start and date < args.start:
            continue
        if args.end and date > args.end:
            continue
        out_path = Path(args.out_root) / f"{sym} ##-##" / f"{date}.nrd"
        if not args.verify and out_path.exists() and not args.force:
            skipped += 1
            continue

        l1_path = season_dir / f"{sym}-{season}_L1" / f"{date}.parquet"
        l2_path = season_dir / f"{sym}-{season}_L2" / f"{date}.parquet"
        t0 = time.time()
        try:
            l1, l2 = read_pair(l1_path, l2_path)
            raw, tick = build_nrd(l1, l2, date, args.tick)
        except (ValueError, OSError) as e:
            print(f"{sym} {date}: BUILD FAILED: {e}")
            failed += 1
            continue
        secs = time.time() - t0
        n = len(l1[0]) + len(l2[0])

        if args.verify:
            diff, shift = verify(raw, l1, l2)
            if diff:
                verdict = "MISMATCH -> " + diff
            elif shift:
                verdict = f"EXACT (timestamps snapped to the 100 ns grid, max {shift} ns)"
            else:
                verdict = "EXACT ROUND TRIP"
            print(f"{sym} {date}: {n:,} events, tick={tick:g}, {len(raw):,} bytes -> "
                  f"{verdict} ({secs:.0f}s)")
            failed += 1 if diff else 0
            done += 0 if diff else 1
        else:
            out_path.parent.mkdir(parents=True, exist_ok=True)
            tmp = out_path.with_suffix(f".tmp{os.getpid()}")
            tmp.write_bytes(raw)
            os.replace(tmp, out_path)
            print(f"{sym} {date}: {n:,} events, tick={tick:g} -> {out_path} "
                  f"({len(raw):,} bytes, {secs:.0f}s)")
            done += 1
    print(f"\n{'verified' if args.verify else 'built'}: {done}, "
          f"skipped: {skipped}, failed: {failed}")
    if failed:
        sys.exit(1)


if __name__ == "__main__":
    main()
