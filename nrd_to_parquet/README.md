# nrd_to_parquet — convert NT8 Market Replay to backtester Parquet yourself

One-step converter: reads NinjaTrader 8 Market Replay `.nrd` files directly
(reverse-engineered binary format, no NinjaTrader needed, no CSV middle step)
and writes day files in exactly the layout the backtester reads — both L1 trades
and L2 depth:

```
M:\NinjaTrader_DataRepo\RawData\Parquet\<SEASON>\<SYMBOL>-<SEASON>_L1\<YYYYMMDD>.parquet
M:\NinjaTrader_DataRepo\RawData\Parquet\<SEASON>\<SYMBOL>-<SEASON>_L2\<YYYYMMDD>.parquet
```

`<SEASON>` is the roll-season year (quarterly financials roll on the Monday
before December's 3rd Friday; other symbols use the calendar year), matching the
repo's `Parquet\<YEAR>\` layout.

Output is tagged `replay_importer.timestamps=UTC`, so downstream code treats it
identically to files from the original NRD→CSV→Parquet pipeline. Both L1 and L2
were validated event-for-event (timestamp, type, price, volume, and for L2
operation/position/side) against that pipeline's output across **every traded
symbol** — exact match wherever the source `.nrd` is the same capture.

## Requirements

Python 3.9+ with `numpy` and `pyarrow` (`pip install numpy pyarrow`).

## Convert new days (the usual thing)

```powershell
python nrd_to_parquet.py
```

That's it. It scans your NinjaTrader replay folder, finds every day file for
every instrument, skips days already in the Parquet repo, and converts the rest
to **both L1 and L2**. Roughly 30–60 s per MNQ day, ~10 s per MYM day. Run it
whenever you've downloaded fresh replay data in NinjaTrader.

Paths are configurable; the defaults are `--replay-dir
%USERPROFILE%\Documents\NinjaTrader 8\db\replay` and `--out-root
M:\NinjaTrader_DataRepo\RawData\Parquet`. Override them for your own setup:

```powershell
python nrd_to_parquet.py --replay-dir "D:\NinjaTrader 8\db\replay" --out-root "D:\Parquet"
```

Useful variants:

```powershell
# only some symbols / a date range
python nrd_to_parquet.py --symbols MNQ --start 20260704
# only one record level (default writes both L1 and L2)
python nrd_to_parquet.py --levels L1
# double-check a day against an existing file instead of writing
python nrd_to_parquet.py --validate --symbols MNQ --start 20260601 --end 20260603
# re-convert (overwrite) existing days — normally leave this off
python nrd_to_parquet.py --force
# bulk rebuild from the Continuous archive instead of db\replay
python nrd_to_parquet.py --continuous-root "M:\NinjaTrader_DataRepo\RawData\Continuous" --years 2025 2026
```

## Things worth knowing

- **Don't `--force` over the old repo days.** NinjaTrader occasionally
  revises replay data server-side, so a fresh dump of an old day can differ
  microscopically from what was converted back then (seen: cumulative daily
  volume offset by a few contracts, a few summary-event timestamps shifted
  ~10–90 ms). The default skip-existing behavior keeps the repo stable.
- **Truncated replay days happen** (e.g. `MYM 06-26\20260527.nrd` stops at
  16:17 ET — NinjaTrader stopped refreshing the expiring contract). The
  existing repo file for that day is complete; the skip rule protects it too.
- When a date exists in two contract folders (roll weeks, e.g. `MNQ 06-26`
  and `MNQ 09-26`), the converter picks the larger file = the front/most
  active contract, which matches how the repo was built.
- Both **L1** (Ask/Bid/Last/DailyHigh/DailyLow/DailyVolume/LastClose/Opening/
  OpenInterest/Settlement) and **L2** market-depth records are exported by
  default. Use `--levels L1` (or `--levels L2`) to write just one. L2 columns:
  `MarketDataType` (0 ask / 1 bid), `Operation` (0 add / 1 update / 2 remove),
  `Position`, `MarketMaker` (always null for these futures), `Price`, `Volume`.
  Both levels were validated event-for-event against the old gbNRDtoCSV ->
  `replay_csv_to_parquet` pipeline across every traded symbol.
- Prices are rounded to the instrument's tick precision, so 0.1-tick symbols
  (GC, MGC, RTY, M2K) reconstruct e.g. `4342.9` exactly rather than
  `4342.900000000001` — matching the CSV pipeline bit-for-bit.
- Timestamps inside `.nrd` files are already UTC (.NET ticks, 100 ns), so no
  timezone conversion happens at all — the wall-clock bug class from the old
  CSV pipeline can't occur here.
- The full reverse-engineered format spec lives in the docstring of
  [nrd_to_parquet.py](nrd_to_parquet.py). If a file uses an encoding variant
  never seen in the validation corpus, or a record can't be aligned, the
  converter stops that day with a loud `DECODE FAILED ...` (e.g. `unknown volume
  code` or `stream not fully consumed`) rather than writing wrong data, and
  moves on to the next day.
- Header event counts are treated as a size hint, not gospel — some `.nrd`
  files under-report their true event count, and the converter reads the whole
  stream regardless.
