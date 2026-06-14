<div align="center">

<h3>NRDToCSV</h3>
  
![image](https://user-images.githubusercontent.com/14204888/132671281-c7f68d43-cbfa-47da-87db-3092c60ec55c.png)

</div>

#
**NinjaTrader 8** AddOn to convert NRD (`*.nrd`) market replay files to CSV (`*.csv`)<br>
(based on [not yet documented][market-data] `MarketReplay.DumpMarketDepth` feature)

## Quick Start

1. Download the latest **zip file** with AddOn from [Releases][releases]
2. Import AddOn into NinjaTrader 8 via `Tools` / `Import` / `NinjaScript Add-On...`
3. Open NRD to CSV tool via `Tools` / `NRD to CSV`
5. Press **Convert** button to convert all `*.nrd` replay files (could take some time to proceed)
6. Check `Documents` \ `NinjaTrader 8` \ `db` \ `replay.csv` folder with the results

## Select instruments to convert

Instruments found in the NRD replay folder are listed as checkboxes; only checked
instruments are converted. Use **All** / **None** to toggle every instrument at once.

For large lists, you can bulk-select instruments with semicolon-separated regular
expressions (experiment [here][regex]) matched against the instrument name, then press
**Check** or **Uncheck** to apply:

- Select only Gold Commodity futures: `GC`
- Select several instruments: `GC; HG; 6E`
- Select all MNQ contracts expiring in March, June, September or December of 2021/2022: `^MNQ (03|06|09|12)-2[12]$`

## Converted `*.csv` file format

### File and folder layout
Each `*.nrd` replay file is converted into one `*.csv` file, written to:

```
<CSV root directory>\<Instrument FullName>\<YYYYMMDD>.csv
```

For example, the 2021-01-20 replay of the March 2021 E-mini S&P 500 contract
becomes `replay.csv\ES 03-21\20210120.csv`.

### Content example
Each line is a single tick / depth-of-market event. Fields are **semicolon (`;`)
separated**, there is **no header row**, and `L1` and `L2` records have a
**different number of fields** (see below) - keep this in mind when loading the
file, e.g. with `pandas` (see [Importing into pandas](#importing-into-pandas)).

```csv
L1;0;20210120050050;2300000;1855.8;2
L1;1;20210120050107;2140000;1855.4;8
L2;0;20210120050000;70000;0;0;;1855.5;1
```

### L1 Records
- `NinjaTrader.Data.MarketDataType`
```csharp
Ask = 0
Bid = 1
Last = 2
DailyHigh = 3
DailyLow = 4
DailyVolume = 5
LastClose = 6
Opening = 7
OpenInterest = 8
Settlement = 9
Unknown = 10
```
- `Timestamp` in `YYYYMMDDhhmmss` format (local NinjaTrader timezone is used)
- `Timestamp offset` as an integer amount of 100-nanoseconds (`1e-7`)
- `Price` value (local NinjaTrader price format is used for thousand/decimal separators)
- `Volume` value

### L2 Records
- `NinjaTrader.Data.MarketDataType`
```csharp
Ask = 0
Bid = 1
Last = 2
DailyHigh = 3
DailyLow = 4
DailyVolume = 5
LastClose = 6
Opening = 7
OpenInterest = 8
Settlement = 9
Unknown = 10
```
- `Timestamp` in `YYYYMMDDhhmmss` format (local NinjaTrader timezone is used)
- `Timestamp offset` as an integer amount of 100-nanoseconds (`1e-7`)
- `NinjaTrader.Cbi.Operation`
```csharp
Add = 0
Update = 1
Remove = 2
```
- `Position` in Order Book
- `MarketMaker` identifier
- `Price` value (local NinjaTrader price format is used for thousand/decimal separators)
- `Volume` value

## Importing into pandas

Because each file has **no header row**, uses `;` as separator and mixes two
record layouts with a different field count (`L1` has 6 fields, `L2` has 9),
`pd.read_csv()` cannot parse a file directly. Split the `L1` and `L2` lines
first, then build a `DataFrame` for each record type.

### Minimal loader

```python
import pandas as pd

L1_COLUMNS = ["MarketDataType", "Timestamp", "TimestampOffset", "Price", "Volume"]
L2_COLUMNS = ["MarketDataType", "Timestamp", "TimestampOffset", "Operation",
              "Position", "MarketMaker", "Price", "Volume"]

def load_replay_csv(path):
    l1_rows, l2_rows = [], []
    with open(path, newline="") as f:
        for line in f:
            fields = line.rstrip("\n").split(";")
            if fields[0] == "L1":
                l1_rows.append(fields[1:])
            elif fields[0] == "L2":
                l2_rows.append(fields[1:])

    l1 = pd.DataFrame(l1_rows, columns=L1_COLUMNS)
    l2 = pd.DataFrame(l2_rows, columns=L2_COLUMNS)
    return l1, l2

l1, l2 = load_replay_csv(r"replay.csv\ES 03-21\20210120.csv")
```

### Recommended dtypes and decoding

Combine `Timestamp` (second precision) and `TimestampOffset` (100ns units, i.e.
.NET ticks) into a single `datetime64[ns]` column:

```python
for df in (l1, l2):
    df["Timestamp"] = (
        pd.to_datetime(df["Timestamp"], format="%Y%m%d%H%M%S")
        + pd.to_timedelta(df["TimestampOffset"].astype("int64") * 100, unit="ns")
    )
    df.drop(columns="TimestampOffset", inplace=True)
```

Map the `MarketDataType` / `Operation` enums to readable categories and convert
the remaining numeric columns:

```python
MARKET_DATA_TYPE = {0: "Ask", 1: "Bid", 2: "Last", 3: "DailyHigh", 4: "DailyLow",
                     5: "DailyVolume", 6: "LastClose", 7: "Opening",
                     8: "OpenInterest", 9: "Settlement", 10: "Unknown"}
OPERATION = {0: "Add", 1: "Update", 2: "Remove"}

for df in (l1, l2):
    df["MarketDataType"] = df["MarketDataType"].astype(int).map(MARKET_DATA_TYPE).astype("category")
    df["Price"] = df["Price"].astype(float)
    df["Volume"] = df["Volume"].astype("int64")

l2["Operation"] = l2["Operation"].astype(int).map(OPERATION).astype("category")
l2["Position"] = l2["Position"].astype("int32")
l2["MarketMaker"] = l2["MarketMaker"].replace("", pd.NA)
```

> **Decimal separator:** prices are written using NinjaTrader's regional number
> format. If your installation uses a comma as the decimal separator (e.g.
> `1855,8` instead of `1855.8`), convert with
> `df["Price"].str.replace(",", ".").astype(float)` instead of `astype(float)`.

### Loading a whole instrument / multiple days

The CSV root directory mirrors the replay folder layout described above
(`<root>\<Instrument FullName>\<YYYYMMDD>.csv`), so it's straightforward to glob
multiple days/instruments and tag each row with the instrument and date:

```python
import glob
import os

frames = []
for path in glob.glob(r"replay.csv\ES *\*.csv"):
    instrument = os.path.basename(os.path.dirname(path))
    date = os.path.splitext(os.path.basename(path))[0]
    l1, _ = load_replay_csv(path)
    l1["Instrument"] = instrument
    l1["Date"] = pd.to_datetime(date, format="%Y%m%d")
    frames.append(l1)

ticks = pd.concat(frames, ignore_index=True)
```

### Performance notes

- `L2` (order book) files are much larger than `L1` files. If you only need
  top-of-book/trade data, don't append `L2` lines to `l2_rows` at all - this
  cuts memory use and load time significantly.
- For very large files, avoid materializing all rows in memory: process the
  file in chunks (e.g. with `csv.reader`) and feed batches to `pd.concat`, or
  use `pyarrow`/`polars` for the initial parse if pandas becomes too slow.
- Reconstructing the live order book from `L2` rows requires replaying `Add` /
  `Update` / `Remove` operations in timestamp order per
  `(MarketDataType, Position)`. This is inherently sequential and isn't a
  single vectorized pandas call - the cleaned-up `l2` DataFrame above is the
  input you'd iterate over (e.g. with `itertuples()` or a `groupby`-based
  apply) to build book snapshots.

## Change Log
This project adheres to [Semantic Versioning][semver].<br>
Every release, along with the migration instructions, is documented on the GitHub [Releases][releases] page.

## License
The code is available under the [MIT license][license].

## Contacts
Feel free to contact me at **@gmail.com**: **eugene.ilyin**

[releases]: https://github.com/eugeneilyin/nrdtocsv/tree/main/Releases
[license]: /License.txt
[semver]: http://semver.org
[market-data]: https://ninjatrader.com/support/forum/forum/ninjatrader-8/platform-technical-support-aa/1067384-more-info-on-marketreplay-dumpmarketdata-marketreplay-dumpmarketdepth
[regex]: https://regex101.com/r/8EqW6n/2
