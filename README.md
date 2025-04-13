# SC4023

```bash
go run main.go -matric="U2220371G" -data="./ResalePricesSingapore.csv"
```

Expects raw data file to be in `./ResalePricesSingapore.csv` and sorted file to be in `./column_store/sorted.csv`

```bash
go test ./test
```

To calculate Run Length Encoded file size compared to the original and uncompressed raw column store, run:

```bash
du -b ./column_store/rle_* 2>/dev/null | awk '{total += $1} END {print total}'
du -b ./column_store/raw_* 2>/dev/null | awk '{total += $1} END {print total}'
du -b ./ResalePricesSingapore.csv
```