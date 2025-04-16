# SC4023

## Project Overview

This project consists of 2 parts, initialization of the column store and querying it. Initialization includes sorting the data on month, computing indexes, and splitting into per column files. Querying includes caluclating minimum, average, standard deviation of price, and minimum price per area.

To simulate big data environmets, we use custom slice (Go equivalent of arrays), reader, and writer. At any moment we will only load and process 2000 data points for both querying and initialization of the column store. Eventhough under the hood our custom readers and writers use Go's `bufio` package which buffers disk loads, we assume that each call to our custom reader and writer would incur one I/O cost, hence effort will be made to limit calls to readers and writers.

## Project Structure

```
sc4023/
│
├── main.go                        # Main entry point
│
├── custom/
│   ├── limited_slice.go           # Custom length limited slice
│   ├── reader.go                  # Custom reader to read to limited slice
│   └── writer.go                  # Custom writer to write to limited slice
|
├── data/
│   ├── bit_map.go                 # Bit map index for exact queries
│   ├── csv.go                     # CSV related structs and utilities
│   ├── dictionary.go              # Maps for dicionary encoding
│   ├── metadata.go                # Metadata of column store
│   └── server.go                  # Zone map index for range queries
│
├── query/
│   ├── query.go                   # Structs of various query operations
│   └── runner.go                  # Entrypoint of column store query
|
├── store/
│   ├── heap.go                    # Heap data structure for external sort
│   └── store.go                   # Entrypoint of column store intialization
|
├── test/
│   ├── dictionary_test.go         # Tests dictionary encoding
│   ├── rle_test.go                # Tests results of run length encoding
│   └── sorted_test.go             # Tests results of external sort (on month)
|
├── utils/
│   ├── files.go                   # Utilities for file operations
│   ├── parse.go                   # Utilities for matric number parsing
│   └── rle.go                     # Utilities for run length encoding
|
└── README.md                      # This file
```

## Installation Guide

- Go 1.23.2 or higher

```bash
git clone https://github.com/pascalpandey/sc4023
cd sc4023
go mod tidy
```

## Running the Application

```bash
go run main.go -matric="U2220371G" -data="./ResalePricesSingapore.csv"
```

Expects raw data file to be in `./ResalePricesSingapore.csv` and column store files file to be in `./column_store`

```bash
go test ./test
```

To calculate Run Length Encoded file size compared to the original and uncompressed raw column store, run:

```bash
du -b ./column_store/rle_* 2>/dev/null | awk '{total += $1} END {print total}'
du -b ./column_store/raw_* 2>/dev/null | awk '{total += $1} END {print total}'
du -b ./ResalePricesSingapore.csv
```