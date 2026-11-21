# Shopping Sessions: native Rust pipeline

Implements `rules.md` of the Buildathon Databricks "Shopping Sessions" track as one
Rust binary. The image's `CMD` is `/app/ss`; it reads `$DATA_DIR` (default `/data`)
and writes `$OUTPUT_DIR/user_daily/*.parquet` and `$OUTPUT_DIR/campaign_daily/*.parquet`.

## How it works

1. **Inputs are memory-mapped** and split into ~8 MB chunks of whole lines. A
   64-byte AVX2 scan finds `,` `"` and `\n` and applies the CSV dialect exactly
   (quotes only around a whole field, `\r` is data, wrong field counts dropped).
2. **Links** are parsed on a dedicated thread and joined with union-find; every
   root holds the byte-wise smallest ID of its set. The result is an
   open-addressing table (ID hash -> person name), probed with prefetching.
3. **Events** are parsed in parallel; each line is validated once (timestamp
   forms F1/F2/F3, window, type, price, late check) and its `event_id` and
   `user_id` are hashed with xxh3-128. Records are hash-partitioned by
   `event_id` (256 partitions).
4. **Dedup** per partition keeps the copy with the smallest `ingest_ts`
   (before validation), then resolves each survivor's person and
   re-partitions by person.
5. **Clicks** are parsed, validated and partitioned by person the same way
   (duplicates have identical values, so they never change a result).
6. **Per person partition**: counting sort by person, sort by `ts`,
   sessionize (gap > 30 min), credit each session with a two-pointer window over
   the person's clicks (latest `ts`, then smallest `click_id`), count assists
   (an interval sweep for people with very many clicks), and aggregate
   `user_daily` rows and per-(campaign, day) counters.
7. Parquet is written uncompressed with arrow-rs, one `user_daily` file per
   group of partitions and one `campaign_daily` file.

## Verification

- All 36 starter fixtures and `make check-dev` (seed 1) match.
- `tools/ref.py` is an independent, slow Python implementation written straight
  from `rules.md`; it matches the dev answer, and the Rust binary matches it on
  13 other generated seeds (including windows containing 2028-02-29).

## Local run

    cargo build --release
    DATA_DIR=dev OUTPUT_DIR=out target/release/ss
