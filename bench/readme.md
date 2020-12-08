benchmark nebula

People has been asking the benchmark comparing to other engine such as druid.
This note is trying to reproduce the similar benchmark druid did before and show some initial results.

# Benchmark Data & Query
The data is standard 100GB TPC-H benchmark source.
- data: https://gist.githubusercontent.com/xvrl/9552286/raw/77a134359ba73eaffcb30de0792ed3f19c209535/download-data.sh
- schema: https://github.com/druid-io/druid-benchmark/blob/master/lineitem.task.json#L14-L29

Druid benchmark result: https://druid.apache.org/blog/2014/03/17/benchmarking-druid.html (a bit old)

## Prepare data
Download the data and use this spec to load it into Nebula test cluster with 3 EC2 c5.4x machines (16 cores).
- download: `for i in $(seq 1 100) ; do curl -O http://static.druid.io/data/benchmarks/tpch/100/lineitem.tbl.$i.gz ; done`
- decompress: `for i in $(seq 1 100) ; do gzip -d lineitem.tbl.$i.gz; done`
- (optional) upload to s3: `for i in $(seq 1 100) ; do s3 cp lineitem.tbl.$i s3://nebula/bench/lineitem.tbl.$i; done`

## Ingest spec
```
  nebula.bench:
    max-mb: 10000
    max-hr: 0
    schema: "ROW<l_orderkey:bigint, l_partkey:bigint, l_suppkey:bigint, l_linenumber:int, l_quantity:int, l_extendedprice:float, l_discount:float, l_tax:float, l_returnflag:string, l_linestatus:string, l_shipdate:string, l_commitdate:string, l_receiptdate:string, l_shipinstruct:string, l_shipmode:string, l_comment:string>"
    data: s3
    loader: Swap
    source: s3://pinlogs/nebula/bench1/
    backup: s3://nebula/n109/
    format: csv
    columns:
      l_returnflag:
        dict: true
      l_linestatus:
        dict: true
      l_shipinstruct:
        dict: true
      l_shipmode:
        dict: true
    time:
      type: column
      column: l_shipdate
      pattern: "%Y-%m-%d"
    settings:
      csv.delimiter: "|"
      csv.header: false
```



