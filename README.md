# Nebula
A distributed cell-based data storage and compute engine
Nebula is designed for high-performance tabular data serving gateway. It serves 3 main scenarios
- Security, Privacy and Access Control
- Extreme Fast Data Analytics
- Tiered Storage Between Compute Cluster And Blob Storage For Best Efficiency

Details could be found in the [project site](https://shawncao.github.io/nebula/). 
Design, Art & Techniques will be shared through posts in the page.

# Get Started
## Run It!
Get **Nebula** run on your linux machine (Only Ubuntu Tested) in one minute through docker.
Follow the 3 simple steps documented here [test/README.md](./test/README.md)

## Build Source
Please refer [Developer Guide](./dev.md) for building nebula from source code.
Feel free to submit PR to become a contributor.

# Use Cases
## Static Data Analytics
Configure your data source from a permanent storage (file system) and run analytics on it. 
AWS S3, Azure Blob Storage are often used storage system with support of file formats like CSV, Parquet, ORC. 
These file formats and storage system are frequently used in modern big data ecosystems.

## Realtime Data Analytics
Connect Nebula to real-time data source such as Kafka with data formats in thrift or JSON, and do real-time data analytics.

## Ephemeral Data Analytics
Define a template in Nebula, and load data through Nebula API to allow data live for specific period. 
Run analytics on Nebula to serve queries in this ephemeral data's life time.

## Sparse Storage
Highly break down input data into huge small data cubes living in Nebula nodes, usually a simple predicate (filter) will massively 
prune dowm data to scan for super low latency in your analytics.

## Ad-hoc Data Analytics Platform
This is a public platform facing to individuals, its goal is to enable every single user to write a few lines of javascript code to analyze terabytes of data in seconds. (Not Online Yet. Keep an eye on the launch event.)

Basically, on this new platform, you can write short lines of JS code like below to analyze your big data in seconds.
```javascript
    // const values
    const name = "test";
    const schema = "ROW<a:int, b:string, c:bigint>";

    // define an customized column
    const colx = () => nebula.column("a") % 3;
    nebula.def("colx", nebula.Types.INT, expr);

    // get a data set from data set stored in HTTPS or S3
    const data = nebula.csv("https://varchar.io/test/data.csv")
                     .table(name, schema)
                     .select(col("colx"), count(1).as("count"))
                     .where(col("_time_") >= 104598762 && col("_time_") <= 108598762)
                     .groupby(1)
                     .orderby(2, nebula.SORT.DESC);

    // render the data result with PIE chart with x-axis from "colx" and y-axis from "count"
    data.pie("colx", "count");
```