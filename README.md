# Nebula - Super Fast & Flexible OLAP
A distributed block-based data storage and compute engine
Nebula is designed as a high-performance columnar data storage and tabular OLAP engine.
It can do more than
- Extreme Fast Data Analytics Platform.
- Column Level Access Control Storage System.
- Distributed Cache Tier For Tabular Data.

Documents of design, internals and stories will be added to [project site](https://nebula.bz).

## Demo
### 10 minutes video
[![Click To Watch Nebula Demo Video](./test/nebula-rep.png)](https://youtu.be/ciYD73z6Eiw "Nebula Demo")

### generating bar chart from 100GB data in 600ms
![Generate bar from 700M rows in 600ms](./test/nebula-rep2.png)

### Write an instant javascript function in real-time query.
![Transform column, aggregate by it with filters](./test/nebula-ide.png)


# Get Started
## Run It!
- clone the repo: `git clone https://github.com/varchar-io/nebula.git`
- run run.sh in source root: `cd nebula && ./run.sh`
- explore nebula UI in browser: `http://localhost:8088`


## Build Source
Please refer [Developer Guide](./dev.md) for building nebula from source code.
Welcome to become a contributor.

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

## Nebula Is Programmable
Through the great projecct QuickJS, Nebula is able to support full ES6 programing through its simple UI code editor.
Below is an snippet code that generates a pie charts for your SQL-like query code in JS.

On the page top, the demo video shows how nebula client SDK is used and tables and charts are generated in milliseconds!

```javascript
    // define an customized column
    const colx = () => nebula.column("value") % 20;
    nebula.apply("colx", nebula.Type.INT, colx);

    // get a data set from data set stored in HTTPS or S3
    nebula
        .source("nebula.test")
        .time("2020-08-16", "2020-08-26")
        .select("colx", count("id"))
        .where(and(gt("id", 5), eq("flag", true)))
        .sortby(nebula.Sort.DESC)
        .limit(10)
        .run();
```
