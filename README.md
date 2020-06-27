# Nebula
A distributed cell-based data storage and compute engine
Nebula is designed for high-performance tabular data serving gateway. It serves 3 main scenarios
- Security, Privacy and Access Control
- Extreme Fast Data Analytics
- Tiered Storage Between Compute Cluster And Blob Storage For Best Efficiency

Details could be found in the [project site](https://shawncao.github.io/nebula/). 
Design, Art & Techniques will be shared through posts in the page.


[![Click to watch a 3 minutes short video of Nebula Quick Play](https://img.youtube.com/vi/ZstsfiUQYOc/0.jpg)](https://youtu.be/ZstsfiUQYOc "Nebula Quick Play")


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

## Nebula Is Programmable
Through the great projecct QuickJS, Nebula is able to support full ES6 programing through its simple UI code editor.
Below is an snippet code that generates a pie charts for your SQL-like query code in JS.

On the page top, the demo video shows how nebula client SDK is used and tables and charts are generated in milliseconds!

```javascript
    // const values
    const name = "test";
    const schema = "ROW<a:int, b:string, c:bigint>";

    // define an customized column
    const colx = () => nebula.column("a") % 3;
    nebula.apply("colx", nebula.Type.INT, colx);

    // get a data set from data set stored in HTTPS or S3
    const query = nebula.csv("https://varchar.io/test/data.csv")
                     .source(name)
                     .time("2020-06-22 15:00:00", "2020-06-25 15:00:00")
                     .select("colx", count("id"))
                     .sortby(nebula.Sort.DESC)
                     .limit(100);

    // render the data result with PIE chart
    query.pie();
```