---
layout: default
---

# Common Configs
If you have seen the examples in basics/overview, you probably already know how to add a configs to connect different data sources from Nebula.

In this section, we will list all common configs and explain thier usage.

## Config Structure
The whole cluster config is a yaml file which you specified for NebulaServer to start with (eg. `--CLS_CONF cluster.yaml`), in the config, there are just a few major sections:
1. `version`: version value of the config
2. `server`: the nebula cluster settings, useful sub-configs are:
   1. `anode`: boolean value - treat nebula server as one of the normal nebula node, default to "false".
   2. `auth`: boolean value - indicating if nebula query execution will enforce auth check. If set true, the necessary auth model needs to be set up in the same config, check basics/access_control for details, default to "false".
   3. `meta`: specify what metadata tracking service to use, it's either an external meta db service or the built-in one implemented using LevelDB + cloud storage (such as S3) for data persistence. It can be used to store nebula shortened URL and some other server running metrics, default to none.
   ```yaml
    # configure meta db as none
    meta:
      db: none
   ```
   or 
   ```yaml
    # use the built-in metadb and use s3://nebula/meta as the data store.
    meta:
      db: native
      store: s3://nebula/meta
   ```
   4. `discovery`: config how nebula cluster is discover each other between Nebula Server and Nebula Nodes. Only two options provided so far, check basics/discovery for details.
   ```yaml
     # most common way - let nebula nodes connect nebula server an self-register itself
     discovery:
      method: service
   ```
   or 
   ```yaml
    # useful for static setup with known address for all nebula nodes
    discovery:
      method: config
   ```
   when `method: config` is specified, all nebula nodes needs to configured in this same config file, as shown in the next config
3. `nodes`: configs to list all static nebula nodes, this is only used when discover/method specified as `config`, here is an example:
```yaml
# will be provided by environment
nodes:
  - node:
      host: neula.node.1
      port: 9199
  - node:
      host: neula.node.2
      port: 9199
```
4. `tables`: this is the most active configured section to add/change/update all the data sources available for current nebula cluster. It includes all tables definition.
A table could be backed by a cloud storage location, could be backed by a real time streaming, could be even backed by a HTTP API endpoint or simply a data service like Google Spreadsheet.
This config will also include `data format` settings (csv, json, thrift, parquet, spreadsheet, ...) as well as `data rolling policy`.

We will use following documentations to describe each different ways to connect different data source.
```yaml
tables:
  table-name-1:
    {configs}
  table-name-2:
    {configs}
  ...
```

## Resource Constraints

For each table, we can define resource constraints for it, this is required for rolling tables, so that Nebula only keeps the most recent data in memory, data past the contraints will be either erased or backup for future query.

`retention` is used to specify maximum size and maximum time window for the table, whichever reaches firstly, nebula will purge out the old data segments.

Let's take a look at one example:
```yaml
tables:
  table-name-1:
    retention:
      max-mb: 10000
      max-hr: 0.5
```
This config asks nebula cluster to keep maximum 10GB of data for `table-name-1`, or maximum half an hour of the data if applicable. For real time streaming, this means, Nebula serves `last 30minutes` of data.
- `max-mb`: MegaBytes - an integer.
- `max-hr`: hours - an double, converted to seconds internally.


## Schema
Schema is needed for every single table definition, regardless where the data is coming from or what the data format is. It's defined in a string of serialized format of schema.

Here is an exmaple:
```yaml
tables:
  table-name-1:
    schema: "ROW<id:int, event:string, tag:string, items:list<string>, flag:bool, value:tinyint>"
```
Basically, it is a collection of `name:type`. Nebula is `hive` data types compatible, so here is the list supported, some type has multiple aliaes, whichever alias you use, it's effectively the same:
1. bool, boolean
2. tinyint, byte, char
3. smallint, short
4. int, integer
5. bigint, long
6. real, float
7. double, decimal
8. int128
9. varchar, string, binary
10. array, list, vector
11. map, dictionary
12. row, struct

Note that, compound types like array/map/struct are not well tested so far.

## Data, Loader, Source, Backup, Format
These are common configs for each table definition, depending on different type of data, their values vary a lot.
- `data`: this defines data source type, it could be one of these supported types - 
  - `s3`: data from AWS S3 
  - `gs`: data from Google Cloud Storage
  - `kafka`: data from kafka real time streaming.
  - `local`: data from a local file system (mostly used for single node testing)
  - `http`: data from a HTTP endpoint

- `loader`: this config indicates how Nebula loads data into nebula cluster, it's high level data ingestion policy, here are some possible values:
  - `Swap`: the table has one location to read data from, whever it changes, Nebula swaps new data in.
  - `Roll`: this is mostly used, it ask Nebula to compute how many different locations it could read data from under the `Resource Contraints`, and generate ingestion specs to roll data in, it will evict old data past the contraints and ingest new data in whenever it's available.
  - `Api`: used mostly for on-demand data loading when a client use API to ask Nebula to load some data

- `source`: source is usually address, path or location template.

for example, a S3 data could define source like below
```yaml
tables:
  table-name-1:
    data: s3
    loader: Roll
    source: s3://bucket/path/%7BDATE%7D/part=abc/
    backup: s3://nebula/n107/
    format: parquet
```
this definition asks Nebula to load data from path `s3://bucket/path/%7BDATE%7D/part=abc/` with `DATE` as a macro to compute real path based on `max-hr` definition from resource contraints, and the data has `parquet` file format, under the folder, there are one or more parquet files.

another example, let's look at how Kafka works
```yaml
tables:
  table-name-1:
    data: kafka
    loader: Streaming
    source: <brokers>
    backup: s3://nebula/n116/
    kafka:
      topic: topic-1
```
in this case, `source` is used to place Kafka broker list that Nebula could connect to fetch data from `topic-1`.

- `backup`: is always an accessible location where Nebula can backup the data to when those data become evictable due to resource contraints. Nebula doesn't practice this often till today, maybe supported much better in the future when it seeks support for querying historical data.

- `format`: data format - the wide range and growing data format to support in Nebula so that we can ingest all types of data sources and query it in a single interface. Till writing time, the supported formats include:
  - CSV
  - JSON
  - THRIFT
  - PARQUET
  - GOOGLE SHEET

We will expand the format specific settings in the post for "data format".

## Time
Time section is very important - as it impacts how Nebula organize the data internally, Nebula has an implicit built-in time column (`_time_`) which is available for every single table.
Also for most rolling data, they rely on `time` definition to materialize those time related macro to compute real data location to read data from.

Similarily, let's use examples to explain all different ways to define time.

Example 1: use a static value for time
```yaml
tables:
  table-name-1:
    time:
      type: static
      value: 1571875200
```
Value is specfified as unix time in seconds, in this case, all table rows will have the same value.

Example 2: use current time
```yaml
tables:
  table-name-1:
    time:
      type: current
```
Only type is needed, Nebula will put ingestion time of each row to its _time_ column.

Example 3: use a column value as _time_ column
```yaml
tables:
  table-name-1:
    time:
      type: column
      column: col2
      pattern: UNIXTIME_NANO
```
In this case, Nebula will convert an existing column into the value of _time_ column. For this type, column name is needed and value pattern is needed as well for a successful conversion.
- `column`: the column name from "schema" definition discussed previously.
- `pattern`: the supported time value format for correct conversion, this includes:
  - "UNIXTIME": column value is `unix time in seconds`.
  - "UNIXTIME_MS": column value is `unix time in milli seconds`.
  - "UNIXTIME_NANO": column value is `unix time in nano seconds`.
  - "SERIAL_NUMBER": column value is in `serial number` format which is usually defined by excel or google spreadsheet data.
  - "C++ time format": string pattern which Nebula uses "std::get_time" to convert, such as "%m/%d/%Y %H:%M:%S", or "%Y-%m-%d", check pattern details https://en.cppreference.com/w/cpp/io/manip/get_time


Example 4: use macro value for time column
To support rolling data source from a cloud storage, Nebula supports list of macros from its source template, which is usually constructed by different time units.
```yaml
  nebula.daily:
    # the source has supported macro in it
    source: s3://nebula/ephemeral/dt=%7Bdate%7D/
    time:
      type: macro
      pattern: daily
```
In this case, Nebula will generate real source like `s3://nebula/ephemeral/dt=2021-01-01/`, and use day time level value to set its _time_ column which is equivalent to `2021-01-01`.

Similarily, all supported granularity of time macros in path could be combination of these:
- DATE
- HOUR
- MINUTE
- SECOND

## Settings
Lastly, Nebula has a very generic config called `settings`, this will be used as extension to accept any key-value pairs to impact Nebula behavior for the defined table. Both key and value are in string value, though sometimes we use a JSON string to hold a structure for more complex configurations. 

We will touch these "misc" configs while talking about other scenarios.