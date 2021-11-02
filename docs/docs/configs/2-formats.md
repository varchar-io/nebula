---
layout: default
---

# Data Formats

Nebula supports big range of data formats to run anaytics on, it includes but limited to: CSV/TSV, JSON, THRIFT, PARQUET, GOOGLE SHEET.
A few more formats to be supported soon: Protobuf and Microsoft Excel. Adding supports new format is purely based on user requests and popularity of the format itself.
Please let us know by logging an issue in the Github project.

## CSV/TSV
Very simple but widely used data formats, human readable bug not storage efficient.
Nebula implements a CSV reader by following RFC4180, you can find Nebula CSV reader from [this source](https://github.com/varchar-io/nebula/blob/master/src/storage/CsvReader.h).

To add a CSV sourced table, you can just specify `format` as __csv__, you can aslo add csv properties to change a few supported properties
- `hasHeader`: it indiates if every csv file has first row as header. Default to true if not specified.
- `delimiter`: if the csv file(s) has different delimiter, such as TAB for TSV files. Default to comma if not specified.


Here is an example:
```yaml
tables:
  table-name-1:
    retention:
      max-mb: 1000
      max-hr: 24
    schema: "ROW<id:bigint, name:string, value: double>"
    data: s3
    loader: Roll
    source: s3://nebula/data/%7BDATE%7D/csv/
    backup: s3://nebula/n116/
    format: csv
    csv:
      hasHeader: true
      delimiter: ","
    time:
      type: macro
      pattern: daily
```
This config add a table named `table-name-1`, data ingested from s3 for upto 1 day, time specified by MACRO `DATE` value.
The data files are in CSV file format separated by comma, every csv file has header in it. The data keeps rolling daily.

## JSON
JSON is such a common format in the world of services, easy to communicate and flexbile to change as it is essentially just a string.
Nebula supports JSON data regardless the source type is static data, HTTP endpoints or realtime streaming like Kafka.

Adding a table with source of JSON format is similar to CSV, and you can use `json` config to customize properties:
- `rowsField`: which field in the JSON object to get row data payload. There are two special values:
  - `[ROOT]`: the whole payload is an array, every item is a row object.
  - "": the whole payload is a single row object.
  - "field": the field to read the array of row objects.
- `columnsMap`: this is a map of "column" name to "field path" within a row object. for example, if you have a payload like this
```json
{
  a: 1
  b: 2
  c: [
    {
      x: 10
      y: 11
    },
    {
      x: 20
      y: 21
    }
  ]
}
```
You can define a table to consume this payload as a table
```yaml
tables:
  table-name-2:
    retention:
      max-mb: 1000
      max-hr: 24
    schema: "ROW<id:int, value:int>"
    data: HTTP
    loader: Swap
    source: "http://somewhere/api/request=what"
    backup: s3://nebula/n120/
    format: json
    json:
      rowsField: "c"
      columnsMap:
        id: "x"
        value: "y"
    time:
      type: current
```

This table config basically asks Nebula to load data from given HTTP URL, in the returned payload, we extract rows from field `c`, and for each row, we get value of `x` for column `id` and value of `y` for column `value`.


## THRIFT
Similar to JSON, thrift is another common binary format used to encode a message, which is smaller and faster than JSON.
Thrift data format has two properties you can set:
- `protocol`: "binary" is the only supported thrift serialization protocol (TBinaryProtocol). Could easily add support other types such as TCompatProtocol if needed.
- `columnsMap`: similar like JSON, this mapping is column name to field ID (unsigned integer).

For example:

```yaml
tables:
  k.table:
    retention:
      max-mb: 200000
      max-hr: 12
    schema: "ROW<userId:long, magicType:short, statusCode:byte, objectCount:int>"
    data: kafka
    loader: Streaming
    source: kafkabroker.home.01
    backup: s3://nebula/n105/
    format: thrift
    kafka:
      topic: homefeed
    thrift:
      protocol: binary    
      columnsMap:
        # TODO(cao): this temporary hack to work around nested thrift definition
        # we're using 1K to separate two levels asssuming no thrift definition has more than 1K fields
        # in reverse order, such as 1003 => (field 3 -> field 1)
        _time_: 1
        userId: 3001
        magicType: 3003
        statusCode: 4002
        objectCount: 4001
```
We need some more work to support nested structure in thrift object, recommend users to try to flat your structure if possible.

## PARQUET
## GOOGLE SHEET