---
layout: default
---

# SDK: javascript
Nebula provides native UI to allows user to browse data samples and run analytics through aggregation computing.
However, a lot of times, we need to transform and customize the existing column data into a desired state.
For example, we want to trim some long string literals into a fixed-size prefix and aggregate by it.

Nebula SDK enables users to write code to run analytics as wish.
## Demo Code Snippet
This demo code snippet is already shown in project homepage on Github, but let's revisit it again and explain all available functions provided by the SDK.

```javascript
    // const values
    const name = "test";
    const schema = "ROW<a:int, b:string, c:bigint>";

    // define an customized column
    const colx = () => nebula.column("a") % 3;
    nebula.apply("colx", nebula.Type.INT, colx);

    // get a data set from data set stored in HTTPS or S3
    const query = nebula.source(name)
                     .time("2020-06-22 15:00:00", "2020-06-25 15:00:00")
                     .select("colx", count("id"))
                     .sortby(nebula.Sort.DESC)
                     .limit(100);

    // render the data result with a table
    query.table();
```

## Nebula SDK
The basic idea is to allow user to define any new column through a ES6 JS function.

### namespace
All SDK functions are defined inside nebula "namespace".

To simplify code user to write, we also defined aliases for all native aggregation functions, they are

| Function |                      Definition                       |                                                                                     Note |
| -------- | :---------------------------------------------------: | ---------------------------------------------------------------------------------------: |
| count    |                 count number of rows                  | column name is required to be consistent with all other functions, though it is not used |
| sum      |                 sum the given column                  |                             only number columns including floating numbers are supported |
| max      |               max value in given column               |                                                                                          |
| min      |               min value in given column               |                                                                                          |
| avg      |             average value in given column             |                                                                                          |
| p10      |        percentile value of given column at 10%        |                         arbitrary percentile is available yet, but a few often used ones |
| p25      |        percentile value of given column at 25%        |                                                                                          |
| p50      |        percentile value of given column at 50%        |                                                                                          |
| p75      |        percentile value of given column at 75%        |                                                                                          |
| p90      |        percentile value of given column at 90%        |                                                                                          |
| p99      |        percentile value of given column at 99%        |                                                                                          |
| p99_9    |       percentile value of given column at 99.9%       |                                                                                          |
| p99_99   |      percentile value of given column at 99.99%       |                                                                                          |
| tree     | aggregate single paths into a tree with counted nodes |  the expected column should be string type, with lines break by \n, with root at the top |

### column
`nebula.column("{col}")` is API to be used in your custom code logic to compute new column values. It evaluated as the specified column value in runtime.

### apply
`nebula.apply` is required to be called to register a new custom column. It takes 3 arguments
- new column name
- new column type
- function/lambdd that defines the logic how the column value will be computed. Either `function x(){}` or `const x = () => {}` works, but lambda is preferable.

### Type
`nebula.Type` is a enum list defines all supported colummn types
- INT:      32bit integer
- LONG:     64bit integer
- FLOAT:    32bit floating number
- DOUBLE:   64bit floating number
- STRING:   string value

### query
`nebula` defines a list of query related functions to help construct a SQL-like query.

| Function |                                          Definition                                          |                                                                                                                                           Note |
| -------- | :------------------------------------------------------------------------------------------: | ---------------------------------------------------------------------------------------------------------------------------------------------: |
| source   |                            define what data source/table to query                            |                                                                  it should be an existing table or a runtime generated table through other API |
| time     |                           defines time range of the data to query                            | it accepts both UNIX time value in milliseconds, or the string literals. <br/> e.g `2020-06-22 13:00:00` or `new Date("2020-06-22").getTime()` |
| select   | var args defined by `...arg` format in JS, you can pass column name, or aggregation function |                                                                                                       special validation rules may be enforced |
| sortby   |                            specify sort type, ASC, DESC, or NONE                             |                                                                                            currently only supporting first column to be sorted |
| limit    |                        specify maximum number of rows to be returned                         |                                                                                                                                                |

### Sort
SORT type is another enum object defined in nebula. Referenced by `nebula.Sort.ASC`, `nebula.Sort.DESC` or `nebula.Sort.NONE`
- ASC:      ascending order
- DESC:     descending order
- NONE:     no sorting 
  
### display
`nebula` defines a list of APIs to allow display the query result in selected visual type.

| Function |                  Definition                  |                                                                                              Note |
| -------- | :------------------------------------------: | ------------------------------------------------------------------------------------------------: |
| samples  |    browes sample rows of selected columns    |                                                 no aggregation function is allowed in select list |
| table    |        display result in table format        |                                                              at least one aggregation is required |
| pie      |        display result in a pie chart         |                                                  first aggregated column is used to slice the pie |
| bar      |        display result in a bar chart         |                                                                   first aggregated column is used |
| line     |        display result in a line chart        |                                                                   first aggregated column is used |
| timeline | display result in a time line sliced by time |                            time interval is auto computed based on suitable number of data points |
| flame    |        display result in flame graph         | this is special visual working for `tree` aggregation function only for meaningful flame analysis |

# Thanks
Without the great project QuickJS, we won't make this available in such an efficient way.