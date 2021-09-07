---
layout: default
---

# SDK: javascript
Nebula provides native UI to allows user to browse data samples and run analytics through aggregation computing.
However, a lot of times, we need to transform and customize the existing column data into a desired state.
For example, we want to trim some long string literals into a fixed-size prefix and aggregate by it.

Nebula SDK enables users to write code to run analytics as wish.

## Example Code Snippet
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
                     .where(and(eq("coly", true), gt("colz", 25)))
                     .sortby(nebula.Sort.DESC)
                     .limit(100);

    // render the data result with a table
    // other visuals can be achieved by different API: bar, pie, line, timeline(seconds)
    query.run();
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

### filters
filters are used by `where` API （see the example on page top).

these DSL functions will help us construct filter for the query (note there is a limitation for now - only single AND or OR to be used, will be fixed in the future).

| Function |                   Syntax                   |            Definition              |
| -------- | :----------------------------------------: | ---------------------------------: |
| and      |                 and(...)                   | "logical and" multiple predicates  |
| or       |                 or(...)                    | "logical or" multiple predicates   |
| eq       |                 eq(col, ...)               | column "col" equals specified values, "in" meaning if multi values provided    |
| neq       |                neq(col, ...)               | column "col" not equals specified value, "not in" meaning if multi values provided    |
| gt       |                gt(col, val)               | column "col" greater than given value    |
| lt       |                lt(col, val)               | column "col" smaller than given value    |
| like       |              like(col, pattern)         | column "col" like SQL pattern such as "%abc", case sensitive |
| ilike       |             ilike(col, pattern)        | column "col" like SQL pattern such as "%ABC", case insensitive |
| unlike       |              unlike(col, pattern)         | opposite value of like |
| iunlike       |             iunlike(col, pattern)        | opposite value of ilike |

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
  
### execute query
`nebula` defines a list of APIs to allow display the query result in selected visual type.


| Function | Definition | Note |
| -------------------------- | :-----------------------------------: | -------------------------------: |
| run (timeline, window)  | execute current query    | run the query |
| timeline (window)    | a short cut for run(true, window) |

### pivot & map
`pivot` is a method to pivot a key column to metrics. This is useful if you pivot a column's metric values into single row for meaningful comparison. Hence we only support pivot API when there is single metric column.

Also, the column to be pivoted should be in low cardinality otherwise the number of columns will be exploded.
for exmaple:
```javascript
    // this is valid pivot query
    // the result will look like []
    nebula
        .source("data-set")
        .time("-5h", "now")
        .select("tag", count("id"))
        .pivot("tag")
        .run();
```

Another API is map which provides function to allow user to compute new columns per row.
For example:
The query returns data as [C1, C2, C3], map function can yield new row schema as [C1, C4=C2/C3].
The schema will change from 3 columns to 2 columns, with the second column value equals C2 / C3.

Both `pivot` and `map` are executed on the client side.
| Function | Signature | Note |
| -------- | :-----------------------------------: | -------------------------------: |
| pivot    | pivot(col/key)     | pivot a key column - do not apply to a metric column |
| map      | map(f, ...cols)    | f is a function to transform from original row, append metric column only. cols are optional metrics column names to remove in visualization | 

for example:
```javascript
    // this map function will add a new metric column `x4` and remove metric column `id.COUNT`
    nebula.source("nebula.test")
        .time("2019-08-16", "2019-08-26")
        .select("tag", "flag", count("id"))
        .map(r => {
            r["x4"] = r["id.COUNT"] * 4;
        }, "id.COUNT")
        .run();
```

Sometimes, we want to use `pivot` and `map` together to compute a new metric. 
for example:
```javascript
    // below code is aggregate total count by "tag", "flag"
    // and pivot by "flag" for each tag column
    // and then compute false/true ratio and put it as a new column "ratio"
    // remove the previous pivoted metrics for "false" and "true"
    // this eventually display this ratio for each tag value
    nebula.source("nebula.test")
        .time("2019-08-16", "2019-08-26")
        .select("tag", "flag", count("id"))
        .pivot("flag")
        .map(r => {
        r["ratio"] = r["false"] / r["true"];
        }, "true", "false")
        .run();
```


# Thanks
Without the great project QuickJS, we won't make this available in such an efficient way.