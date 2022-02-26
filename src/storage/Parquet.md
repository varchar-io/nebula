# Parquet type system includes a few different definitions

[Parquet Physical Types]
```
  // enum type {
  //   BOOLEAN = 0,
  //   INT32 = 1,
  //   INT64 = 2,
  //   INT96 = 3,
  //   FLOAT = 4,
  //   DOUBLE = 5,
  //   BYTE_ARRAY = 6,
  //   FIXED_LEN_BYTE_ARRAY = 7,
  //   // Should always be last element.
  //   UNDEFINED = 8
  // };
```
[Parquet Logical Types]
```
  // enum type {
  //   UNKNOWN = 0,
  //   STRING = 1,
  //   MAP = 2,
  //   LIST = 3 ,
  //   ENUM = 4,
  //   DECIMAL = 5,
  //   DATE = 6,
  //   TIME = 7,
  //   TIMESTAMP = 8,
  //   INTERVAL = 9,
  //   INT = 10,
  //   NIL = 11,  // Thrift NullType
  //   JSON = 12 ,
  //   BSON = 13,
  //   UUID = 14,
  //   NONE = 15
  // };
```

[Parquet Converted Types]
```
  // enum type {
  //   NONE,
  //   UTF8,
  //   MAP,
  //   MAP_KEY_VALUE,
  //   LIST,
  //   ENUM,
  //   DECIMAL,
  //   DATE = 7,
  //   TIME_MILLIS,
  //   TIME_MICROS,
  //   TIMESTAMP_MILLIS,
  //   TIMESTAMP_MICROS,
  //   UINT_8,
  //   UINT_16,
  //   UINT_32,
  //   UINT_64,
  //   INT_8,
  //   INT_16,
  //   INT_32,
  //   INT_64,
  //   JSON,
  //   BSON,
  //   INTERVAL,
  //   NA = 25,
  //   // Should always be last element.
  //   UNDEFINED = 26
  // };
```