# BigInt Problem
We know that Javascript can only represent 2^53-1 as it max number. For any values larger than that will lose precision during data exchange in javascript runtime. Most of the time, we have to pay cost to serializing 64bits in string format.

Nebula faces this issue too, in this short note, I would just like to summarize all places with the concern and how we handle them for now, this doesn't mean it is ideal, in fact, we are still looking for improvements. 

## No 1. rapidjson
We have been using RapidJson as the JSON serde library in our C++ runtime, due to its capability handling this concern well. 
We don't do `long<-->string` conversion trick. Hence we removed similar trick in your code base, specifically in InExpression serde code path.


## No 2. node.js
node.js is the web server runtime `Nebula Web` resides in, it is definitely the most hot place regarding this issue. We have been using JSON as data fromat to exchange requests/responses between `web client` and `Nebula Server`.  Here are what we have done:
1. We're using json-bigint package to do `json.parse` so if the input JSON includes bigint values, it won't lose the value're precision.
2. In nebula proto, we added two more types of predicate, so it allows client to use one of them
   1.  value: string type 
   2.  nvalue: int64 type
   3.  dvalue: double type.


with this change, we don't pay string->int or string->double type data conversion. (In fact, protobuf does this for us - it is uncertain if perf is better, but at least cleaner at interface).


The serde work is dene by protobuf-js, which is still using `Number` for all number values, so the problem still exists, if passing bigint value in `grpc` call defined by int64 in `nebula.proto`, Nebula Server may receive wrong value. And I haven't seen this is going to be fixed soon anywhere. We will keep eyes open for any alternative solution.


As a work-around, we will use "string" representation for `nvalue` by appending `[jstype = JS_STRING]` in its proto declaration. After that, we requires client to send string list for this field for now. Definitely this is neither convinient nor efficient.

## No 3. web
In `Nebula UI` which means browser here, we haven't handle any thing like this, since it is for display purpose, basically all bigint values are returned in `string` type. Client code (browser js) needs to convert it if used in further computation.

## No 4. other clients.
This is obvious but just to mention, other clients that is not executing in javascript runtime should not have this issue, for example, if I use `curl` to post a JSON blob of bigint array, `Nebula API` (node.js) won't lose precision of values. Other clients like Java should be fine too.