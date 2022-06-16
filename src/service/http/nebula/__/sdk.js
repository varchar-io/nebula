/*
 * Copyright 2017-present varchar.io
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

/*
 * SDK.js provides web client SDK for end user to program interactions with Nebula
 * It will support the basic code sample like below, its main purpose is to translate the code into a nebula request.
 *     
 * TODO(cao) - it is debatable whether display method should have x-y axis spec or leave it to rendering system 
 * so that it can build interactive module when there is ambiguous data to render.
 * Keep it simple for now - Not Specified.
 * 
   //const values
   const name = "test";
   const schema = "ROW<a:int, b:string, c:bigint>";

   // define an customized column
   const colx = () => nebula.column("a") % 3;
   nebula.apply("colx", nebula.Types.INT, expr);

   // get a data set from data set stored in HTTPS or S3
   const query = nebula.csv("https://varchar.io/test/data.csv", schema)
                    .source(name)
                    .time(104598762, 108598762)
                    .select("colx", count(1).as("count"))
                    .where(col("value") > 10)
                    .groupby(1)
                    .orderby(2, nebula.SORT.DESC);

   // render the data result with PIE chart with x-axis from "colx" and y-axis from "count"
   query.run();
*/

import {
    neb
} from "../dist/web/main2.js";

import {
    State
} from "./state.min.js";

const isType = (obj, type) => {
    return typeof obj === type;
};

const isNumber = (obj) => isType(obj, 'number');
const isString = (obj) => isType(obj, 'string');
const isFunction = (obj) => isType(obj, 'function');
const s = (arr) => arr.map(e => `${e}`);

const log = console.log;
const except = msg => {
    throw {
        message: msg
    };
};

const CT = neb.CustomType;
const OT = neb.OrderType;
const RU = neb.Rollup;
const OP = neb.Operation;
export class Nebula {
    constructor() {
        // types supported by nebula JS SDK
        this.Type = {
            INT: CT.INT,
            LONG: CT.LONG,
            FLOAT: CT.FLOAT,
            DOUBLE: CT.DOUBLE,
            STRING: CT.STRING
        };

        this.Sort = {
            ASC: OT.ASC,
            DESC: OT.DESC,
            NONE: OT.NONE
        };

        // reset current object
        this.reset = () => {
            this.src_ = "";
            this.keys_ = [];
            this.metrics_ = [];
            this.start_ = 0;
            this.end_ = 0;
            this.columns_ = [];
            this.timeline_ = false;
            this.time_unit_ = "";
            this.sort_ = this.Sort.ASC;
            this.limit_ = 100;
            this.filter_ = {};
            this.pivot_ = "";
            this.map_ = null;
            this.rm_ = [];

            // only useful for timeline - in unit of seconds.
            this.window_ = 0;
        };

        // table specify a table name
        this.source = (src) => {
            this.src_ = src;
            return this;
        };

        // time range is a must set, range [s, e] of unix tiem epoch
        this.time = (start, end) => {
            this.start_ = start;
            this.end_ = end;
            return this;
        };

        // select method will pass keys and metrics definition
        this.select = (...args) => {
            // process each arg and determine if they are keys or metrics
            for (var i = 0; i < args.length; ++i) {
                const arg = args[i];
                const type = typeof arg;
                // if type is string - it is referring a column name
                if (type === 'string') {
                    this.keys_.push(arg);
                } else if (type === 'function') {
                    // if type is statement/method call, it is a UDF (or UDAF)
                    this.metrics_.push(eval(arg));
                } else if (type === 'object') {
                    this.metrics_.push(arg);
                } else {
                    throw `Unsupported field type: ${type}.`;
                }
            }

            return this;
        };

        // this API allows user to pivot a column - only applied to single metric query.
        // this could be extended to support pivot on a few columns (using combination).
        this.pivot = (col) => {
            if (!this.keys_.includes(col) || this.metrics_.length !== 1) {
                except("pivot existing key for single metric.");
            }

            this.pivot_ = col;
            return this;
        };

        // a map function is a function to transform each row to a new row
        // the f should be function/lambda - we use unique name NMAP 
        // indicating only one map function supported in each query
        // we use `() => [];` pattern to support both function and lambda in evaluation
        // so client can extract the method by eval(map)() pointing to the function body
        this.map = (f, ...cols) => {
            this.map_ = f ? `() => ${f.toString()};` : "";
            this.rm_ = cols;
            return this;
        };

        this.where = (f) => {
            // one option is to translate the filter tree into filter structure.
            // the other option is to set expectation of the filter as boolean script column.
            // the former is probably faster but needs a lot of effort to finish the translation
            // the latter relies on JS code capabillity, much easier but may be slower.
            if (f) {
                // this is logic wrapper AND or OR
                // if not, it should be a single predicate
                if (f.l) {
                    this.filter_ = f;
                } else {
                    this.filter_ = {
                        l: "AND",
                        r: [f]
                    };
                }
            }

            return this;
        };

        // TODO(cao): need to support complex sorting scheme
        // right now, only sort by first column, set sort dir only here
        // set sort by prop 
        this.sortby = (x) => {
            this.sort_ = x;
            return this;
        };

        // set limit of the result
        this.limit = (l) => {
            this.limit_ = l;
            return this;
        }

        // column definition API - register a new column with specified logic
        // example:
        // const expr = () => nebula.column("value") % 5;
        this.apply = (name, type, expr) => {
            if (!name || !isString(name)) {
                except('apply - column name required');
            }
            if (type === undefined || !isNumber(type) || type < this.Type.INT || type > this.Type.STRING) {
                except('apply - unsupported type.');
            }
            if (!expr || !isFunction(expr)) {
                except('apply - lambda/function required.');
            }

            // no matter the target is a function or a lambda, 
            // we assign its handle to the named variable so that this can be looked up in script context
            // for lambda, the expr will be 
            //         "const {name} = () => {...};"
            // or function, the expr will be
            //         "const {name} = function abc(){...};
            this.columns_.push({
                name: name,
                type: type,
                expr: `const ${name} = ${expr.toString()};`
            });

            return this;
        };

        // all display types
        this.run = (timeline, window) => {
            this.timeline_ = !!timeline;
            this.window_ = window || 0;
            return this;
        };

        this.timeline = (window) => {
            return this.run(true, window);
        };

        // validate the properties
        this.validate = () => {
            // check if valid table name is set
            if (!this.src_) {
                return "Invalid table source.";
            }

            // validate time range is specified.
            if (this.start_ === 0 || this.end_ === 0) {
                log(`Invalid time range [${this.start_}, ${this.end_}].`);
                return "Invalid time range.";
            }

            // we should have keys, metrics or both
            if (this.keys_.length === 0 && this.metrics_.length === 0) {
                return "Keys or metrics missing";
            }

            // limit needs to be a valid value 
            if (this.limit_ < 1) {
                log(`Invalid limit: ${this.limit_}`);
                return "Invalid limit value.";
            }

            // sort value needs to be in range
            if (this.sort_ != this.Sort.ASC &&
                this.sort_ != this.Sort.DESC &&
                this.sort_ != this.Sort.NONE) {
                log(`Invalid sort value: ${this.sort_}`);
                return "Invalid sort option.";
            }

            return null;
        };

        this.build = () => {
            // TODO(cao) - we should operate on state object directly
            // the contract is defined in state.js
            const state = new State();
            const s = state.state_;
            s.table = this.src_;
            s.start = this.start_;
            s.end = this.end_;
            s.keys = this.keys_;
            s.window = this.window_;
            s.timeline = this.timeline_;
            s.time_unit = this.time_unit_;
            s.limit = this.limit_;
            s.sort = this.sort_;
            s.metrics = this.metrics_;
            s.filter = this.filter_;

            // send columns back
            s.customs = this.columns_;

            // pivot column set by user - used by client side only
            s.pivot = this.pivot_;

            // map is a transform method
            // rm is columns to remove from previous dataset
            s.map = this.map_;
            s.rm = this.rm_;

            // return the q object
            return s;
        };
    }
};

// define all available supported functions
// by iterate all available rollups
// TODO(cao) - figure out a way to map all methods into an export function
// which is available from SDK to user to call.
// for (const f : RU){
//    eval(`export const ${f} = () => {};`)
// }
// But now, let's start with manual decoration keep the method name same as its definition
export const count = (col) => ({
    M: RU.COUNT,
    C: col,
    A: col,
    as: function (alias) {
        this.A = alias;
        return this;
    }
});

export const sum = (col) => ({
    M: RU.SUM,
    C: col,
    A: col,
    as: function (alias) {
        this.A = alias;
        return this;
    }
});

export const max = (col) => ({
    M: RU.MAX,
    C: col,
    A: col,
    as: function (alias) {
        this.A = alias;
        return this;
    }
});

export const min = (col) => ({
    M: RU.MIN,
    C: col,
    A: col,
    as: function (alias) {
        this.A = alias;
        return this;
    }
});

export const avg = (col) => ({
    M: RU.AVG,
    C: col,
    A: col,
    as: function (alias) {
        this.A = alias;
        return this;
    }
});

export const tree = (col) => ({
    M: RU.TREEMERGE,
    C: col,
    A: col,
    as: function (alias) {
        this.A = alias;
        return this;
    }
});

// P10 = 5;
// P25 = 6;
// P50 = 7;
// P75 = 8;
// P90 = 9;
// P99 = 10;
// P99_9 = 11;
// P99_99 = 12;
export const p10 = (col) => ({
    M: RU.P10,
    C: col,
    A: col,
    as: function (alias) {
        this.A = alias;
        return this;
    }
});

export const p25 = (col) => ({
    M: RU.P25,
    C: col,
    A: col,
    as: function (alias) {
        this.A = alias;
        return this;
    }
});

export const p50 = (col) => ({
    M: RU.P50,
    C: col,
    A: col,
    as: function (alias) {
        this.A = alias;
        return this;
    }
});

export const p75 = (col) => ({
    M: RU.P75,
    C: col,
    A: col,
    as: function (alias) {
        this.A = alias;
        return this;
    }
});

export const p90 = (col) => ({
    M: RU.P90,
    C: col,
    A: col,
    as: function (alias) {
        this.A = alias;
        return this;
    }
});

export const p99 = (col) => ({
    M: RU.P99,
    C: col,
    A: col,
    as: function (alias) {
        this.A = alias;
        return this;
    }
});

export const p99_9 = (col) => ({
    M: RU.P99_9,
    C: col,
    A: col,
    as: function (alias) {
        this.A = alias;
        return this;
    }
});

export const p99_99 = (col) => ({
    M: RU.P99_99,
    C: col,
    A: col,
    as: function (alias) {
        this.A = alias;
        return this;
    }
});

// filter related API
// and(1, 2, 3)
export const and = (...args) =>
    args.length == 0 ? {} : {
        l: "AND",
        r: args
    };

export const or = (...args) =>
    args.length == 0 ? {} : {
        l: "OR",
        r: args
    };

export const eq = (col, ...args) => {
    if (args.length == 0) {
        except("syntax: eq(<column>, val1, val2, ...)");
    }

    return {
        c: col,
        o: OP.EQ,
        v: s(args)
    };
};

export const neq = (col, ...args) => {
    if (args.length == 0) {
        except("syntax: eq(<column>, val1, val2, ...)");
    }

    return {
        c: col,
        o: OP.NEQ,
        v: s(args)
    };
};

export const gt = (col, val) => ({
    c: col,
    o: OP.MORE,
    v: s([val])
});

export const lt = (col, val) => ({
    c: col,
    o: OP.LESS,
    v: s([val])
});

export const like = (col, val) => ({
    c: col,
    o: OP.LIKE,
    v: s([val])
});

export const ilike = (col, val) => ({
    c: col,
    o: OP.ILIKE,
    v: s([val])
});

export const unlike = (col, val) => ({
    c: col,
    o: OP.UNLIKE,
    v: s([val])
});

export const iunlike = (col, val) => ({
    c: col,
    o: OP.IUNLIKE,
    v: s([val])
});