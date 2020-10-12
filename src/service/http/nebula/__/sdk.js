/*
 * Copyright 2017-present Shawn Cao
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
   query.pie();
*/

import {
    neb
} from "../dist/web/main2.js";

import {
    State
} from "./state.min.js";

const assert = (expr) => {
    if (!expr) {
        throw `Failed assertion: ${expr}`;
    }
};

const isType = (obj, type) => {
    return typeof obj === type;
};

const isNumber = (obj) => isType(obj, 'number');
const isString = (obj) => isType(obj, 'string');
const isFunction = (obj) => isType(obj, 'function');

const log = console.log;

const CT = neb.CustomType;
const DT = neb.DisplayType;
const OT = neb.OrderType;
const RU = neb.Rollup;
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
            this.groups_ = [];
            this.start_ = 0;
            this.end_ = 0;
            this.columns_ = [];
            this.display_ = -1;
            this.sort_ = this.Sort.ASC;
            this.limit_ = 100;

            // only useful for timeline
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

        this.where = (filter) => {
            // one option is to translate the filter tree into filter structure.
            // the other option is to set expectation of the filter as boolean script column.
            // the former is probably faster but needs a lot of effort to finish the translation
            // the latter relies on JS code capabillity, much easier but may be slower.
            return this;
        };

        //  we can figure out keys-metrics from select already
        // this is optional for user, but if specified, it will be used to validate the select statement
        this.groupby = (...fields) => {
            for (var i = 0; i < fields.length; ++i) {
                const f = fields[i];
                assert(isNumber(f));
                this.groups_.push(f);
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
            assert(isString(name));
            assert(isNumber(type) && (type >= this.Type.INT && type <= this.Type.STRING));
            assert(isFunction(expr));

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
        this.samples = () => {
            this.display_ = DT.SAMPLES;
            return this;
        };

        this.table = () => {
            this.display_ = DT.TABLE;
            return this;
        };

        this.timeline = () => {
            this.display_ = DT.TIMELINE;
            return this;
        };

        this.bar = () => {
            this.display_ = DT.BAR;
            return this;
        };

        this.pie = () => {
            this.display_ = DT.PIE;
            return this;
        };

        this.line = () => {
            this.display_ = DT.LINE;
            return this;
        };

        this.flame = () => {
            this.display_ = DT.FLAME;
            return this;
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

            // display type is specified
            if (this.display_ === -1) {
                return "Invalid display type.";
            }

            // we should have keys, metrics or both
            if (this.keys_.length === 0 && this.metrics_.length === 0) {
                return "Keys or metrics missing";
            }

            // samples can't have metrics
            if (this.display_ === DT['SAMPLES'] && this.metrics_.length > 0) {
                log("Can't have metrics in samples display mode.");
                return "No metrics allowed in samples mode.";
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
            s.display = this.display_;
            s.limit = this.limit_;
            s.sort = this.sort_;
            s.metrics = this.metrics_;

            // send columns back
            s.customs = this.columns_;

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