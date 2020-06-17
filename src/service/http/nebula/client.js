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

import * as d3 from './c/d3.v5.min.js';
import * as flamegraph from './c/d3-flamegraph.min.js';

const {
    EchoRequest,
    TableStateRequest,
    ListTables,
    // define query request and response
    Operation,
    Predicate,
    PredicateAnd,
    PredicateOr,
    Rollup,
    Metric,
    Order,
    OrderType,
    DisplayType,
    QueryRequest,
    Statistics,
    DataType,
    QueryResponse,
} = require('nebula-pb');

const {
    EchoClient,
    V1Client
} = require('nebula-web-rpc');

function bytes2utf8(data) {
    const extraByteMap = [1, 1, 1, 1, 2, 2, 3, 0];
    var count = data.length;
    var str = "";

    for (var index = 0; index < count;) {
        var ch = data[index++];
        if (ch & 0x80) {
            var extra = extraByteMap[(ch >> 3) & 0x07];
            if (!(ch & 0x40) || !extra || ((index + extra) > count))
                return null;

            ch = ch & (0x3F >> extra);
            for (; extra > 0; extra -= 1) {
                var chx = data[index++];
                if ((chx & 0xC0) != 0x80)
                    return null;

                ch = (ch << 6) | (chx & 0x3F);
            }
        }

        str += String.fromCharCode(ch);
    }

    return str;
}

export default {
    EchoClient,
    V1Client,
    EchoRequest,
    TableStateRequest,
    ListTables,
    Operation,
    Predicate,
    PredicateAnd,
    PredicateOr,
    Rollup,
    Metric,
    Order,
    OrderType,
    DisplayType,
    QueryRequest,
    Statistics,
    DataType,
    QueryResponse,
    bytes2utf8,
    d3,
    flamegraph
};