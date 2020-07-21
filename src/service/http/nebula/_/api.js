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

/**
 * This file is used by specific deployment to add extended API.
 */

// DO NOT USE IMPORT for better compability.
import {
    createRequire
} from 'module';
const require = createRequire(
    import.meta.url);
const {
    QueryRequest,
    Predicate,
    PredicateAnd
} = require('../dist/node/main.cjs');
import {
    time
} from './time.min.js';
import {
    Handler
} from './handler.js';
import {
    bytes2utf8
} from './serde.min.js';

// utility function used by most of the API methods
const query = (table, start, end, fc, op, fv, cols, metrics, order, limit, handler, client) => {
    const req = new QueryRequest();
    req.setTable(table);
    req.setStart(time.seconds(start));
    req.setEnd(time.seconds(end));

    // single filter
    const p2 = new Predicate();
    p2.setColumn(fc);
    p2.setOp(op);
    p2.setValueList(Array.isArray(fv) ? fv : [fv]);
    const filter = new PredicateAnd();
    filter.setExpressionList([p2]);
    req.setFiltera(filter);

    // set dimension
    req.setDimensionList(cols);

    if (metrics.length > 0) {
        req.setMetricList(metrics);
    }

    if (order) {
        req.setOrder(order);
    }

    // set limit
    req.setTop(limit);

    // do the query
    client.query(req, handler.metadata, (err, reply) => {
        if (err !== null) {
            return handler.onError(err);
        }

        if (reply == null) {
            return handler.onNull();
        }
        return handler.onSuccess(bytes2utf8(reply.getData()));
    });
};

// defines api name to api method mappings.
// for example: "test: getTest" entry will define api name "test", logic defined by getTest
// every single logic routine will accept 3 parameters: (query, handler, client)
const test = (query, handler, client) => {
    handler.onSuccess(JSON.stringify({
        test: "nebula"
    }));
};

export const APIS = {
    test: test
};