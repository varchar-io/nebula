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
 * This is entry point of the web server served by node JS
 */
import {
    createServer
} from 'http';
import {
    parse
} from 'url';
import {
    createDeflate,
    createGzip
} from 'zlib';
import {
    APIS
} from './_/api.js';
import {
    time
} from './_/time.min.js';
import {
    Handler
} from './_/handler.js';

// nebula distribute library package
// add require in ES6 module type - remove for commonjs
import {
    createRequire
} from 'module';
const require = createRequire(
    import.meta.url);
const {
    EchoRequest,
    ListTables,
    TableStateRequest,
    Predicate,
    PredicateAnd,
    PredicateOr,
    Metric,
    Order,
    DisplayType,
    CustomColumn,
    QueryRequest,
    LoadRequest,
    static_res,
    qc,
    grpc
} = require('./dist/node/main.cjs');

// service call module
const serviceAddr = process.env.NS_ADDR || "dev-shawncao:9190";
const client = qc(serviceAddr);
const timeCol = "_time_";
const error = Handler.error;
const log = console.log;
const max_json_request = 4 * 1024 * 1024;

// note: http module header are formed in lower cases
// (headers are case insensitive per RFC2616)
// a lot other places using captized 'Content-Type' such as browser
const contentTypeKey = "content-type";
const jsonContentType = "application/json";

// TODO(cao): these should go more flexible way to figure user info after auth
// Right now, it is only one example way.
const AuthHeader = "authorization";
const UserHeader = "x-forwarded-user";
const GroupHeader = "x-forwarded-groups";

/**
 * List all apis
 */
const listApi = (q) => {
    return JSON.stringify(Object.keys(api_handlers));
};

/**
 * Current user info through OAuth
 * q: query object
 * h: request headers
 */
const userInfo = (q, h) => {
    const info = {
        auth: 0
    };

    if (UserHeader in h) {
        info.auth = 1;
        info.authorization = h[AuthHeader] || "NONE";
        info.user = h[UserHeader];
        info.groups = h[GroupHeader];
    }

    return info;
};
/**
 * Convert user info into nebula headers as metadata for grpc service
 */
const toMetadata = (info) => {
    const metadata = new grpc.Metadata();
    for (var f in info) {
        metadata.add(`nebula-${f}`, `${info[f]}`);
    }
    return metadata;
};

/** 
 * get all available tables in nebula.
 */
const getTables = (query, handler, client) => {
    const listReq = new ListTables();
    listReq.setLimit(100);
    client.tables(listReq, {}, (err, reply) => {
        if (err !== null) {
            return handler.onError(err);
        }

        if (reply == null) {
            return handler.onNull();
        }
        return handler.onSuccess(JSON.stringify(reply.getTableList()));
    });
};

const getTableState = (q, handler, client) => {
    const req = new TableStateRequest();
    req.setTable(q.table);
    client.state(req, {}, (err, reply) => {
        if (err !== null) {
            return handler.onError(err);
        }

        if (reply == null) {
            return handler.onNull();
        }
        return handler.onSuccess(JSON.stringify({
            bc: reply.getBlockcount(),
            rc: reply.getRowcount(),
            ms: reply.getMemsize(),
            mt: reply.getMintime(),
            xt: reply.getMaxtime(),
            dl: reply.getDimensionList(),
            ml: reply.getMetricList()
        }));
    });
}

/**
 * Generic web query routing.
 * This supports invoking nebula service behind web server rather than from web client directly.
 * Please refer two different modes on how to architecture web component in Nebula.
 */
const webq = (q, handler, client) => {
    const state = JSON.parse(q.query);
    const req = new QueryRequest();
    req.setTable(state.table);
    req.setStart(time.seconds(state.start));
    req.setEnd(time.seconds(state.end));

    // the filter can be much more complex
    const filter = state.filter;
    if (filter) {
        // all rules under this group
        const rules = filter.r;
        if (rules && rules.length > 0) {
            const predicates = [];
            rules.forEach(r => {
                const pred = new Predicate();
                if (r.v && r.v.length > 0) {
                    pred.setColumn(r.c);
                    pred.setOp(r.o);
                    pred.setValueList(r.v);
                    predicates.push(pred);
                }
            });

            if (predicates.length > 0) {
                if (filter.l === "AND") {
                    const f = new PredicateAnd();
                    f.setExpressionList(predicates);
                    req.setFiltera(f);
                } else if (filter.l === "OR") {
                    const f = new PredicateOr();
                    f.setExpressionList(predicates);
                    req.setFiltero(f);
                }
            }
        }
    }

    // set sort by first key for samples or first metric for aggregations
    const setOrder = (col, order, req) => {
        const o = new Order();
        o.setColumn(col);
        o.setType(order);
        req.setOrder(o);
    };

    // set dimension
    const keys = state.keys;
    const display = state.display;
    if (display == DisplayType.SAMPLES) {
        setOrder(keys[0], state.sort, req);
        keys.unshift(timeCol);
    }
    req.setDimensionList(keys);


    // set query type and window
    req.setDisplay(display);
    req.setWindow(state.window);

    // TODO(cao) - this part logic does not exist in web mode (arch=1)
    const columns = state.customs;
    if (columns && columns.length > 0) {
        const cc = [];
        columns.forEach(c => {
            const col = new CustomColumn();
            col.setColumn(c.name);
            col.setExpr(c.expr);
            col.setType(c.type);
            cc.push(col);
        });
        req.setCustomList(cc);
    }

    // set metric for non-samples query 
    // (use implicit type convert != instead of !==)
    if (display != DisplayType.SAMPLES) {
        const m = new Metric();
        const mcol = state.metrics;
        m.setColumn(mcol);
        m.setMethod(state.rollup);
        req.setMetricList([m]);

        // set order on metric only means we don't order on samples for now
        setOrder(mcol, state.sort, req);
    }

    // set limit
    req.setTop(state.limit);

    // send request to service to get result
    client.query(req, handler.metadata, (err, reply) => {
        if (err !== null) {
            return handler.onError(err);
        }

        if (reply == null) {
            return handler.onNull();
        }
        const stats = reply.getStats();
        return handler.onSuccess(
            JSON.stringify({
                error: stats.getError(),
                duration: stats.getQuerytimems(),
                data: Array.from(reply.getData())
            }));
    });
};

const api_handlers = {
    // system API supporting proxy query to nebula server
    "tables": getTables,
    "state": getTableState,
    "query": webq,
    // load extended api into this api handlers list
    ...APIS
};

/**
 * Shut down nebula server and its nodes.
 * Used for debugging and profiling purpose.
 */
const shutdown = () => {
    const req = new EchoRequest();
    req.setName("_nuclear_");

    // do the query
    client.nuclear(req, {}, (err, reply) => {
        log(`ERR=${err}, REP=${reply.getMessage()}`);
    });

    return JSON.stringify({
        state: 'OK'
    });
};

/**
 * On-demand loading data from parameters.
 * ?api=load&table=x&json=xxx&ttl=5000
 */
const load = (table, json, ttl, handler) => {
    const req = new LoadRequest();
    if (!table) {
        handler.endWithMessage("Missing table");
        return;
    }
    req.setTable(table);

    if (!json) {
        handler.endWithMessage("Missing params json.");
        return;
    }
    req.setParamsjson(json);

    req.setTtl(ttl);

    // send the query
    client.load(req, {}, (err, reply) => {
        if (err !== null) {
            return handler.onError(err);
        }

        if (reply == null) {
            return handler.onNull();
        }
        return handler.onSuccess(JSON.stringify({
            er: reply.getError(),
            tb: reply.getTable(),
            ms: reply.getLoadtimems()
        }));
    });
};

const cmd_handlers = {
    "list": (req, res, q) => res.write(listApi(q)),
    "user": (req, res, q) => res.write(JSON.stringify(userInfo(q, req.headers))),
    "nuclear": (req, res, q) => res.write(shutdown()),
    "load": (req, res, q) => load(q.table, q.json, q.ttl || 3600, new Handler(res))
};

const compression = (req) => {
    // figure out accept encoding, and do some compression
    let acceptEncoding = req.headers['accept-encoding'];
    if (!acceptEncoding) {
        acceptEncoding = '';
    }

    if (acceptEncoding.match(/\bdeflate\b/)) {
        return {
            encoding: 'deflate',
            encoder: createDeflate()
        };
    } else if (acceptEncoding.match(/\bgzip\b/)) {
        return {
            encoding: 'gzip',
            encoder: createGzip()
        };
    }

    return {
        encoding: null,
        encoder: null
    };
};

const readPost = (req) => {
    // try to parse values into data
    return new Promise((resolve, reject) => {
        const empty = "{}";
        const type = req.headers[contentTypeKey];
        if (type != jsonContentType) {
            log(`Only JSON data supported in post: ${type}`);
            reject(empty);
            return;
        }

        let data = "";
        let size = 0;

        req.on('data', (b) => {
                // size check - max request 4MB
                size += b.length;
                if (size > max_json_request) {
                    reject(empty);
                    return;
                }

                data += b;
            }).on("error", (e) => {
                log(`Error reading post JSON: ${e}`);
                reject(empty);
            })
            .on('end', () => {
                // extend the object into current q
                if (data.length > 0) {
                    log(`Resolve a valid data: ${data}`);
                    resolve(data);
                } else {
                    reject(empty);
                }
            });
    });
};

//create a server object listening at 80:
createServer(async function (req, res) {
    const q = parse(req.url, true).query;
    if (q.api) {
        // support post API in JSON as well if query object is not found in URL.
        // if URL specified query object, then POST data will be ignored
        // because it is an application/json data, so query is already json object
        if (!q.query && req.method == "POST") {
            q.query = await readPost(req);
        }

        // pitfall: use brackets, otherwise it will become literal 'contentTypeKey'
        const heads = {
            [contentTypeKey]: jsonContentType
        };

        const c = compression(req);

        // routing generic query through web UI
        if (q.api in cmd_handlers) {
            res.writeHead(200, heads);
            cmd_handlers[q.api](req, res, q);
            if (q.api === "load") {
                return;
            }
        } else if (q.api in api_handlers) {
            // basic requirement
            if (!q.start || !q.end) {
                res.write(error(`start and end time required for every api: ${q.api}`));
            } else {
                try {
                    // build a handler, all data will be compressed based on encoder
                    const handler = new Handler(res, heads, c.encoding, c.encoder);
                    // add user meta data for security
                    handler.metadata = toMetadata(userInfo(q, req.headers));
                    api_handlers[q.api](q, handler, client);
                    return;
                } catch (e) {
                    res.write(error(`api ${q.api} handler exception: ${e}`));
                }
            }
        } else {
            res.write(error(`API not found: ${q.api}`));
        }

        res.end();
        return;
    }

    // serving static resources
    static_res(req, res);
}).listen(process.env.NODE_PORT || 80);