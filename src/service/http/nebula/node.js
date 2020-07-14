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
const http = require('http');
const url = require('url');
const zlib = require('zlib');
const {
    Readable
} = require('stream');
const NebulaClient = require('./dist/node/main');
const APIS = require('./api');

// service call module
const serviceAddr = process.env.NS_ADDR || "dev-shawncao:9190";
const client = NebulaClient.qc(serviceAddr);
const grpc = NebulaClient.grpc;
const timeCol = "_time_";
const compressBar = 8 * 1024;
const seconds = (ds) => (+ds == ds) ?
    Math.round(new Date(+ds).getTime() / 1000) :
    Math.round(new Date(ds).getTime() / 1000);
const error = (msg) => {
    return JSON.stringify({
        "error": msg,
        "duration": 0
    });
};

// TODO(cao): these should go more flexible way to figure user info after auth
// Right now, it is only one example way.
const AuthHeader = "authorization";
const UserHeader = "x-forwarded-user";
const GroupHeader = "x-forwarded-groups";

/**
 * Query API
 * start: 2019-04-01
 * end: 2019-06-20
 * fc: user_id
 * op: 0=, 1!=, 2>,3<, 4like
 * fv: 23455554444
 * cols: ["user_id", "pin_id"]
 * limit: 1000
 * res: http response
 */
class Handler {
    constructor(response, heads, encoding, encoder) {
        this.response = response;
        this.heads = heads;
        this.encoding = encoding;
        this.encoder = encoder;
        this.metadata = {};
        this.onError = (err) => {
            this.response.writeHead(200, this.heads);
            this.response.write(error(`${err}`));
            this.response.end();
        };
        this.onNull = () => {
            this.response.writeHead(200, this.heads);
            this.response.write(error("Failed to get reply"));
            this.response.end();
        };
        this.flush = (buf) => {
            // write heads, data and end it
            this.response.writeHead(200, this.heads);
            this.response.write(buf);
            this.response.end();
        };
        this.onSuccess = (data) => {
            // comress data only when it's more than the compression bar
            if (this.encoder && data.length > compressBar) {
                var s = new Readable();
                s.push(data);
                s.push(null);
                let bufs = [];
                s.pipe(this.encoder).on('data', (c) => {
                    bufs.push(c);
                }).on('end', () => {
                    const buf = Buffer.concat(bufs);
                    this.heads["Content-Encoding"] = this.encoding;
                    this.heads["Content-Length"] = buf.length;
                    console.log(`Data compressed: before=${data.length}, after=${buf.length}`);
                    this.flush(buf);
                });
            } else {
                // uncompressed and sync approach
                this.flush(data);
            }
        };
        this.endWithMessage = (message) => {
            this.response.writeHead(200, this.heads);
            this.response.write(error(message));
            this.response.end();
        }
    }
};

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
    const listReq = new NebulaClient.ListTables();
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

const getTableState = (query, handler, client) => {
    const req = new NebulaClient.TableStateRequest();
    req.setTable(query.table);
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
const webq = (query, handler, client) => {
    // the query object schema is defined in state.js
    const state = JSON.parse(query.query);
    const req = new NebulaClient.QueryRequest();
    req.setTable(state.table);
    req.setStart(seconds(state.start));
    req.setEnd(seconds(state.end));

    // the filter can be much more complex
    const filter = state.filter;
    if (filter) {
        // all rules under this group
        const rules = filter.r;
        if (rules && rules.length > 0) {
            const predicates = [];
            rules.forEach(r => {
                const pred = new NebulaClient.Predicate();
                if (r.v && r.v.length > 0) {
                    pred.setColumn(r.c);
                    pred.setOp(r.o);
                    pred.setValueList(r.v);
                    predicates.push(pred);
                }
            });

            if (predicates.length > 0) {
                if (filter.l === "AND") {
                    const f = new NebulaClient.PredicateAnd();
                    f.setExpressionList(predicates);
                    req.setFiltera(f);
                } else if (filter.l === "OR") {
                    const f = new NebulaClient.PredicateOr();
                    f.setExpressionList(predicates);
                    req.setFiltero(f);
                }
            }
        }
    }

    // set sort by first key for samples or first metric for aggregations
    const setOrder = (col, order, req) => {
        const o = new NebulaClient.Order();
        o.setColumn(col);
        o.setType(order);
        req.setOrder(o);
    };

    // set dimension
    const keys = state.keys;
    const display = state.display;
    if (display == NebulaClient.DisplayType.SAMPLES) {
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
            const col = new NebulaClient.CustomColumn();
            col.setColumn(c.name);
            col.setExpr(c.expr);
            col.setType(c.type);
            cc.push(col);
        });
        req.setCustomList(cc);
    }

    // set metric for non-samples query 
    // (use implicit type convert != instead of !==)
    if (display != NebulaClient.DisplayType.SAMPLES) {
        const m = new NebulaClient.Metric();
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
    const req = new NebulaClient.EchoRequest();
    req.setName("_nuclear_");

    // do the query
    client.nuclear(req, {}, (err, reply) => {
        console.log(`ERR=${err}, REP=${reply.getMessage()}`);
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
    const req = new NebulaClient.LoadRequest();
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
            encoder: zlib.createDeflate()
        };
    } else if (acceptEncoding.match(/\bgzip\b/)) {
        return {
            encoding: 'gzip',
            encoder: zlib.createGzip()
        };
    }

    return {
        encoding: null,
        encoder: null
    };
};

//create a server object listening at 80:
http.createServer(async function (req, res) {
    const q = url.parse(req.url, true).query;
    if (q.api) {
        const heads = {
            'Content-Type': 'application/json'
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
    NebulaClient.static_res(req, res);
}).listen(process.env.NODE_PORT || 80);