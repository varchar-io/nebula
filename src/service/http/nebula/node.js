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
    CustomColumn,
    QueryRequest,
    LoadType,
    LoadRequest,
    UrlData,
    static_res,
    qc,
    grpc,
    jsonb
} = require('./dist/node/main.cjs');

// service call module
const log = Handler.log;
const serviceAddr = process.env.NS_ADDR || "dev-shawncao:9190";
log(`Create client to nebula server: ${serviceAddr}`);
const client = qc(serviceAddr);
const timeCol = "_time_";
const error = Handler.error;
// bota (base64 encoding) and atob (base64 decoding) only available in browsers
// so let's use alternative way to simulate it
const btoa = (bytes) => Buffer.from(bytes).toString('base64');

// keep it the same as grpc max send message length, ref `n/node.js`
const max_json_request = 64 * 1024 * 1024;

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
            ml: reply.getMetricList(),
            ht: reply.getHistsList()
        }));
    });
}

/**
 * Generic web query routing.
 * This supports invoking nebula service behind web server rather than from web client directly.
 * Please refer two different modes on how to architecture web component in Nebula.
 */
const webq = (q, handler, client) => {
    log(`build query for ${q.query}`);
    const state = jsonb.parse(q.query);
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
    // keys.unshift(timeCol);
    req.setDimensionList(keys);

    // set query type and window
    req.setTimeline(state.timeline);
    req.setTimeUnit(state.time_unit);
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
    if (state.metrics.length > 0) {
        const metrics = [];
        state.metrics.forEach(e => {
            const m = new Metric();
            m.setColumn(e.C);
            m.setMethod(e.M);
            metrics.push(m);
        });
        req.setMetricList(metrics);

        // set order on metric only means we don't order on samples for now
        const firstMetricColumn = state.metrics[0].C;
        setOrder(firstMetricColumn, state.sort, req);
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
        // why do we use base64 here rather than pass the raw byte array?
        // JSON.stringify will serialize every single byte into a number string.
        // for example, [0x89] -> "[137]", one byte becomes 3 bytes. It is 3x increase.
        // while base64 is 33% increase in size overwire, and it's extremely fast to decode (native).
        const data = btoa(reply.getData());
        log(`Get nebula reply ${data.length} bytes.`);
        return handler.onSuccess(JSON.stringify({
            error: stats.getError(),
            duration: stats.getQuerytimems(),
            rows_scan: stats.getRowsscanned(),
            blocks_scan: stats.getBlocksscanned(),
            rows_ret: stats.getRowsreturn(),
            data: data
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
 * Shorten given URL
 * url(q.url, new Handler(res))
 */
const shorten = (url, handler) => {
    if (!url || url.length < 1) {
        handler.endWithMessage("Missing url to shorten");
        return false;
    }

    const req = new UrlData();
    req.setRaw(url);
    log(`shorten URL: ${url}`);

    // send the query to shorten
    client.url(req, {}, (err, reply) => {
        if (err !== null) {
            return handler.onError(err);
        }

        if (reply == null) {
            return handler.onNull();
        }
        return handler.onSuccess(JSON.stringify({
            code: reply.getCode(),
            raw: reply.getRaw()
        }));
    });

    // already handled
    return false;
};

/**
 * On-demand loading data from parameters.
 * ?api=load&type=config&table=x&json=xxx&ttl=5000
 */
const load = (type, table, json, ttl, handler) => {
    const req = new LoadRequest();
    if (!table) {
        handler.endWithMessage("Missing table");
        return false;
    }

    // different load type - default to config
    let lt = LoadType.CONFIG;
    if (type === "gsheet") {
        lt = LoadType.GOOGLE_SHEET;
    } else if (type === "demand") {
        lt = LoadType.DEMAND;
    }
    req.setType(lt);
    req.setTable(table);

    if (!json) {
        handler.endWithMessage("Missing demand json.");
        return false;
    }
    req.setJson(json);

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

    // already handled
    return false;
};

const cmd_handlers = {
    "list": (req, res, q) => res.write(listApi(q)),
    "user": (req, res, q) => res.write(JSON.stringify(userInfo(q, req.headers))),
    "nuclear": (req, res, q) => res.write(shutdown()),
    "load": (req, res, q) => load(q.type, q.table, q.json, q.ttl || 3600, new Handler(res)),
    "url": (req, res, q) => shorten(q.url || q.post, new Handler(res))
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

// read a post request body as a string
const readPost = (req) => {
    // try to parse values into data
    return new Promise((resolve, reject) => {
        const empty = "{}";
        const type = req.headers[contentTypeKey];
        // some content type may not exactly as expected, such as `application/json; charset=UTF-8`.
        if (!type.includes(jsonContentType)) {
            reject(`Only JSON content is supported in post: ${type}.`);
            return;
        }

        let data = "";
        let size = 0;

        req.on('data', (b) => {
                // size check - max request 4MB
                size += b.length;
                if (size > max_json_request) {
                    reject(`Request data is too large: ${size}`);
                    return;
                }

                data += b;
            }).on("error", (e) => {
                reject(`Error reading post JSON data: ${e}`);
            })
            .on('end', () => {
                // extend the object into current q
                if (data.length > 0) {
                    resolve(data);
                } else {
                    reject('Empty data posted.');
                }
            });
    });
};

// redirect a path, return true if success
const redirect = (parsed, res) => {
    // short url redirect
    const m = parsed.pathname.match(/\/n\/(?<token>\w+)/i);
    if (m) {
        const req = new UrlData();
        req.setCode(m.groups['token']);
        client.url(req, {}, (err, reply) => {
            const full = reply.getRaw() || "/";
            res.writeHead(302, {
                'Location': full
            });
            res.end();
        });
        return true;
    }

    return false;
};

const endError = (msg, res, heads) => {
    res.writeHead(200, heads);
    res.write(error(msg));
    res.end();
};

//create a server object listening at 80:
createServer(async function (req, res) {
    const parsed = parse(req.url, true);
    if (redirect(parsed, res)) {
        return;
    }

    const q = parsed.query;
    if (q.api) {
        // fetch post body if present
        if (req.method == "POST") {
            q.post = await readPost(req).catch(e => log(`Error: ${e}`));
        }
        // support post API in JSON as well if query object is not found in URL.
        // if URL specified query object, then POST data will be ignored
        // because it is an application/json data, so query is already json object
        if (!q.query) {
            q.query = q.post;
        }

        // pitfall: use brackets, otherwise it will become literal 'contentTypeKey'
        const heads = {
            [contentTypeKey]: jsonContentType
        };

        const c = compression(req);
        // routing generic query through web UI
        try {
            if (q.api in cmd_handlers) {
                res.writeHead(200, heads);
                if (cmd_handlers[q.api](req, res, q)) {
                    res.end();
                }
                return;
            } else if (q.api in api_handlers) {
                // basic requirement
                if (q.start && q.end) {
                    // build a handler, all data will be compressed based on encoder
                    const handler = new Handler(res, heads, c.encoding, c.encoder);
                    // add user meta data for security
                    handler.metadata = toMetadata(userInfo(q, req.headers));
                    return api_handlers[q.api](q, handler, client);
                }

                return endError(`start and end time required for every api: ${q.api}`, res, heads);
            } else {
                return endError(`API not found: ${q.api}`, res, heads);
            }
        } catch (e) {
            return endError(`api ${q.api} handler exception: ${JSON.stringify(e)}`, res, heads);
        }
    }

    // serving static resources
    res.setHeader("Cache-Control", "max-age=31536000");
    static_res(req, res);
}).listen(process.env.NODE_PORT || 80);