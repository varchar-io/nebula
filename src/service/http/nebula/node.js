/**
 * This is entry point of the web server served by node JS
 */
const http = require('http');
const url = require('url');

// service call module
const NebulaClient = require('./dist/node/main');
const serviceAddr = process.env.NS_ADDR || "dev-shawncao:9190";
const comments_table = "pin.comments";
const signatures_table = "pin.signatures";
const client = NebulaClient.qc(serviceAddr);
const seconds = (ds) => Math.round(new Date(ds).getTime() / 1000);
const error = (msg) => {
    return JSON.stringify({
        "error": msg
    });
};
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
const query = (table, start, end, fc, op, fv, cols, limit, res) => {
    const req = new NebulaClient.QueryRequest();
    req.setTable(table);
    req.setStart(seconds(start));
    req.setEnd(seconds(end));

    // single filter
    const p2 = new NebulaClient.Predicate();
    p2.setColumn(fc);
    p2.setOp(op);
    p2.setValueList([fv]);
    const filter = new NebulaClient.PredicateAnd();
    filter.setExpressionList([p2]);
    req.setFiltera(filter);

    // set dimension
    req.setDimensionList(cols);

    // set limit
    req.setTop(limit);

    // do the query
    client.query(req, {}, (err, reply) => {
        if (err !== null) {
            res.write(error(`${err}`));
        } else if (reply == null) {
            res.write(error("Failed to get reply"));
        } else {
            res.write(NebulaClient.bytes2utf8(reply.getData()));
        }

        res.end();
    });

};

/**
 * get all comments for given user id
 */
const getCommentsByUserId = (q, res) => {
    console.log(`querying comments for user_id=${q.user_id}`);
    if (q.user_id) {
        // search user Id
        query(comments_table, q.start, q.end, "user_id", "0", q.user_id, ["_time_", "pin_signature", "comments"], q.limit || 100, res);
        return;
    }

    res.write(error("Missing user_id."));
    res.end();
};

/**
 * get all comments for given pin id
 */
const getCommentsByPinSignature = (q, res) => {
    console.log(`querying comments for pin_signature=${q.pin_signature}`);
    if (q.pin_signature) {
        // search pin Id
        query(comments_table, q.start, q.end, "pin_signature", "0", q.pin_signature, ["_time_", "user_id", "comments"], q.limit || 100, res);
        return;
    }

    res.write(error("Missing pin_signature."));
    res.end();
};

/**
 * get all pins for given pin signature
 */
const getPinsBySignature = (q, res) => {
    console.log(`querying comments for pin_signature=${q.pin_signature}`);
    if (q.pin_signature) {
        // search pin Id
        query(signatures_table, q.start, q.end, "pin_signature", "0", q.pin_signature, ["pin_id"], q.limit || 100, res);
        return;
    }

    res.write(error("Missing pin_signature."));
    res.end();
};

/**
 * get signature for given pin
 */
const getSignatureByPin = (q, res) => {
    console.log(`querying comments for pin_id=${q.pin_id}`);
    if (q.pin_id) {
        // search pin Id
        query(signatures_table, q.start, q.end, "pin_id", "0", q.pin_id, ["pin_signature"], q.limit || 100, res);
        return;
    }

    res.write(error("Missing pin_id."));
    res.end();
};

/**
 * get all comments for given pin id
 */
const getCommentsByPattern = (q, res) => {
    console.log(`querying comments for comments like ${q.pattern}`);
    if (q.pattern) {
        // search comments like
        query(comments_table, q.start, q.end, "comments", "4", q.pattern, ["_time_", "user_id", "pin_signature", "comments"], q.limit || 100, res);
        return;
    }

    res.write(error("Missing pattern."));
    res.end();
};

/**
 * List all apis
 */
const listApi = (q) => {
    return JSON.stringify(Object.keys(api_handlers));
};

const api_handlers = {
    "comments_by_userid": getCommentsByUserId,
    "comments_by_pinsignature": getCommentsByPinSignature,
    "comments_by_pattern": getCommentsByPattern,
    "pins_by_signature": getPinsBySignature,
    "signature_of_pin": getSignatureByPin
};

//create a server object listening at 80:
http.createServer(async function (req, res) {
    const q = url.parse(req.url, true).query;
    if (q.api) {
        res.writeHead(200, {
            'Content-Type': 'application/json'
        });

        if (q.api === "list") {
            res.write(listApi(q));
        } else if (q.api in api_handlers) {
            // basic requirement
            if (!q.start || !q.end) {
                res.write(error(`start and end dates required for every api: ${q.api}`));
            } else {
                api_handlers[q.api](q, res);
                return;
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