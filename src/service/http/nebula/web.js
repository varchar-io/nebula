import {
    NebulaClient
} from "/dist/web/main.js";

import {
    Charts
} from "/c/charts.min.js";

// define jquery style selector 
const d3 = NebulaClient.d3;
const ds = NebulaClient.d3.select;
const $$ = (e) => ds(e).property('value');

const serviceAddr = "http://dev-shawncao:8080";
const v1Client = new NebulaClient.V1Client(serviceAddr);

const formatTime = (unix) => {
    const date = new Date(unix * 1000);
    const y = date.getFullYear();
    // month is 0-based index
    const m = date.getMonth() + 1;
    const d = date.getDate();
    return `${y}-${m}-${d}`;
};

const initTable = (table, callback) => {
    const req = new NebulaClient.TableStateRequest();
    req.setTable(table);

    // call the service 
    v1Client.state(req, {}, (err, reply) => {
        const stats = ds('#stats');
        if (err !== null) {
            stats.text("Error code: " + err);
        } else if (reply == null) {
            stats.text("Failed to get reply");
        } else {
            const bc = reply.getBlockcount();
            const rc = reply.getRowcount();
            const ms = reply.getMemsize();
            const mit = formatTime(reply.getMintime());
            const mat = formatTime(reply.getMaxtime());

            stats.text(`[Blocks: ${bc}, Rows: ${rc}, Mem: ${ms}, Min T: ${mit}, Max T: ${mat}]`);

            // populate dimension columns
            const dimensions = reply.getDimensionList().filter((v) => v !== '_time_');
            ds('#dcolumns')
                .html("")
                .selectAll("option")
                .data(dimensions)
                .enter()
                .append('option')
                .text(d => d)
                .attr("value", d => d);

            // populate metrics columns
            const metrics = reply.getMetricList().filter((v) => v !== '_time_');
            ds('#mcolumns')
                .html("")
                .selectAll("option")
                .data(metrics)
                .enter()
                .append('option')
                .text(d => d)
                .attr("value", d => d);

            // populate all columns
            const all = dimensions.concat(metrics);
            ds('#fcolumns')
                .html("")
                .selectAll("option")
                .data(all)
                .enter()
                .append('option')
                .text(d => d)
                .attr("value", d => d);
        }

        if (callback) {
            callback();
        }
    });
};

const opFilter = (op) => {
    switch (op) {
        case "EQ":
            return NebulaClient.Operation.EQ;
        case "NEQ":
            return NebulaClient.Operation.NEQ;
        case "GT":
            return NebulaClient.Operation.MORE;
        case "LT":
            return NebulaClient.Operation.LESS;
        case "LK":
            return NebulaClient.Operation.LIKE;
    }
};

const displayType = (op) => {
    switch (op) {
        case "0":
            return NebulaClient.DisplayType.TABLE;
        case "1":
            return NebulaClient.DisplayType.TIMELINE;
        case "2":
            return NebulaClient.DisplayType.BAR;
        case "3":
            return NebulaClient.DisplayType.PIE;
        case "4":
            return NebulaClient.DisplayType.LINE;
    }
};

// make another query, with time[1548979200 = 02/01/2019, 1556668800 = 05/01/2019] 
// place basic check before sending to server
// return true if failed the check
const checkRequest = () => {
    // 1. timeline query
    const display = $$("#display");
    if (display === "1") {
        const windowSize = $$("#window");
        const start = new Date($$("#start")).getTime();
        const end = new Date($$("#end")).getTime();
        const rangeSeconds = (end - start) / 1000;
        const buckets = rangeSeconds / windowSize;
        if (buckets > 500) {
            ds("#qr").text(`Too many data points to return ${buckets}, please increase window granularity.`);
            return true;
        }
    }

    // pass the check
    return false;
};

const hash = (v) => {
    if (v) {
        window.location.hash = v;
    }

    return window.location.hash;
};

const build = () => {
    // build URL and set URL
    const Q = {
        t: $$('#tables'),
        s: $$('#start'),
        e: $$('#end'),
        fv: $$('#fvalue'),
        fo: $$('#fop'),
        ff: $$("#fcolumns"),
        ds: [$$('#dcolumns')],
        w: $$("#window"),
        d: $$('#display'),
        m: $$('#mcolumns'),
        r: $$('#ru'),
        o: $$('#ob'),
        l: $$('#limit')
    };

    if (!Q.s || !Q.e) {
        alert('please enter start and end time');
        return;
    }

    // change hash will trigger query
    hash('#' + encodeURI(JSON.stringify(Q)));
};

const restore = () => {
    const h = hash();
    if (!h || h.length < 10) {
        console.log(`Bad URL: ${h}`);
        return;
    }

    // get parameters from URL
    const p = JSON.parse(decodeURI(h.substr(1)));
    const set = (N, V) => ds(N).property('value', V);
    if (p.t) {
        set('#tables', p.t);
        initTable(p.t, () => {
            // set other fields
            set('#start', p.s);
            set('#end', p.e);
            set('#fvalue', p.fv);
            set('#fop', p.fo);
            set('#fcolumns', p.ff);
            set('#dcolumns', p.ds[0]);
            set("#window", p.w);
            set('#display', p.d);
            set('#mcolumns', p.m);
            set('#ru', p.r);
            set('#ob', p.o);
            set('#limit', p.l);

            // the URL needs to be executed
            execute();
        });
    }
};

const seconds = (ds) => Math.round(new Date(ds).getTime() / 1000);

const execute = () => {
    // get parameters from URL
    const p = JSON.parse(decodeURI(hash().substr(1)));
    console.log(`Query: ${p}`);

    // URL decoding the string and json object parsing
    const q = new NebulaClient.QueryRequest();
    q.setTable(p.t);
    q.setStart(seconds(p.s));
    q.setEnd(seconds(p.e));

    // the filter can be much more complex
    // but now, it only take one filter
    const fvalue = p.fv;
    if (fvalue) {
        const p2 = new NebulaClient.Predicate();
        p2.setColumn(p.ff);
        p2.setOp(opFilter(p.fo));
        p2.setValueList([fvalue]);
        const filter = new NebulaClient.PredicateAnd();
        filter.setExpressionList([p2]);
        q.setFiltera(filter);
    }

    // set dimensions 
    q.setDimensionList(p.ds);

    // set query type and window
    q.setDisplay(displayType(p.d));
    q.setWindow(p.w);

    // set metric 
    const m = new NebulaClient.Metric();
    const mcol = p.m;
    m.setColumn(mcol);
    const rollupType = p.r;
    switch (rollupType) {
        case "0":
            m.setMethod(NebulaClient.Rollup.COUNT);
            break;
        case "1":
            m.setMethod(NebulaClient.Rollup.SUM);
            break;
        case "2":
            m.setMethod(NebulaClient.Rollup.MIN);
            break;
        case "3":
            m.setMethod(NebulaClient.Rollup.MAX);
            break;
        default:
            m.setMethod(NebulaClient.Rollup.SUM);
            break;
    }
    q.setMetricList([m]);

    // set order and limit
    const o = new NebulaClient.Order();
    o.setColumn(mcol);
    const orderType = p.o;
    o.setType(orderType === "1" ? NebulaClient.OrderType.DESC : NebulaClient.OrderType.ASC);
    q.setOrder(o);
    q.setTop(p.l);

    // do the query 
    const extractXY = (json, q) => {
        // extract X-Y (dimension - metric) columns to display
        // TODO(cao) - revisit this if there are multiple X or multiple Y
        const dims = q.getDimensionList();

        // dumb version of first dimension and first metric 
        let metric = "";
        for (const key in json[0]) {
            if (!dims.includes(key)) {
                metric = key;
            }
        }

        return {
            "d": dims[0],
            "m": metric
        };
    };

    const MIN_SECONDS = 60;
    const HOUR_SECONDS = MIN_SECONDS * 60;
    const DAY_SECONDS = HOUR_SECONDS * 24;
    const WEEK_SECONDS = DAY_SECONDS * 7;
    const timeFormat = (unit) => {
        // unit is secnods value
        // format refer: https://github.com/d3/d3-time-format
        let fmt = "%b,%Y";
        if (unit < MIN_SECONDS) {
            fmt = "%S";
        } else if (unit < HOUR_SECONDS) {
            fmt = "%H:%M:%S";
        } else if (unit < DAY_SECONDS) {
            fmt = "%H:%M";
        } else if (unit < WEEK_SECONDS) {
            fmt = "%m/%d/%y";
        }

        return d3.timeFormat(fmt);
    };

    if (checkRequest()) {
        return;
    }

    v1Client.query(q, {}, (err, reply) => {
        ds('#table_head').html("");
        ds('#table_content').html("");

        const result = ds('#qr');
        if (err !== null) {
            result.text("Error code: " + err);
        } else if (reply == null) {
            result.text("Failed to get reply");
        } else {
            const stats = reply.getStats();
            const json = JSON.parse(NebulaClient.bytes2utf8(reply.getData()));
            result.text(`[query: error=${stats.getError()}, latency=${stats.getQuerytimems()}ms, rows=${json.length}]`);

            // get display option
            if (json.length > 0) {
                const charts = new Charts();
                const display = $$('#display');
                const keys = extractXY(json, q);
                switch (display) {
                    case '0':
                        charts.displayTable(json);
                        break;
                    case '1':
                        const WINDOW_KEY = '_window_';
                        const start = new Date($$('#start')).getTime();
                        const window = +$$('#window');
                        const fmt = timeFormat(window);

                        charts.displayLine(json, keys.m, (scale = 1) => (d, i) => fmt(new Date(start + window * 1000 * json[Math.floor(i * scale)][WINDOW_KEY])));
                        break;
                    case '2':
                        charts.displayBar(json, keys.d, keys.m);
                        break;
                    case '3':
                        charts.displayPie(json, keys.d, keys.m);
                        break;
                    case '4':
                        charts.displayLine(json, keys.m, () => (d, i) => json[i][keys.d]);
                        break;
                }
            }
        }
    });
};

ds('#btn').on("click", build);

// hook up hash change event
window.onhashchange = function () {
    execute();
};

// load table list 
const listReq = new NebulaClient.ListTables();
listReq.setLimit(5);
v1Client.tables(listReq, {}, (err, reply) => {
    const list = reply.getTableList();
    const options = ds('#tables').selectAll("option").data(list).enter().append('option');
    options.text(d => d).attr("value", d => d);

    // properties of the first table
    initTable(list[0]);

    // if user change the table selection, initialize it again
    ds('#tables').on('change', () => {
        initTable($$('#tables'));
    });

    // restore the selection
    setTimeout(restore, 50);
});

$('#fvalue').selectize({
    plugins: ['restore_on_backspace', 'remove_button'],
    persist: false,
    create: function (input) {
        return {
            value: input,
            text: input
        }
    }
});
$('#fvalue').data('selectize').setValue("Option Value Here");