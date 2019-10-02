import {
    NebulaClient
} from "/dist/web/main.js";

import {
    Charts
} from "/c/charts.min.js";

// define jquery style selector 
const d3 = NebulaClient.d3;
const ds = NebulaClient.d3.select;
const $$ = (e) => $(e).val();

const serviceAddr = "{SERVER-ADDRESS}";
const v1Client = new NebulaClient.V1Client(serviceAddr);

const formatTime = (unix) => {
    const date = new Date(unix);
    const y = date.getFullYear();
    // month is 0-based index
    const m = date.getMonth() + 1;
    const d = date.getDate();
    const h = date.getHours();
    const mi = date.getMinutes();
    return `${y}-${m}-${d} ${h}:${mi}`;
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
            const rc = Math.round(reply.getRowcount() / 10000) / 100;
            const ms = Math.round(reply.getMemsize() / 10000000) / 100;
            const mints = reply.getMintime() * 1000;
            const maxts = reply.getMaxtime() * 1000;

            stats.text(`[Blocks: ${bc}, Rows: ${rc}M, Mem: ${ms}GB, Min T: ${formatTime(mints)}, Max T: ${formatTime(maxts)}]`);

            // set up datetime picker for start and end dates
            const dto = {
                enableTime: true,
                defaultDate: "today",
                minDate: mints,
                maxDate: maxts
            };

            $("#start").flatpickr(dto);
            $("#end").flatpickr(dto);

            // populate dimension columns
            const dimensions = reply.getDimensionList().filter((v) => v !== '_time_');
            let metrics = reply.getMetricList().filter((v) => v !== '_time_');
            const all = dimensions.concat(metrics);
            let rollups = Object.keys(NebulaClient.Rollup);

            $('#dwrapper').html("Dimension: <select id=\"dcolumns\" multiple></select>");
            ds('#dcolumns')
                .html("")
                .selectAll("option")
                .data(all)
                .enter()
                .append('option')
                .text(d => d)
                .attr("value", d => d);
            $sdc = $('#dcolumns').selectize({
                plugins: ['restore_on_backspace', 'remove_button'],
                persist: false
            });

            // if the table has no metrics column (no value columns)
            // we only allow count on first column then
            if (metrics.length == 0) {
                metrics = [dimensions[0]];
                rollups = ['COUNT'];
            }

            // populate metrics columns
            ds('#mcolumns')
                .html("")
                .selectAll("option")
                .data(metrics)
                .enter()
                .append('option')
                .text(d => d)
                .attr("value", d => d);

            // populate all columns
            ds('#fcolumns')
                .html("")
                .selectAll("option")
                .data(all)
                .enter()
                .append('option')
                .text(d => d)
                .attr("value", d => d);

            // populate all display types
            ds('#display')
                .html("")
                .selectAll("option")
                .data(Object.keys(NebulaClient.DisplayType))
                .enter()
                .append('option')
                .text(k => k.toLowerCase())
                .attr("value", k => NebulaClient.DisplayType[k]);

            // populate all operations
            const om = {
                EQ: "=",
                NEQ: "!=",
                MORE: ">",
                LESS: "<",
                LIKE: "like"
            };
            ds('#fop')
                .html("")
                .selectAll("option")
                .data(Object.keys(NebulaClient.Operation))
                .enter()
                .append('option')
                .text(k => om[k])
                .attr("value", k => NebulaClient.Operation[k]);

            // roll up methods
            ds('#ru')
                .html("")
                .selectAll("option")
                .data(rollups)
                .enter()
                .append('option')
                .text(k => k.toLowerCase())
                .attr("value", k => NebulaClient.Rollup[k]);

            // order type 
            ds('#ob')
                .html("")
                .selectAll("option")
                .data(Object.keys(NebulaClient.OrderType))
                .enter()
                .append('option')
                .text(k => k.toLowerCase())
                .attr("value", k => NebulaClient.OrderType[k]);

        }

        if (callback) {
            callback();
        }
    });
};

// make another query, with time[1548979200 = 02/01/2019, 1556668800 = 05/01/2019] 
// place basic check before sending to server
// return true if failed the check
const checkRequest = () => {
    // 1. timeline query
    const display = $$("#display");
    if (display == NebulaClient.DisplayType.TIMELINE) {
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

    if (display == NebulaClient.DisplayType.SAMPLES) {
        // TODO(cao) - support * when user doesn't select any dimemsions
        const dimensions = $$("#dcolumns");
        if (dimensions.length == 0) {
            ds("#qr").text(`Please specify dimensions for samples`);
            return true;
        }
    }

    // if data aggregations, we enforce filter
    // if (display != NebulaClient.DisplayType.SAMPLES &&
    //     display != NebulaClient.DisplayType.TIMELINE) {
    //     if ($$('#fvalue').length == 0) {
    //         ds("#qr").text(`Please specify filters for specific aggregations`);
    //         return true;
    //     }
    // }

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
        ds: $$('#dcolumns'),
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
    // if no hash value - use the first table as the hash
    let h = hash();
    if (!h || h.length < 10) {
        console.log(`Bad URL: ${h}`);
        const tb = $$('#tables');
        h = `?{"t": "${tb}"}`;
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
            set('#fop', p.fo);
            set('#fcolumns', p.ff);
            set("#window", p.w);
            set('#display', p.d);
            set('#mcolumns', p.m);
            set('#ru', p.r);
            set('#ob', p.o);
            set('#limit', p.l);

            // selectize init value
            {
                const fv = p.fv || [];
                ds('#fvalue')
                    .html("")
                    .selectAll("option")
                    .data(fv)
                    .enter()
                    .append('option')
                    .text(d => d)
                    .attr("value", d => d);
                const $sdv = $('#fvalue').selectize({
                    plugins: ['restore_on_backspace', 'remove_button'],
                    persist: false,
                    create: input => {
                        return {
                            value: input,
                            text: input
                        };
                    }
                });
                $sdv[0].selectize.setValue(fv);
                if ($sdc && p.ds) {
                    $sdc[0].selectize.setValue(p.ds);
                }
            }
            // the URL needs to be executed
            execute();
        });
    }
};

const seconds = (ds) => Math.round(new Date(ds).getTime() / 1000);

const execute = () => {
    // get parameters from URL
    const h = hash();
    if (!h || h.length < 2) {
        return;
    }

    const p = JSON.parse(decodeURIComponent(h.substr(1)));
    console.log(`Query: ${p}`);

    // URL decoding the string and json object parsing
    const q = new NebulaClient.QueryRequest();
    q.setTable(p.t);
    q.setStart(seconds(p.s));
    q.setEnd(seconds(p.e));

    // the filter can be much more complex
    // but now, it only take one filter
    const fvalue = p.fv;
    if (fvalue.length > 0) {
        const p2 = new NebulaClient.Predicate();
        p2.setColumn(p.ff);
        p2.setOp(p.fo);
        p2.setValueList(fvalue);
        const filter = new NebulaClient.PredicateAnd();
        filter.setExpressionList([p2]);
        q.setFiltera(filter);
    }

    // set dimension
    q.setDimensionList(p.ds);


    // set query type and window
    q.setDisplay(p.d);
    q.setWindow(p.w);

    // set metric for non-samples query 
    // (use implicit type convert != instead of !==)
    if (p.d != NebulaClient.DisplayType.SAMPLES) {
        const m = new NebulaClient.Metric();
        const mcol = p.m;
        m.setColumn(mcol);
        m.setMethod(p.r);
        q.setMetricList([m]);

        // set order on metric only means we don't order on samples for now
        const o = new NebulaClient.Order();
        o.setColumn(mcol);
        o.setType(p.o);
        q.setOrder(o);
    }

    // set limit
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
    const timeFormat = (unit, range) => {
        // unit is secnods value
        // format refer: https://github.com/d3/d3-time-format
        // no more than 10 ticks
        let scale = Math.round(range / unit / 1000 / 10);
        if (scale < 1) {
            scale = 1;
        }

        let ticks = d3.timeWeek.every(scale);
        let fmt = "%b %Y";
        if (unit < MIN_SECONDS) {
            ticks = d3.timeMinute.every(scale);
            fmt = "%S";
        } else if (unit < HOUR_SECONDS) {
            ticks = d3.timeHour.every(scale);
            fmt = "%H:%M:%S";
        } else if (unit < DAY_SECONDS) {
            ticks = d3.timeHour.every(scale);
            fmt = "%H:%M";
        } else if (unit < WEEK_SECONDS) {
            ticks = d3.timeDay.every(scale);
            fmt = "%m/%d/%y";
        }

        return {
            t: ticks,
            f: d3.timeFormat(fmt)
        };
    };

    if (checkRequest()) {
        return;
    }

    ds('#qr').text("soaring in nebula to land...");
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
                const draw = () => {
                    const charts = new Charts();
                    // enum value are number and switch/case are strong typed match
                    const display = +$$('#display');
                    const keys = extractXY(json, q);
                    switch (display) {
                        case NebulaClient.DisplayType.SAMPLES:
                        case NebulaClient.DisplayType.TABLE:
                            charts.displayTable(json);
                            break;
                        case NebulaClient.DisplayType.TIMELINE:
                            const WINDOW_KEY = '_window_';
                            const start = new Date($$('#start'));
                            const end = new Date($$('#end'));
                            const window = +$$('#window');
                            const fmt = timeFormat(window, end - start);

                            charts.displayTimeline(json, WINDOW_KEY, keys.m, start, end, window * 1000, fmt.t, fmt.f);
                            // charts.displayLine(json, keys.m, (scale = 1) => (d, i) => fmt(new Date(start + window * 1000 * json[Math.floor(i * scale)][WINDOW_KEY])));
                            break;
                        case NebulaClient.DisplayType.BAR:
                            charts.displayBar(json, keys.d, keys.m);
                            break;
                        case NebulaClient.DisplayType.PIE:
                            charts.displayPie(json, keys.d, keys.m);
                            break;
                        case NebulaClient.DisplayType.LINE:
                            charts.displayLine(json, keys.m, () => (d, i) => json[i][keys.d]);
                            break;
                    }
                };

                // draw and redraw on window resize
                draw();
                $(window).on("resize", draw);
            }
        }
    });
};

ds('#btn').on("click", build);

// hook up hash change event
window.onhashchange = function () {
    execute();
};

// load table list - maximum 100?
const listReq = new NebulaClient.ListTables();
listReq.setLimit(100);
v1Client.tables(listReq, {}, (err, reply) => {
    const stats = ds('#stats');
    if (err !== null) {
        stats.text(`RPC Error: ${err}`);
        return;
    }

    const list = reply.getTableList().sort();
    const options = ds('#tables').selectAll("option").data(list).enter().append('option');
    options.text(d => d).attr("value", d => d);

    // if user change the table selection, initialize it again
    ds('#tables').on('change', () => {
        hash('n');
        restore();
    });

    // restore the selection
    restore();
});

// a pointer to latest dimensions selectize
let $sdc = null;