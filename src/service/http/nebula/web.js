import {
    NebulaClient
} from "/dist/web/main.js";

import {
    Charts
} from "/c/charts.min.js";

import {
    Constraints
} from "/c/constraints.min.js";

// define jquery style selector 
const ds = NebulaClient.d3.select;
const $$ = (e) => $(e).val();

// global value represents current data set
let json = [];
let newdata = false;

// two calendar instances
let fpcs, fpce;

// TODO(cao): this has assumption web server living together with nebula server/envoy
// before we replace "{SERVER-ADDRESS}" in build phase, not good for docker image repo
const serviceAddr = `${window.location.protocol}//${window.location.hostname}:8080`;
const v1Client = new NebulaClient.V1Client(serviceAddr);
const timeCol = "_time_";
const charts = new Charts();
const formatTime = charts.formatTime;

// arch mode indicates the web architecture mode
// 1: v1 - web client will query nebula server directly
// 2: v2 - web client will query nebula server through web API. 
// In mode 2, we will need only single place for OAuth which is web (80).
// and potentially close 8080 to public.
let archMode = 2;

// filters
let filters;

// a pointer to latest dimensions selectize
let $sdc = null;

const onTableState = (state, stats, callback) => {
    const bc = state.bc;
    const rc = Math.round(state.rc / 10000) / 100;
    const ms = Math.round(state.ms / 10000000) / 100;
    const mints = formatTime(state.mt * 1000);
    const maxts = formatTime(state.xt * 1000 + 1);

    stats.text(`[Blocks: ${bc}, Rows: ${rc}M, Mem: ${ms}GB, Min T: ${mints}, Max T: ${maxts}]`);

    fpcs = $("#start").flatpickr({
        enableTime: true,
        allowInput: true,
        clickOpens: false,
        defaultDate: mints,
        minDate: mints,
        maxDate: maxts
    });
    // hook calendar click event
    $('#startc').on("click", () => {
        fpcs.open();
    });

    fpce = $("#end").flatpickr({
        enableTime: true,
        allowInput: true,
        clickOpens: false,
        defaultDate: maxts,
        minDate: mints,
        maxDate: maxts
    });
    // hook calendar click event
    $('#endc').on("click", () => {
        fpce.open();
    });

    // populate dimension columns
    const dimensions = state.dl.filter((v) => v !== timeCol);
    let metrics = state.ml.filter((v) => v !== timeCol);
    const all = dimensions.concat(metrics);
    let rollups = Object.keys(NebulaClient.Rollup);

    $('#dwrapper').html("<select id=\"dcolumns\" multiple></select>");
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

    // populate all display types
    ds('#display')
        .html("")
        .selectAll("option")
        .data(Object.keys(NebulaClient.DisplayType))
        .enter()
        .append('option')
        .text(k => k.toLowerCase())
        .attr("value", k => NebulaClient.DisplayType[k]);

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

    if (callback) {
        callback(all);
    }
};

const initTable = (table, callback) => {
    const stats = ds('#stats');
    if (archMode === 1) {
        const req = new NebulaClient.TableStateRequest();
        req.setTable(table);

        // call the service 
        v1Client.state(req, {}, (err, reply) => {

            if (err !== null) {
                stats.text("Error code: " + err);
            } else if (reply == null) {
                stats.text("Failed to get reply");
            } else {
                onTableState({
                    bc: reply.getBlockcount(),
                    rc: reply.getRowcount(),
                    ms: reply.getMemsize(),
                    mt: reply.getMintime(),
                    xt: reply.getMaxtime(),
                    dl: reply.getDimensionList(),
                    ml: reply.getMetricList()
                }, stats, callback);
            }
        });
    } else if (archMode === 2) {
        $.ajax({
            url: "/?api=state&start=0&end=0&table=" + table
        }).fail((err) => {
            stats.text("Error: " + err);
        }).done((data) => {
            onTableState(data, stats, callback);
        });
    }
};

// make another query, with time[1548979200 = 02/01/2019, 1556668800 = 05/01/2019] 
// place basic check before sending to server
// return true if failed the check
const checkRequest = () => {
    // 1. timeline query
    const display = $$("#display");
    if (display == NebulaClient.DisplayType.TIMELINE) {
        const windowSize = $$("#window");
        // window size == 0: auto
        if (windowSize > 0) {
            const start = new Date($$("#start")).getTime();
            const end = new Date($$("#end")).getTime();
            const rangeSeconds = (end - start) / 1000;
            const buckets = rangeSeconds / windowSize;
            if (buckets > 1000) {
                ds("#qr").text(`Too many data points to return ${buckets}, please increase window granularity.`);
                return true;
            }
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
        ff: filters.expr(),
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
        initTable(p.t, (cols) => {
            // set other fields
            set('#start', p.s);
            set('#end', p.e);
            set("#window", p.w);
            set('#display', p.d);
            set('#mcolumns', p.m);
            set('#ru', p.r);
            set('#ob', p.o);
            set('#limit', p.l);

            // set value of dimensions if there is one
            if ($sdc && p.ds) {
                $sdc[0].selectize.setValue(p.ds);
            }

            // populate all operations
            const om = {
                EQ: "=",
                NEQ: "!=",
                MORE: ">",
                LESS: "<",
                LIKE: "like",
                ILIKE: "ilike"
            };

            const ops = {};
            for (var k in NebulaClient.Operation) {
                let value = NebulaClient.Operation[k];
                ops[value] = om[k];
            }

            // TODO(cao): due to protobuf definition, we can't build nested group.
            // Should update to support it, then we can turn this flag to true
            // create a filter
            filters = new Constraints(false, "filters", cols, ops, p.ff);

            // the URL needs to be executed
            execute();
        });
    }
};

const seconds = (ds) => Math.round(new Date(ds).getTime() / 1000);

// extract X-Y for line charts based on json result and query object
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

const buildRequest = (queryStr) => {
    const p = JSON.parse(decodeURIComponent(queryStr));
    // switch between different arch mode
    if (p.arch) {
        archMode = parseInt(p.arch);
    }

    // URL decoding the string and json object parsing
    const q = new NebulaClient.QueryRequest();
    q.setTable(p.t);
    q.setStart(seconds(p.s));
    q.setEnd(seconds(p.e));

    // the filter can be much more complex
    if (p.ff) {
        // all rules under this group
        const rules = p.ff.r;
        if (rules && rules.length > 0) {
            const predicates = [];
            $.each(rules, (i, r) => {
                const pred = new NebulaClient.Predicate();
                if (r.v && r.v.length > 0) {
                    pred.setColumn(r.c);
                    pred.setOp(r.o);
                    pred.setValueList(r.v);
                    predicates.push(pred);
                }
            });


            if (predicates.length > 0) {
                if (p.ff.l === "AND") {
                    const filter = new NebulaClient.PredicateAnd();
                    filter.setExpressionList(predicates);
                    q.setFiltera(filter);
                } else if (p.ff.l === "OR") {
                    const filter = new NebulaClient.PredicateOr();
                    filter.setExpressionList(predicates);
                    q.setFiltero(filter);
                }
            }
        }
    }

    // set dimension
    const dimensions = p.ds;
    if (p.d == NebulaClient.DisplayType.SAMPLES) {
        dimensions.unshift(timeCol);
    }
    q.setDimensionList(dimensions);


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

    return q;
};

const onQueryResult = (q, r, msg) => {
    if (r.error) {
        msg.text(`[query: error=${r.error}, latency=${r.duration} ms]`);
        return;
    }

    msg.html(`[query time: ${r.duration} ms]`);

    // JSON result
    json = JSON.parse(NebulaClient.bytes2utf8(r.data));
    newdata = true;

    // clear table data
    ds('#table_head').html("");
    ds('#table_content').html("");

    // get display option
    if (json.length == 0) {
        // TODO(cao): popuate scanned rows metric: rows: ${stats.getRowsscanned()}
        $('#show').html("<b>NO RESULTS.</b>");
        return;
    }

    const draw = () => {
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
                let data = {
                    default: json
                };
                // with dimension
                if (keys.d && keys.d.length > 0) {
                    const groupBy = (list, key) => {
                        return list.reduce((rv, x) => {
                            (rv[x[key]] = rv[x[key]] || []).push(x);
                            return rv;
                        }, {});
                    };

                    data = groupBy(json, keys.d);
                }

                charts.displayTimeline(data, WINDOW_KEY, keys.m, +start);
                break;
            case NebulaClient.DisplayType.BAR:
                charts.displayBar(json, keys.d, keys.m);
                break;
            case NebulaClient.DisplayType.PIE:
                charts.displayPie(json, keys.d, keys.m);
                break;
            case NebulaClient.DisplayType.LINE:
                charts.displayLine(json, keys.d, keys.m);
                break;
        }
    };

    // draw and redraw on window resize
    draw();
    $(window).on("resize", draw);
};

const execute = () => {
    // get parameters from URL
    const h = hash();
    if (!h || h.length <= 2) {
        return;
    }

    if (checkRequest()) {
        return;
    }

    // display message indicating processing
    const msg = ds('#qr');
    msg.text("soaring in nebula to land...");

    // build the request object
    const queryStr = h.substr(1);
    const q = buildRequest(queryStr);

    if (archMode === 1) {
        v1Client.query(q, {}, (err, reply) => {
            if (reply == null || err) {
                msg.text(`Failed to get reply: ${err}`);
                return;
            }

            const stats = reply.getStats();
            const r = {
                error: stats.getError(),
                duration: stats.getQuerytimems(),
                data: reply.getData()
            };

            // display data
            onQueryResult(q, r, msg);
        });
    } else if (archMode === 2) {
        $.ajax({
            url: "/?api=query&start=0&end=0&query=" + queryStr
        }).fail((err) => {
            msg.text("Error: " + err);
        }).done((data) => {
            onQueryResult(q, data, msg);
        });
    }
};

ds('#btn').on("click", build);
$("#sdw").hide();

// hook up hash change event
window.onhashchange = function () {
    execute();
};

// load table list - maximum 100?
const onTableList = (tables) => {
    const options = ds('#tables').selectAll("option").data(tables.sort()).enter().append('option');
    options.text(d => d).attr("value", d => d);
    // restore the selection
    restore();
};
$(() => {
    const stats = ds('#stats');
    if (archMode === 1) {
        const listReq = new NebulaClient.ListTables();
        listReq.setLimit(100);
        v1Client.tables(listReq, {}, (err, reply) => {
            if (err !== null) {
                stats.text(`RPC Error: ${err}`);
                return;
            }

            onTableList(reply.getTableList());
        });
    } else if (archMode === 2) {
        $.ajax({
            url: "/?api=tables&start=0&end=0"
        }).fail((err) => {
            stats.text("Error: " + err);
        }).done((data) => {
            onTableList(data);
        });
    }

    // if user change the table selection, initialize it again
    ds('#tables').on('change', () => {
        hash('n');
        restore();
    });

    // display current user info if available
    $.ajax({
        url: "/?api=user"
    }).done((data) => {
        ds('#user').text(data.auth ? data.user : "unauth");
    });
});

const vis = async (r) => {
    // n=>nebula, s=>sanddance
    const choice = r.target.value;
    if (choice == 'n') {
        $("#show").show();
        $("#sdw").hide();
    } else if (choice == 's') {
        $("#show").hide();
        $("#sdw").show();

        // go with sanddance
        if (newdata) {
            if (json && json.length > 0) {
                const sandance = () => {
                    // TODO(cao): to use content window we have to use document.getElementById, not sure why
                    const iframe = document.getElementById("sandance");
                    const _post = m => iframe.contentWindow.postMessage(m, '*');
                    return new Promise(resolve => {
                        resolve(_post);
                    });
                };

                // display an embeded explorer
                (await sandance())(json);
            }
            newdata = false;
        }
    }
};

$("#vn").on("click", vis);
$("#vs").on("click", vis);