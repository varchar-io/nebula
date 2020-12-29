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

import {
    neb
} from "./dist/web/main2.js";

import {
    Nebula,
    count,
    sum,
    min,
    max,
    avg,
    tree,
    p10,
    p25,
    p50,
    p75,
    p90,
    p99,
    p99_9,
    p99_99,
    and,
    or,
    eq,
    neq,
    gt,
    lt,
    like,
    ilike
} from './__/sdk.min.js';
import {
    Constraints
} from '../c/constraints.min.js';
import {
    Charts
} from "./c/charts2.min.js";

// define jquery style selector 
const time = neb.time;

// enable selectize and flatpicker
const $$ = (e) => $(e).val();

// global value represents current data set
// keys + metrics: column names for each respectively, rows is array of row objects.
let ds = {
    keys: [],
    metrics: [],
    rows: []
};

// current table info, change when switching different tables
let tableInfo = {};

// two calendar instances
let fpcs, fpce;

const timeCol = "_time_";
const windowCol = "_window_";
const autoKey = (c) => c == timeCol || c == windowCol;
const charts = new Charts();
const nebula = new Nebula();
const formatTime = charts.formatTime;
// chart display area container
const chartId = "#show";
const fieldsId = "fields";
const fieldsRef = `#${fieldsId}`;
// tables list
const tablesId = '#tables';
// start/end elements
const startId = '#start';
const endId = '#end';
const windowId = '#window';
const displayId = '#display';

const NoRollup = -1;
const msg = (text) => $('#qr').text(text);
const vis = (id, flag) => $(id).css('visibility', flag ? 'visible' : 'hidden');
const FETCH_OPT = {
    method: 'GET'
};

// arch mode indicates the web architecture mode
// 1: v1 - web client will query nebula server directly
// 2: v2 - web client will query nebula server through web API. 
// In mode 2, we will need only single place for OAuth which is web (80).
// and potentially close 8080 to public.
// We are deleting mode one support for simplicity - rarely seeing use cases for mode 1
// let archMode = 2;

// filters
let filters;

// a pointer to latest dimensions selectize
let $sdc = null;

// c: column name, m: rollup name
const field = (c, m) => {
    const valid = m in neb.Rollup;
    return {
        M: valid ? neb.Rollup[m] : NoRollup,
        C: c,
        T: valid ? `${m.toLowerCase()}(${c})` : c
    };
};

const makeCalendar = (obj, id, btn, def, min, max) => {
    obj = $(id).flatpickr({
        enableTime: true,
        allowInput: true,
        clickOpens: false,
        dateFormat: "Y-m-d H:i:S",
        defaultDate: def,
        minDate: min,
        maxDate: max,
        allowInvalidPreload: true
    });
    // hook calendar click event
    $(btn).on("click", () => {
        setTimeout(() => {
            obj.open();
        }, 0);
    });
};

const onTableState = (tb, stats, callback) => {
    // save current selected table
    tableInfo = tb;
    const bc = tb.bc;
    const rc = Math.round(tb.rc / 10000) / 100;
    const ms = Math.round(tb.ms / 10000000) / 100;
    const mints = formatTime(tb.mt * 1000);
    const maxts = formatTime(tb.xt * 1000 + 1);

    stats.text(`[Blocks: ${bc}, Rows: ${rc}M, Mem: ${ms}GB, Min T: ${mints}, Max T: ${maxts}]`);

    makeCalendar(fpcs, startId, '#startc', mints, mints, maxts);
    makeCalendar(fpce, endId, '#endc', maxts, mints, maxts);

    // populate all columns with pairs {M:, C:, T:} for "method", "column", "text"
    // for normal dimension, M will be -1, C==T
    const strColumns = (tb.dl || []).filter((v) => v !== timeCol);
    const numColumns = (tb.ml || []).filter((v) => v !== timeCol);
    const allColumns = strColumns.concat(numColumns);
    const all = [];

    // enter all columns
    allColumns.map(e => all.push(field(e)));

    // enter all possible rollups
    for (let r in neb.Rollup) {
        // special handling, just pick one
        if (r === 'COUNT') {
            // use first column only
            all.push(field(all[0].C, r));
            continue;
        }

        let targetColumns = numColumns;
        if (r === 'TREEMERGE') {
            // tree merge applies on string column (list column in future)
            targetColumns = strColumns;
        }

        // pair with every metric column
        targetColumns.map(e => all.push(field(e, r)));
    }

    $('#dwrapper').html(`<select id="${fieldsId}" multiple></select>`);
    const ef = $(fieldsRef).html('');
    all.forEach(d => {
        $('<option/>').appendTo(ef)
            .text(d.T)
            .val(`${JSON.stringify(d)}`);
    });

    $sdc = ef.selectize({
        plugins: ['restore_on_backspace', 'remove_button'],
        persist: false
    });

    // order type 
    $('#ob').html('');
    Object.keys(neb.OrderType).forEach(k => {
        $("<option/>").appendTo($('#ob')).text(k.toLowerCase()).val(neb.OrderType[k]);
    });

    if (callback) {
        callback(allColumns);
    }
};

const initTable = (table, callback) => {
    const stats = $('#stats');
    // table name may have special characters, so encode the table name
    fetch(`/?api=state&start=0&end=0&table=${encodeURIComponent(table)}`, FETCH_OPT)
        .then(response => response.json())
        .then((data) => onTableState(data, stats, callback))
        .catch((err) => stats.text("Error: " + err));
};

// make another query, with time[1548979200 = 02/01/2019, 1556668800 = 05/01/2019] 
// place basic check before sending to server
// return true if failed the check
const checkRequest = (state) => {
    // 0. check if metrics contain hist
    if (state.metrics && state.metrics.length > 1) {
        for (var i = 0; i < state.metrics.length; i++) {
            if (state.metrics[i]["M"] == neb.Rollup.HIST) {
                msg(`Please only select one hist in Fields`);
                return true;
            }
        }
    }

    // 1. valid start and end 
    if (!state.start || !state.end) {
        return true;
    }

    // 2. timeline query
    if (state.timeline) {
        const windowSize = state.window;
        // window size == 0: auto
        if (windowSize > 0) {
            const rangeSeconds = (state.end - state.start) / 1000;
            const buckets = rangeSeconds / windowSize;
            if (buckets > 1000) {
                msg(`Too many data points to return ${buckets}, please increase window granularity.`);
                return true;
            }
        }

        if (state.metrics.length == 0) {
            msg(`Timeline requires metric fields.`);
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

const build = (s) => {
    // read values from 'fields' multi-input
    // and split them into keys and metrics for query build
    const keys = [];
    const metrics = [];
    $$(fieldsRef).map(e => {
        const f = JSON.parse(e);
        if (f.M === NoRollup) {
            keys.push(f.C);
        } else {
            metrics.push(f);
        }
    });

    // support empty fields -> *
    if (keys.length == 0 && metrics.length == 0) {
        keys.push(...tableInfo.dl);
        keys.push(...tableInfo.ml);
        keys.sort();
    }

    // if having metrics - do not let keys more than 2
    if (metrics.length > 0 && keys.length > 2) {
        msg('Risk: too many keys to aggregate by.');
        return;
    }

    // if do not have metrics
    if (metrics.length == 0) {
        keys.unshift(timeCol);
    }

    // build URL and set URL
    const state = s || {
        table: $$(tablesId),
        start: $$(startId),
        end: $$(endId),
        filter: filters.expr(),
        keys: keys,
        timeline: $('#timeline').is(':checked'),
        window: $$(windowId),
        metrics: metrics,
        sort: $$('#ob'),
        limit: $$('#limit')
    };

    if (!state.start || !state.end) {
        msg('Error: please enter start and end time');
        return;
    }

    // change hash will trigger query
    hash('#' + encodeURIComponent(JSON.stringify(state)));
};

let timer = null;
let timer2 = null;
const clsTimer = () => {
    if (timer) {
        clearInterval(timer);
    }
    if (timer2) {
        clearTimeout(timer2);
    }
};
const restore = () => {
    clsTimer();
    const state = url2state();
    const set = (N, V) => {
        if (V) $(N).val(V);
    };
    let table = state.table;

    // if requested table is not in list yet
    let found = false;
    let first = null;
    $(`${tablesId} option`).each((t, v) => {
        // found
        first = first || v.value;
        if (v.value === table) {
            found = true;
        }
    });
    if (!found) {
        // special handling: if the table is not available yet
        // it may meeans the table is ephemeral and not loaded yet
        // we refresh current window in 5 seconds.
        timer = animate(`preparing table ${table} `);
        timer2 = setTimeout(() => {
            location.reload();
        }, 3000);
        table = first;
    }

    if (table) {
        set(tablesId, table);
        initTable(table, (cols) => {
            // set other fields
            set(startId, state.start);
            set(endId, state.end);
            $('#timeline').prop('checked', state.timeline);
            set(windowId, state.window);
            vis(windowId, state.timeline);
            set('#ob', state.sort);
            set('#limit', state.limit);

            // set value of dimensions if there is one
            const fields = [];
            (state.keys || []).map(e => fields.push(JSON.stringify(field(e))));
            (state.metrics || []).map(e => fields.push(JSON.stringify(e)));
            if ($sdc && fields.length > 0) {
                $sdc[0].selectize.setValue(fields);
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
            for (var k in neb.Operation) {
                ops[neb.Operation[k]] = om[k];
            }

            // TODO(cao): due to protobuf definition, we can't build nested group.
            // Should update to support it, then we can turn this flag to true
            // create a filter
            filters = new Constraints(false, "filters", cols, ops, state.filter);

            // if code is specified, set code content and switch to IDE
            if (state.code && state.code.length > 0 && editor.getValue() != state.code) {
                editor.setValue(state.code);
                ide();
            }

            // the URL needs to be executed
            execute(state);
        });
    }
};

const onQueryResult = (state, r) => {
    if (r.error) {
        msg(`[query: error=${r.error}, latency=${r.duration} ms]`);
        return;
    }

    // print query result in stats
    msg(`[query: latency=${r.duration}ms, scan=${r.rows_scan}, blocks=${r.blocks_scan}, rows=${r.rows_ret}]`);

    // JSON result
    ds.timeline = state.timeline;
    ds.rows = JSON.parse(atob(r.data));
    ds.keys = [...state.keys];
    ds.metrics = [];
    for (const key in ds.rows[0]) {
        if (!ds.keys.includes(key) && !autoKey(key)) {
            ds.metrics.push(key);
        }
    }

    // if pivot is present - we will pivot specified column, make all its values as new column
    // this has to be single metric query - metric value will become pivoted column's value
    // e.g [K1, K2, K3, M1] -> pivot(K3) -> [K1, K2, K3V1, K3V2, ...]
    if (state.pivot) {
        const pvc = state.pivot;
        const mc = ds.metrics[0];
        const dataMap = {};

        // new keys = keys excluding pivoted key
        const keys = ds.keys.filter(e => e != pvc);
        // put window column for data moving if present
        if (state.timeline) {
            keys.push(windowCol);
        }

        const metrics = {};

        // map key = keys - pivot
        const mapKey = (row) => {
            // use string as key since javascript doesn't support object as key directly
            const key = [];
            let value = {};
            keys.forEach(e => {
                const v = row[e];
                value[e] = v;
                key.push(`${v}`);
            });

            // check if this key already exists (keys.forEach ensures the order)
            const keyStr = key.join('-');
            if (keyStr in dataMap) {
                value = dataMap[keyStr];
            } else {
                dataMap[keyStr] = value;
            }

            // populate the pivoted value into the value object
            const pivotValue = row[pvc];
            metrics[pivotValue] = 1;
            value[pivotValue] = row[mc];
        };

        // process every row
        ds.rows.forEach(mapKey);

        // build new DS from the data map
        // key is the string of non-pivoted table
        const rows = [];
        for (const k in dataMap) {
            const r = dataMap[k];
            // unnecessary? - check every value property bag to ensure all pvv is present
            // if not assign 0 to that entry (instead of missing). 
            // for example if pivoted column has two values: 
            // [k1, k2, p1, p2] => [k1, k2, p1, p2]
            // [k1, k2, p1]     => [k1, k2, p1, p2]
            // [k1, k2, p2]     => [k1, k2, p1, p2]
            for (const m in metrics) {
                if (!(m in r)) {
                    r[m] = 0;
                }
            }

            rows.push(r);
        }

        // assign the new ds
        ds.rows = rows;
        ds.keys = keys.filter(e => e != windowCol);
        ds.metrics = Object.keys(metrics);
    }

    // if map is present, we will transform some of the columns
    if (state.map && state.map.length > 0) {
        // make NMAP object available
        const nmap = eval(state.map)();
        if (nmap && typeof (nmap) == 'function') {
            // process each row object
            ds.rows.forEach(nmap);

            // figure what metrics columns added
            for (const col in ds.rows[0]) {
                if (!autoKey(col) &&
                    !ds.keys.includes(col) &&
                    !ds.metrics.includes(col)) {
                    // new metric column
                    ds.metrics.push(col);
                }
            }
        }
    }

    // if rm is present - we will remove some of the columns
    if (state.rm && state.rm.length > 0) {
        // only allow metrics to be removed
        const reduced = ds.metrics.filter(e => !state.rm.includes(e));
        if (reduced.length < ds.metrics.length) {
            ds.metrics = reduced;
        }
    }

    // clear table data
    $('#table').html("");
    $(displayId).hide();

    // get display option
    if (ds.rows.length == 0) {
        // TODO(cao): popuate scanned rows metric: rows: ${stats.getRowsscanned()}
        $('#show').html("<b>NO RESULTS.</b>");
        return;
    }

    // decide what chart options are available to current query
    const choices = (list) => {
        // hide it if there is no choice
        const arr = (list || []);
        if (arr.length > 0) {
            $(displayId).show();
        } else {
            $(displayId).hide();
        }

        // add all options in the list
        $(displayId).html('');
        arr.forEach(t => $('<option/>').appendTo($(displayId)).text(t).val(t));
    };
    // if time line, we may enable  area or bar for each data point
    if (ds.timeline) {
        choices(['timeline', 'timeline-area', 'timeline-bar']);
    }
    // tree merge metrics
    else if (state.metrics.length == 1 && state.metrics[0].M == neb.Rollup['TREEMERGE']) {
        choices(['icicle', 'flame']);
    }
    else if (state.metrics.length == 1 && state.metrics[0].M == neb.Rollup['HIST']) {
        choices(['column', 'bar']);
    }
    // others
    else if (state.metrics.length > 0) {
        choices(['column', 'bar', 'doughnut', 'pie', 'line']);
    } else {
        choices();
    }

    const draw = () => {
        // clear chart
        $(chartId).html("");

        // figure out keys and metrics columns
        const keys = [...ds.keys];
        const data = [...ds.rows];
        const metrics = [...ds.metrics];

        // if there are multiple keys, we merge them into single one for display
        if (keys.length > 1) {
            // combined key
            const con = ' - ';
            const combinedCol = keys.join(con);
            data.forEach(row => {
                row[combinedCol] = keys.map(e => row[e]).join(con);
            });

            // ask visual to render this new column instead
            keys.length = 0;
            keys.push(combinedCol);
        }

        // if aggregation specified multiple keys, we merge them into a single key
        let err = null;
        let histIndex = histJsonIdx(metrics);
        let transformedData = [];
        let transformedMetrics = {};
        // json response contains hist data
        if (histIndex != -1) {
            [transformedData, transformedMetrics] = processHistJson(data, metrics, histIndex);
        }
        // render data based on visual choice
        const choice = $(displayId).val();
        const beginMs = time.seconds(state.start) * 1000;
        let showTable = true;
        switch (choice) {
            case 'timeline':
                err = charts.displayTimeline(chartId, data, keys, metrics, windowCol, beginMs, 0);
                break;
            case 'timeline-area':
                err = charts.displayTimeline(chartId, data, keys, metrics, windowCol, beginMs, 1);
                break;
            case 'timeline-bar':
                err = charts.displayTimeline(chartId, data, keys, metrics, windowCol, beginMs, 2);
                break;
            case 'icicle':
                err = charts.displayFlame(chartId, data, keys, metrics, false);
                showTable = false;
                break;
            case 'flame':
                err = charts.displayFlame(chartId, data, keys, metrics, true);
                showTable = false;
                break;
            case 'column':
                if (histIndex != -1) {
                    err = charts.displayBar(chartId, transformedData, keys, transformedMetrics, true);
                } else {
                    err = charts.displayBar(chartId, data, keys, metrics, true);
                }
                break;
            case 'bar':
                if (histIndex != -1) {
                    err = charts.displayBar(chartId, transformedData, keys, transformedMetrics, true);
                } else {
                    err = charts.displayBar(chartId, data, keys, metrics, true);
                }
                break;
            case 'doughnut':
                err = charts.displayPie(chartId, data, keys, metrics, true);
                break;
            case 'pie':
                err = charts.displayPie(chartId, data, keys, metrics, false);
                break;
            case 'line':
                err = charts.displayLine(chartId, data, keys, metrics);
                break;
            default:
                break;
        }

        // display data table
        if (showTable) {
            // always display data in table
            $("#table").attr("class", "fh300");
            charts.displayTable("#table", ds.rows, [timeCol, windowCol, ...ds.keys], ds.metrics);
            // if there is no metrics, do not limit table height
            if (metrics.length == 0) {
                $("#table").removeClass("fh300");
            }
        }

        // display error if any
        if (err) {
            msg(`Error: ${err}`);
        }
    };

    // draw and redraw on window resize - browser fires multiple times
    draw();
    var resizeTimer = undefined;
    $(window).on("resize", () => {
        clearTimeout(resizeTimer);
        resizeTimer = setTimeout(draw, 200);
    });

    //  display change will also trigger draw
    $(displayId).change(draw);
};

const histJsonIdx = (metrics) => {
    for (var i = 0; i < metrics.length; i++) {
        if (metrics[i].endsWith('.HIST')) {
            return i;
        }
    }
    return -1;
}

const processHistJson = (data, metrics, histJsonIdx) => {
    let histJsonStr = JSON.parse(data[0][metrics[histJsonIdx]]);
    let hists = histJsonStr["b"];
    let dict = {};
    let labels = [];
    for (var i = 0; i < hists.length; i++) {
        let count = hists[i][2];
        let label = "[" + hists[i][0] + ", " + hists[i][1] + "]";
        dict[label] = count;
        labels.push(label);
    }
    return [[dict], labels];
}

const url2state = () => {
    // if no hash value - use the first table as the hash
    const input = decodeURIComponent((hash() || `#${$$(tablesId)}`).substr(1));

    // by default, assuming it's just a table name if can't be parsed by JSON
    let state = {
        "table": input
    };
    try {
        state = JSON.parse(input);
    } catch {}
    return state;
};

let dots = 0;
const animate = (prefix, interval) => {
    return setInterval(() => {
        dots = (dots + 1) % 50;
        msg(`${prefix}${'.'.repeat(dots)}`);
    }, interval || 200);
};

const execute = (state) => {
    // get parameters from URL
    if (!state) {
        state = url2state();
    }

    // validate state object
    if (checkRequest(state)) {
        return;
    }

    // display message indicating processing
    timer = animate('soaring in nebula ');

    // fetch the query
    const url = `/?api=query&start=0&end=0&query=${encodeURIComponent(JSON.stringify(state))}`;
    fetch(url, FETCH_OPT)
        .then(response => response.json())
        .then((data) => {
            clsTimer();
            onQueryResult(state, data);
        }).catch((err) => {
            clsTimer();
            msg(`Error: ${err}`);
        });
};

// when user click share button
$('#share').on("click", () => {
    const path = location.href.substr(location.origin.length);
    fetch(`/?api=url&url=${encodeURIComponent(path)}`, FETCH_OPT)
        .then(response => response.json())
        .then((data) => {
            if (data.code && data.code.length > 4) {
                msg(`[query url: ${location.origin}/n/${data.code}]`);
            }
        }).catch((err) => {
            msg(`Error: ${err}`);
        });
});

/** switch user interface between UI elemments and coding editor */
const editor = CodeMirror.fromTextArea($("#code").get(0), {
    lineNumbers: false,
    lineWrapping: true,
    styleActiveLine: true,
    matchBrackets: true,
    mode: "javascript",
    theme: "dracula"
});

const ide = () => {
    const c_on = "tap-on";
    const c_off = "tap-off";
    const on = $(`.${c_on}`);
    const off = $(`.${c_off}`);
    on.removeClass(c_on).addClass(c_off);
    off.removeClass(c_off).addClass(c_on);

    // refresh editor to get focus
    setTimeout(() => editor.refresh(), 5);
};

const iside = () => $('#qc').attr('class') == 'tap-off';

/** execute the code written by user */
const exec = () => {
    // evaluate the code in the editor
    // build nebula object out of it
    // translate it into a service call with build
    // call service to get results back
    const code = editor.getValue();

    // reset the nebula context object and call eval
    nebula.reset();

    // try to eval user's code
    try {
        eval(code);
    } catch (e) {
        msg(`Code Error: ${e.message}`);
        return;
    }

    // based on the code, let's build a model from it
    const error = nebula.validate();
    if (error) {
        msg(`Validation Error: ${error}`);
        return;
    }

    // convert the SDK data into a web request object
    const state = nebula.build();

    // append the source code to this query state
    state.code = code;

    // build this state as a query
    build(state);
};

// hook up hash change event
window.onhashchange = function () {
    restore();
};

// load table list - maximum 100?
const onTableList = (tables) => {
    if (tables && tables.length > 0) {
        tables.sort().forEach(t => $('<option/>').appendTo($(tablesId)).text(t).val(t));
        // restore the selection
        restore();
        return;
    }

    msg("Nebula is down: can not fetch tables");
};
$(() => {
    const stats = $('#stats');
    fetch("/?api=tables&start=0&end=0", FETCH_OPT)
        .then(response => response.json())
        .then((data) => {
            onTableList(data);
        }).catch((err) => {
            stats.text("Error: " + err);
        });

    // if user change the table selection, initialize it again
    $(tablesId).change(() => {
        hash($(tablesId).val());
        restore();
    });

    // display current user info if available
    fetch("/?api=user", FETCH_OPT)
        .then(response => response.json())
        .then((data) => {
            $('#user').text(data.auth ? data.user : "unauth");
        });
});

// either  execute code or build UI
$('#exec').click((e) => {
    if (iside()) exec();
    else build();
});

// switch between UI and IDE
$('#ui').on("click", ide);

// show | hide window for timeline query
$('#timeline').click(function () {
    vis(windowId, $(this).is(':checked'));
});