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
} from "./dist/web/main.js";

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
    p99_99
} from './sdk.min.js';

import {
    Charts
} from "./c/charts.min.js";

// define jquery style selector 
const time = neb.time;

// enable selectize and flatpicker
const ds = neb.d3.select;
const $$ = (e) => $(e).val();

// global value represents current data set
let json = [];

// two calendar instances
let fpcs, fpce;

const timeCol = "_time_";
const windowCol = "_window_";
const charts = new Charts();
const nebula = new Nebula();
const formatTime = charts.formatTime;
const chartId = "#show";
const msg = (text) => ds('#qr').text(text);

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

const onTableState = (state, stats, callback) => {
    const bc = state.bc;
    const rc = Math.round(state.rc / 10000) / 100;
    const ms = Math.round(state.ms / 10000000) / 100;
    const mints = formatTime(state.mt * 1000);
    const maxts = formatTime(state.xt * 1000 + 1);

    stats.text(`[Blocks: ${bc}, Rows: ${rc}M, Mem: ${ms}GB, Min T: ${mints}, Max T: ${maxts}]`);

    fpcs = neb.fp($("#start"), {
        enableTime: true,
        allowInput: true,
        clickOpens: false,
        dateFormat: "Y-m-d H:i:S",
        defaultDate: mints,
        minDate: mints,
        maxDate: maxts
    });
    // hook calendar click event
    $('#startc').on("click", () => {
        fpcs.open();
    });

    fpce = neb.fp($("#end"), {
        enableTime: true,
        allowInput: true,
        clickOpens: false,
        dateFormat: "Y-m-d H:i:S",
        defaultDate: maxts,
        minDate: mints,
        maxDate: maxts
    });
    // hook calendar click event
    $('#endc').on("click", () => {
        fpce.open();
    });

    // populate dimension columns
    const dimensions = (state.dl || []).filter((v) => v !== timeCol);
    let metrics = (state.ml || []).filter((v) => v !== timeCol);
    const all = dimensions.concat(metrics);
    let rollups = Object.keys(neb.Rollup);

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

    // populate metrics columns
    const setm = (values) =>
        ds('#mcolumns')
        .html("")
        .selectAll("option")
        .data(values)
        .enter()
        .append('option')
        .text(d => d)
        .attr("value", d => d);
    setm(all);

    // populate all display types
    ds('#display')
        .html("")
        .selectAll("option")
        .data(Object.keys(neb.DisplayType))
        .enter()
        .append('option')
        .text(k => k.toLowerCase())
        .attr("value", k => neb.DisplayType[k]);

    // roll up methods
    ds('#ru')
        .html("")
        .selectAll("option")
        .data(rollups)
        .enter()
        .append('option')
        .text(k => k.toLowerCase())
        .attr("value", k => neb.Rollup[k]);

    // order type 
    ds('#ob')
        .html("")
        .selectAll("option")
        .data(Object.keys(neb.OrderType))
        .enter()
        .append('option')
        .text(k => k.toLowerCase())
        .attr("value", k => neb.OrderType[k]);

    if (callback) {
        callback(all);
    }
};

const initTable = (table, callback) => {
    const stats = ds('#stats');
    // table name may have special characters, so encode the table name
    $.ajax({
        url: "/?api=state&start=0&end=0&table=" + encodeURIComponent(table)
    }).fail((err) => {
        stats.text("Error: " + err);
    }).done((data) => {
        onTableState(data, stats, callback);
    });
};

// make another query, with time[1548979200 = 02/01/2019, 1556668800 = 05/01/2019] 
// place basic check before sending to server
// return true if failed the check
const checkRequest = (state) => {
    // 1. timeline query
    const display = state.display;
    if (display == neb.DisplayType.TIMELINE) {
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
    }

    if (display == neb.DisplayType.SAMPLES) {
        // TODO(cao) - support * when user doesn't select any dimemsions
        if (state.keys.length == 0) {
            msg(`Please specify dimensions for samples`);
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
    // TODO(cao) - we should set state value to all UI fields
    // restore state like refresh page. sync display
    if (s) {
        ds('#display').property('value', s.display);
    }

    // build URL and set URL
    const state = s || {
        table: $$('#tables'),
        start: $$('#start'),
        end: $$('#end'),
        filter: filters.expr(),
        keys: $$('#dcolumns'),
        window: $$("#window"),
        display: $$('#display'),
        metrics: [{
            C: $$('#mcolumns'),
            M: $$('#ru')
        }],
        sort: $$('#ob'),
        limit: $$('#limit')
    };

    if (!state.start || !state.end) {
        alert('please enter start and end time');
        return;
    }

    // change hash will trigger query
    hash('#' + encodeURIComponent(JSON.stringify(state)));
};

const restore = () => {
    // if no hash value - use the first table as the hash
    let h = hash();
    if (!h || h.length < 10) {
        const tb = $$('#tables');
        h = `?{"table": "${tb}"}`;
    }

    // get parameters from URL
    const state = JSON.parse(decodeURIComponent(h.substr(1)));
    const set = (N, V) => ds(N).property('value', V);
    const table = state.table;
    if (table) {
        set('#tables', table);
        initTable(table, (cols) => {
            // TODO(cao): support multiple metrics in UI
            const metrics = state.metrics;
            const metric = (metrics && metrics.length) ? metrics[0] : {
                C: null,
                M: 0
            };
            // set other fields
            set('#start', state.start);
            set('#end', state.end);
            set("#window", state.window);
            set('#display', state.display);
            set('#mcolumns', metric.C);
            set('#ru', metric.M);
            set('#ob', state.sort);
            set('#limit', state.limit);

            // set value of dimensions if there is one
            if ($sdc && state.keys) {
                $sdc[0].selectize.setValue(state.keys);
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
            filters = new neb.Constraints(false, "filters", cols, ops, state.filter);

            // if code is specified, set code content and switch to IDE
            if (state.code && state.code.length > 0) {
                editor.setValue(state.code);
                ide();
            }

            // the URL needs to be executed
            execute();
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
    json = JSON.parse(atob(r.data));

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
        const display = +state.display;

        // draw may be triggered by resize - stale data may mess
        if (display != +$$('#display')) {
            return;
        }

        if (display == neb.DisplayType.SAMPLES ||
            display == neb.DisplayType.TABLE) {
            charts.displayTable(chartId, json);
            return;
        }

        // figure out keys and metrics columns
        const keys = state.keys.slice();
        const data = json.slice();
        const metrics = [];
        for (const key in data[0]) {
            if (!keys.includes(key) && key != timeCol && key != windowCol) {
                metrics.push(key);
            }
        }

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
        switch (display) {
            case neb.DisplayType.TIMELINE:
                const beginMs = time.seconds(state.start) * 1000;
                charts.displayTimeline(chartId, data, keys, metrics, windowCol, beginMs);
                break;
            case neb.DisplayType.BAR:
                charts.displayBar(chartId, data, keys, metrics);
                break;
            case neb.DisplayType.PIE:
                charts.displayPie(chartId, data, keys, metrics);
                break;
            case neb.DisplayType.LINE:
                charts.displayLine(chartId, data, keys, metrics);
                break;
            case neb.DisplayType.FLAME:
                charts.displayFlame(chartId, data, keys, metrics);
        }
    };

    // draw and redraw on window resize - browser fires multiple times
    draw();
    var resizeTimer = undefined;
    $(window).on("resize", () => {
        clearTimeout(resizeTimer);
        resizeTimer = setTimeout(draw, 200);
    });
};

const execute = () => {
    // get parameters from URL
    const h = hash();
    if (!h || h.length <= 2) {
        return;
    }

    // build the request object
    const queryStr = h.substr(1);
    const state = JSON.parse(decodeURIComponent(queryStr));
    if (checkRequest(state)) {
        return;
    }

    // display message indicating processing
    let dots = 0;
    const prefix = 'soaring in nebula ';
    const handle = setInterval(() => {
        dots = (dots + 1) % 50;
        msg(`${prefix}${'.'.repeat(dots)}`);
    }, 200);

    $.ajax({
        url: "/?api=query&start=0&end=0&query=" + queryStr
    }).fail((err) => {
        clearInterval(handle);
        msg(`Error: ${err}`);
    }).done((data) => {
        clearInterval(handle);
        onQueryResult(state, data);
    });
};

ds('#soar').on("click", build);

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

ds('#ui').on("click", ide);


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
ds('#exec').on("click", exec);
// $("#sdw").hide();

// hook up hash change event
window.onhashchange = function () {
    execute();
};

// load table list - maximum 100?
const onTableList = (tables) => {
    if (tables && tables.length > 0) {
        const options = ds('#tables').selectAll("option").data(tables.sort()).enter().append('option');
        options.text(d => d).attr("value", d => d);
        // restore the selection
        restore();
        return;
    }

    msg("Nebula is down: can not fetch tables");
};
$(() => {
    const stats = ds('#stats');
    $.ajax({
        url: "/?api=tables&start=0&end=0"
    }).fail((err) => {
        stats.text("Error: " + err);
    }).done((data) => {
        onTableList(data);
    });

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

    // Remove Sandance for now. 
    // set sundance to load after all page inits so it doesn't block current page loading
    // ds('#sandance').attr("src", "d/sde.html");
});

/** Remove Sandance from nebula for now
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
                    const s = document.getElementById("sandance");
                    const _post = m => s.contentWindow.postMessage(m, '*');
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
 */