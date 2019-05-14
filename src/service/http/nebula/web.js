import {
    NebulaClient
} from "./dist/web/main.js";

// define jquery style selector 
const d3 = NebulaClient.d3;
const $ = NebulaClient.d3.select;
const $$ = (e) => $(e).property('value');

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

const initTable = (table) => {
    const req = new NebulaClient.TableStateRequest();
    req.setTable(table);

    // call the service 
    v1Client.state(req, {}, (err, reply) => {
        const stats = $('#stats');
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
            $('#dcolumns')
                .html("")
                .selectAll("option")
                .data(dimensions)
                .enter()
                .append('option')
                .text(d => d)
                .attr("value", d => d);

            // populate metrics columns
            const metrics = reply.getMetricList().filter((v) => v !== '_time_');
            $('#mcolumns')
                .html("")
                .selectAll("option")
                .data(metrics)
                .enter()
                .append('option')
                .text(d => d)
                .attr("value", d => d);

            // populate all columns
            const all = dimensions.concat(metrics);
            $('#fcolumns')
                .html("")
                .selectAll("option")
                .data(all)
                .enter()
                .append('option')
                .text(d => d)
                .attr("value", d => d);
        }
    });
};

// load table list 
const listReq = new NebulaClient.ListTables();
listReq.setLimit(5);
v1Client.tables(listReq, {}, (err, reply) => {
    const list = reply.getTableList();
    const options = $('#tables').selectAll("option").data(list).enter().append('option');
    options.text(d => d).attr("value", d => d);

    // properties of the first table
    initTable(list[0]);

    // if user change the table selection, initialize it again
    $('#tables').on('change', () => {
        initTable($$('#tables'));
    });
});

const opFilter = () => {
    const op = $$('#fop');
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

const displayType = () => {
    const op = $$('#display');
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
            $("#qr").text(`Too many data points to return ${buckets}, please increase window granularity.`);
            return true;
        }
    }

    // pass the check
    return false;
};

const execute = () => {
    const q = new NebulaClient.QueryRequest();
    q.setTable($$('#tables'));

    // new Date('2012.08.10').getTime() / 1000
    const start = $$('#start');
    const end = $$('#end');
    if (!start || !end) {
        alert('please enter start and end time');
        return;
    }

    const utStart = new Date(start).getTime() / 1000;
    const utEnd = new Date(end).getTime() / 1000;;
    console.log("start: " + utStart + ", end: " + utEnd);
    q.setStart(utStart);
    q.setEnd(utEnd);

    const p2 = new NebulaClient.Predicate();
    p2.setColumn($$("#fcolumns"));
    p2.setOp(opFilter());
    p2.setValueList([$$('#fvalue')]);
    const filter = new NebulaClient.PredicateAnd();
    filter.setExpressionList([p2]);
    q.setFiltera(filter);

    // set dimensions 
    q.setDimensionList([$$("#dcolumns")]);

    // set query type and window
    q.setDisplay(displayType());
    q.setWindow($$("#window"));

    // set metric 
    const m = new NebulaClient.Metric();
    const mcol = $$("#mcolumns");
    m.setColumn(mcol);
    const rollupType = $$('#ru');
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
    const orderType = $$('#ob');
    o.setType(orderType === "1" ? NebulaClient.OrderType.DESC : NebulaClient.OrderType.ASC);
    q.setOrder(o);
    q.setTop($$('#limit'));

    const displayTable = (json) => {
        // Get Table headers and print 
        if (json.length > 0) {
            const area = $('#show');
            area.html("");
            const tb = area.append("table");
            tb.append("thead").append("tr").attr("id", "table_head");
            tb.append("tbody").attr("id", "table_content");

            // append header
            const keys = Object.keys(json[0]);
            $('#table_head').selectAll("th").data(keys).enter().append('th').text(d => d);

            // Get table body and print 
            $('#table_content').selectAll('tr').data(json).enter().append('tr')
                .selectAll('td')
                .data((row) => {
                    return keys.map((column) => {
                        return {
                            column: column,
                            value: row[column]
                        };
                    });
                })
                .enter()
                .append('td')
                .text(d => d.value);
        }
    };

    const displayBar = (json, key, value) => {
        // clear the area first
        const area = $('#show');
        area.html("");
        const margin = {
            top: 20,
            right: 20,
            bottom: 70,
            left: 40
        };
        const width = area.node().scrollWidth - margin.left - margin.right;
        const height = 600;

        const x = d3.scaleBand()
            .rangeRound([0, width])
            .paddingInner([0.2])
            .paddingOuter([0.4])
            .align([0.5]);
        const y = d3.scaleLinear().range([height, 0]);
        const xAxis = d3.axisBottom(x);
        const yAxis = d3.axisLeft(y).ticks(10);

        const svg = area.append("svg")
            .attr("width", width + margin.left + margin.right)
            .attr("height", height + margin.top + margin.bottom)
            .append("g")
            .attr("transform", `translate(${margin.left},${margin.top})`);

        // json is object list
        // set value range for both x and y axis
        x.domain(json.map(row => row[key]));
        y.domain([0, d3.max(json, row => row[value])]);

        svg.append("g")
            .attr("class", "x axis")
            .attr("transform", `translate(0, ${height})`)
            .call(xAxis)
            .selectAll("text")
            .style("text-anchor", "end")
            .attr("dx", "-.8em")
            .attr("dy", "-.55em")
            .attr("transform", "rotate(-45)");

        svg.append("g")
            .attr("class", "y axis")
            .call(yAxis)
            .append("text")
            .attr("transform", "rotate(-90)")
            .attr("y", 6)
            .attr("dy", ".71em")
            .style("text-anchor", "end")
            .text("Value ($)");

        svg.selectAll("bar")
            .data(json)
            .enter().append("rect")
            .style("fill", "steelblue")
            .attr("x", row => x(row[key]))
            .attr("y", row => y(row[value]))
            .attr("width", x.bandwidth())
            .attr("height", row => height - y(row[value]));
    };

    const displayPie = (json, key, value) => {
        // clear the area first
        const area = $('#show');
        area.html("");

        // we limit to display 10 values only, long tail will be aggregated as others
        json.sort((a, b) => {
            return b[value] - a[value];
        });

        const top = 10;
        if (json.length > top) {
            json[top - 1][key] = 'others';
            for (let i = top; i < json.length; ++i) {
                json[top - 1][value] += json[i][value];
            }

            json.length = top;
        }

        const w = area.node().scrollWidth;
        const h = 400;
        const r = Math.min(w, h) / 2;
        const color = (i) => d3.schemeCategory10[i % 10];

        const vis = area
            .append("svg:svg")
            .data([json])
            .attr("width", w)
            .attr("height", h)
            .append("svg:g")
            .attr("transform", `translate(${w/2},${h/2})`);

        const arc = d3.arc().outerRadius(r - 10).innerRadius(0);
        const pie = d3.pie().value(row => row[value]);

        const arcs = vis.selectAll("g.slice").data(pie).enter().append("svg:g").attr("class", "slice");
        arcs.append("svg:path")
            .attr("fill", (d, i) => color(i))
            .attr("d", arc);

        // display legends 
        // place each legend on the right and bump each one down 15 pixels
        // the posiiton of translate is relative to its parent, so the delta is computed to the center of the pie
        const legend = vis.selectAll("g.legend").data(pie).enter().append("svg:g").attr("class", "legend")
            .attr("transform", (d, i) => `translate(${r+100},${-r + i * 20 + 20})`);

        // make a matching color rect
        legend.append("svg:rect")
            .attr("width", 15)
            .attr("height", 15)
            .attr("fill", (d, i) => color(i));

        legend.append("svg:text")
            .attr("text-anchor", "left")
            .attr("x", 18)
            .attr("y", 15)
            .text((d, i) => json[i][key]);
    };

    const displayLine = (json, value, format) => {
        // clear the area first
        const area = $('#show');
        area.html("");

        // set the dimensions and margins of the graph
        const margin = {
            top: 20,
            right: 50,
            bottom: 20,
            left: 50
        };

        const width = area.node().scrollWidth - margin.left - margin.right;
        const height = 400 - margin.top - margin.bottom;

        // set the ranges
        const x = d3.scaleTime().range([0, width]).domain(d3.extent(json, (d, i) => i));
        const y = d3.scaleLinear().range([height, 0]).domain([0, d3.max(json, (d) => d[value])]);

        // define the line
        var line = d3.line()
            .x((d, i) => x(i))
            .y((d) => y(d[value]));

        // append the svg obgect to the body of the page
        // appends a 'group' element to 'svg'
        // moves the 'group' element to the top left margin
        var svg = area.append("svg")
            .attr("width", width + margin.left + margin.right)
            .attr("height", height + margin.top + margin.bottom)
            .append("g")
            .attr("transform", `translate(${margin.left}, ${margin.top})`);

        // Add the valueline path.
        svg.append("path")
            .data([json])
            .attr("class", "line")
            .attr("d", line);

        // Add the X Axis
        svg.append("g")
            .attr("transform", `translate(0, ${height})`)
            .call(
                d3.axisBottom(x)
                .tickFormat(format()));

        // Add the Y Axis
        svg.append("g").call(d3.axisLeft(y));
    };

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
            fmt = "%m/%d";
        }

        return d3.timeFormat(fmt);
    };

    if (checkRequest()) {
        return;
    }

    v1Client.query(q, {}, (err, reply) => {
        $('#table_head').html("");
        $('#table_content').html("");

        const result = $('#qr');
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
                const display = $$('#display');
                const keys = extractXY(json, q);
                switch (display) {
                    case '0':
                        displayTable(json);
                        break;
                    case '1':
                        const WINDOW_KEY = '_window_';
                        const start = new Date($$('#start')).getTime();
                        const window = +$$('#window');
                        const fmt = timeFormat(window);

                        displayLine(json, keys.m, () => (d, i) => fmt(new Date(start + window * 1000 * json[i][WINDOW_KEY])));
                        break;
                    case '2':
                        displayBar(json, keys.d, keys.m);
                        break;
                    case '3':
                        displayPie(json, keys.d, keys.m);
                        break;
                    case '4':
                        displayLine(json, keys.m, () => (d, i) => json[i][key]);
                        break;
                }
            }
        }
    });
};

$('#btn').on("click", execute);