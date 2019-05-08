import {
    NebulaClient
} from "./dist/web/main.js";

// define jquery style selector 
let $ = NebulaClient.d3.select;
let $$ = (e) => $(e).property('value');

var serviceAddr = "http://dev-shawncao:8080";
var v1Client = new NebulaClient.V1Client(serviceAddr);

var formatTime = (unix) => {
    var date = new Date(unix * 1000);
    let y = date.getFullYear();
    let m = date.getMonth();
    let d = date.getDate();
    return `${y}-${m}-${d}`;
};

let initTable = (table) => {
    var req = new NebulaClient.TableStateRequest();
    req.setTable(table);

    // call the service 
    v1Client.state(req, {}, (err, reply) => {
        var stats = $('#stats');
        if (err !== null) {
            stats.text("Error code: " + err);
        } else if (reply == null) {
            stats.text("Failed to get reply");
        } else {
            let bc = reply.getBlockcount();
            let rc = reply.getRowcount();
            let ms = reply.getMemsize();
            let mit = formatTime(reply.getMintime());
            let mat = formatTime(reply.getMaxtime());

            stats.text(`[Blocks: ${bc}, Rows: ${rc}, Mem: ${ms}, Min T: ${mit}, Max T: ${mat}]`);

            // populate dimension columns
            var dimensions = reply.getDimensionList().filter((v) => v !== '_time_');
            $('#dcolumns').html("");
            var options = $('#dcolumns').selectAll("option").data(dimensions).enter().append('option');
            options.text(d => d).attr("value", d => d);

            // populate metrics columns
            var metrics = reply.getMetricList().filter((v) => v !== '_time_');
            $('#mcolumns').html("");
            var options = $('#mcolumns').selectAll("option").data(metrics).enter().append('option');
            options.text(d => d).attr("value", d => d);

            // populate all columns
            var all = dimensions.concat(metrics);
            $('#fcolumns').html("");
            var options = $('#fcolumns').selectAll("option").data(all).enter().append('option');
            options.text(d => d).attr("value", d => d);
        }
    });
};

// load table list 
var listReq = new NebulaClient.ListTables();
listReq.setLimit(5);
v1Client.tables(listReq, {}, (err, reply) => {
    var list = reply.getTableList();
    var options = $('#tables').selectAll("option").data(list).enter().append('option');
    options.text(d => d).attr("value", d => d);

    // properties of the first table
    initTable(list[0]);

    // if user change the table selection, initialize it again
    $('#tables').on('change', () => {
        initTable($$('#tables'));
    });
});

var opFilter = () => {
    var op = $$('#fop');
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
}

// make another query, with time[1548979200 = 02/01/2019, 1556668800 = 05/01/2019] 
var execute = () => {
    var q = new NebulaClient.QueryRequest();
    q.setTable($$('#tables'));

    // new Date('2012.08.10').getTime() / 1000
    var start = $$('#start');
    var end = $$('#end');
    if (!start || !end) {
        alert('please enter start and end time');
        return;
    }

    var utStart = new Date(start).getTime() / 1000;
    var utEnd = new Date(end).getTime() / 1000;;
    console.log("start: " + utStart + ", end: " + utEnd);
    q.setStart(utStart);
    q.setEnd(utEnd);

    var p2 = new NebulaClient.Predicate();
    p2.setColumn($$("#fcolumns"));
    p2.setOp(opFilter());
    p2.setValueList([$$('#fvalue')]);
    var filter = new NebulaClient.PredicateAnd();
    filter.setExpressionList([p2]);
    q.setFiltera(filter);

    // set dimensions 
    q.setDimensionList([$$("#dcolumns")]);

    // set metric 
    var m = new NebulaClient.Metric();
    var mcol = $$("#mcolumns");
    m.setColumn(mcol);
    var rollupType = $$('#ru');
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
    var o = new NebulaClient.Order();
    o.setColumn(mcol);
    var orderType = $$('#ob');
    o.setType(orderType === "1" ? NebulaClient.OrderType.DESC : NebulaClient.OrderType.ASC);
    q.setOrder(o);
    q.setTop($$('#limit'));

    // do the query 
    v1Client.query(q, {}, (err, reply) => {
        $('#table_head').html("");
        $('#table_content').html("");

        var result = $('#qr');
        if (err !== null) {
            result.text("Error code: " + err);
        } else if (reply == null) {
            result.text("Failed to get reply");
        } else {
            var stats = reply.getStats();
            var json = JSON.parse(NebulaClient.bytes2utf8(reply.getData()));
            result.text(`[query: error=${stats.getError()}, latency=${stats.getQuerytimems()}ms, rows=${json.length}]`);

            // Get Table headers and print 
            if (json.length > 0) {
                // append header
                var keys = Object.keys(json[0]);
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
        }
    });
};

$('#btn').on("click", execute);