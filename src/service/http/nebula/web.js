import {
    NebulaClient
} from "./dist/web/main.js";

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

            // get dimension columns and metric columns
            var dimensions = reply.getDimensionList();
            $.each(dimensions, (key) => {
                let v = dimensions[key];
                if (v === "_time_") {
                    return;
                }
                $('#dcolumns').append($('<option>', {
                    value: v
                }).text(v));

                $('#fcolumns').append($('<option>', {
                    value: v
                }).text(v));
            });
            let metrics = reply.getMetricList();
            $.each(metrics, (key) => {
                let v = metrics[key];
                if (v === "_time_") {
                    return;
                }

                $('#mcolumns').append($('<option>', {
                    value: v
                }).text(v));

                $('#fcolumns').append($('<option>', {
                    value: v
                }).text(v));
            });


        }
    });
};

// load table list 
var listReq = new NebulaClient.ListTables();
listReq.setLimit(5);
v1Client.tables(listReq, {}, (err, reply) => {
    var list = reply.getTableList();
    for (var i = 0; i < list.length; ++i) {
        var key = list[i];
        $('#tables').append($('<option>', {
            value: key
        }).text(key));
    }

    // properties of the first table
    initTable(list[0]);

});

var opFilter = () => {
    var op = $('#fop').val();
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
    q.setTable($('#tables').val());

    // new Date('2012.08.10').getTime() / 1000
    var start = $('#start').val();
    var end = $('#end').val();
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
    p2.setColumn($("#fcolumns").val());
    p2.setOp(opFilter());
    p2.setValueList([$('#fvalue').val()]);
    var filter = new NebulaClient.PredicateAnd();
    filter.setExpressionList([p2]);
    q.setFiltera(filter);

    // set dimensions 
    q.setDimensionList([$("#dcolumns").val()]);

    // set metric 
    var m = new NebulaClient.Metric();
    var mcol = $("#mcolumns").val();
    m.setColumn(mcol);
    var rollupType = $('#ru').val();
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
    var orderType = $('#ob').val();
    o.setType(orderType === "1" ? NebulaClient.OrderType.DESC : NebulaClient.OrderType.ASC);
    q.setOrder(o);
    q.setTop($('#limit').val());

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
                var h = $('#table_head');
                var keys = Object.keys(json[0]);
                var columns = keys.length;
                for (var k = 0; k < columns; k++) {
                    h.append('<td>' + keys[k] + '</td>');
                }

                // Get table body and print 
                var c = $('#table_content');
                for (var i = 0; i < json.length; i++) {
                    c.append('<tr>');
                    var row = json[i];
                    for (var j = 0; j < columns; j++) {
                        c.append('<td>' + row[keys[j]] + '</td>');
                    }
                    c.append('</tr>');
                }
            }
        }
    });
};

$('#btn').click(execute);