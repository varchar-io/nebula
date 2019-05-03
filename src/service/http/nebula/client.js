import './jquery.min.js'

const {
    EchoRequest,
    TableStateRequest,
    ListTables,
    // define query request and response
    Operation,
    Predicate,
    PredicateAnd,
    PredicateOr,
    Rollup,
    Metric,
    Order,
    OrderType,
    QueryRequest,
    Statistics,
    DataType,
    QueryResponse,
} = require('nebula-pb');

const {
    EchoClient,
    V1Client
} = require('nebula-web-rpc');

function bytes2utf8(data) {
    const extraByteMap = [1, 1, 1, 1, 2, 2, 3, 0];
    var count = data.length;
    var str = "";

    for (var index = 0; index < count;) {
        var ch = data[index++];
        if (ch & 0x80) {
            var extra = extraByteMap[(ch >> 3) & 0x07];
            if (!(ch & 0x40) || !extra || ((index + extra) > count))
                return null;

            ch = ch & (0x3F >> extra);
            for (; extra > 0; extra -= 1) {
                var chx = data[index++];
                if ((chx & 0xC0) != 0x80)
                    return null;

                ch = (ch << 6) | (chx & 0x3F);
            }
        }

        str += String.fromCharCode(ch);
    }

    return str;
}

export default {
    EchoClient,
    V1Client,
    EchoRequest,
    TableStateRequest,
    ListTables,
    Operation,
    Predicate,
    PredicateAnd,
    PredicateOr,
    Rollup,
    Metric,
    Order,
    OrderType,
    QueryRequest,
    Statistics,
    DataType,
    QueryResponse,
    bytes2utf8,
    jQuery
};