const {
    EchoRequest,
    EchoResponse,
    TableStateRequest,
    TableStateResponse,
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
} = require('nebula-node-rpc');

const sstatic = require('serve-static');
const fh = require('finalhandler');
const grpc = require('grpc');
const qc = (service) => {
    return new V1Client(service, grpc.credentials.createInsecure());
};

// static handler
// serving static files from current folder
const shandler = sstatic(".", {
    'index': ['index.html'],
    'dotfiles': 'deny',
    'fallthrough': false,
    'immutable': true
});

const static_res = (req, res) => {
    shandler(req, res, fh(req, res));
};

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
    static_res,
    qc
};