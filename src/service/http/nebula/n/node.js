import {
    EchoRequest,
    EchoResponse,
    ListTables,
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
    DisplayType,
    CustomType,
    CustomColumn,
    QueryRequest,
    Statistics,
    DataType,
    QueryResponse,
    LoadError,
    LoadType,
    LoadRequest,
    LoadResponse
} from 'nebula-pb';

import {
    EchoClient,
    V1Client
} from 'nebula-node-rpc';

import sstatic from 'serve-static';
import fh from 'finalhandler';
import grpc from 'grpc';
import jsonb from 'json-bigint';

const qc = (service) => {
    // set the maximum message size as 20MB
    return new V1Client(service, grpc.credentials.createInsecure(), {
        "nebula": "node",
        'grpc.max_receive_message_length': 64 * 1024 * 1024,
        'grpc.max_send_message_length': 64 * 1024 * 1024
    });
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

export default {
    EchoClient,
    V1Client,
    EchoRequest,
    EchoResponse,
    ListTables,
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
    DisplayType,
    CustomType,
    CustomColumn,
    QueryRequest,
    Statistics,
    DataType,
    QueryResponse,
    LoadError,
    LoadType,
    LoadRequest,
    LoadResponse,
    static_res,
    qc,
    grpc,
    jsonb
};