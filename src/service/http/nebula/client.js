const {
    EchoRequest,
    EchoResponse,
    TableStateRequest,
    TableStateResponse
} = require('../../gen/nebula/nebula_pb.js');

const {
    EchoClient,
    V1Client
} = require('../../gen/nebula/nebula_grpc_web_pb.js');

export default {
    EchoClient,
    V1Client,
    EchoRequest,
    TableStateRequest,
    test: function (a, b) {
        return a * b;
    }
};