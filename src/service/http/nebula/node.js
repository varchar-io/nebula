const {
    EchoRequest,
    EchoResponse,
    TableStateRequest,
    TableStateResponse
} = require('nebula-pb');

const {
    EchoClient,
    V1Client
} = require('nebula-node-rpc');

export default {
    EchoClient,
    V1Client,
    EchoRequest,
    TableStateRequest,
    test: function () {
        console.log('test');
    }
};