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

var serviceAddr = "http://" + window.location.hostname + ":8080";
var client = new EchoClient(serviceAddr);

// simple unary call
var request = new EchoRequest();
request.setName('Trends On Nebula');

client.echoBack(request, {}, (err, response) => {
    var display = document.getElementById("output");
    if (err !== null) {
        display.innerText = "Error code: " + err;
    } else {
        display.innerText = (response == null) ? "Failed to get reply" : response.getMessage();
    }
});

function queryTable() {
    var tableName = document.getElementById("tableName").value;
    var v1Client = new V1Client(serviceAddr);
    var req = new TableStateRequest();
    req.setTable(tableName);

    // call the service
    v1Client.State(req, {}, (err, reply) => {
        var result = document.getElementById("result");
        if (err !== null) {
            result.innerText = "Error code: " + err;
        } else if (reply == null) {
            result.innerText = "Failed to get reply";
        } else {
            result.innerText = "Blocks: " + reply.getBlockCount() + ", Rows: " + reply.getRowCount() + ", Size: " + reply.getMemSize() + " bytes";
        }
    });
}


// server streaming call
// var streamRequest = new RepeatHelloRequest();
// streamRequest.setName('World');
// streamRequest.setCount(5);

// var stream = client.sayRepeatHello(streamRequest, {});
// stream.on('data', (response) => {
//     console.log(response.getMessage());
// });


// // deadline exceeded
// var deadline = new Date();
// deadline.setSeconds(deadline.getSeconds() + 1);

// client.sayHelloAfterDelay(request, {
//         deadline: deadline.getTime()
//     },
//     (err, response) => {
//         console.log('Got error, code = ' + err.code +
//             ', message = ' + err.message);
//     });