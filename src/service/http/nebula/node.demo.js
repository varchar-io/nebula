const grpc = require('grpc');
const NebulaClient = require("./dist/node/main");

var serviceAddr = "dev-shawncao:9190";
var client = new NebulaClient.EchoClient(serviceAddr, grpc.credentials.createInsecure());

// simple unary call
var request = new NebulaClient.EchoRequest();
request.setName("Trends On Nebula");

client.echoBack(request, (err, response) => {
    if (err !== null) {
        console.log(err);
    } else {
        console.log(response.getMessage());
    }
});