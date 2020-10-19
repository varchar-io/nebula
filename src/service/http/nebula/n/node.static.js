// to enable node find other installed modules 
// we should set NODE_PATH before run "node node.static.js"
// "export NODE_PATH=./node_modules" if running in current folder
var messages = require('../../../gen/nebula/node/nebula_pb');
var services = require('../../../gen/nebula/node/nebula_grpc_pb');

var grpc = require('grpc');

function main() {
    console.log();
    var client = new services.V1('dev-shawncao:9190',
        grpc.credentials.createInsecure());
    var req = new messages.EchoRequest();
    req.setName("Static Trends");
    client.echo(req, function (err, response) {
        if (err !== null) {
            console.log(err);
        } else {
            console.log(response.getMessage());
        }
    });
}

main();