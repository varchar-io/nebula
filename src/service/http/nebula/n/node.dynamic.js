var PROTO_PATH = __dirname + '/../../protos/nebula.proto';

var grpc = require('grpc');
var protoLoader = require('@grpc/proto-loader');
var packageDefinition = protoLoader.loadSync(
    PROTO_PATH,
    {keepCase: true,
     longs: String,
     enums: String,
     defaults: true,
     oneofs: true
    });

var services = grpc.loadPackageDefinition(packageDefinition);

function main() {
    console.log();
    var client = new services.nebula.service.Echo('dev-shawncao:9190',
        grpc.credentials.createInsecure());
    client.echoBack({name: "Trends On Nebula"}, function (err, response) {
        if (err !== null) {
            console.log(err);
        } else {
            console.log(response.message);
        }
    });
}

main();