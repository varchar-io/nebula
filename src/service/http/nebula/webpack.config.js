var path = require('path');

module.exports = {
    mode: "development",
    entry: {
        client: './client.js',
    },
    output: {
        filename: './main.js',
        path: path.resolve(__dirname, 'dist'),
        library: 'NebulaClient',
        libraryExport: 'default'
    },
    resolve: {
        alias: {
            "google-protobuf": path.resolve(__dirname, './node_modules/google-protobuf/'),
            "grpc-web": path.resolve(__dirname, './node_modules/grpc-web/'),
            "nebula-pb": path.resolve(__dirname, '../../gen/nebula/nebula_pb.js'),
            "nebula-rpc": path.resolve(__dirname, '../../gen/nebula/nebula_grpc_web_pb.js'),
        },
        extensions: [
            ".ts",
            ".js",
            ".json"
        ],
        descriptionFiles: ['package.json'],
        modules: [
            "./node_modules"
        ],
    }
}