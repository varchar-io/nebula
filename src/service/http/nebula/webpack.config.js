var path = require('path');
var nodeExternals = require('webpack-node-externals');

const resolveConfig = {
    alias: {
        "google-protobuf": path.resolve(__dirname, './node_modules/google-protobuf/'),
        "grpc-web": path.resolve(__dirname, './node_modules/grpc-web/'),
        "grpc": path.resolve(__dirname, './node_modules/grpc/'),
        "fs": path.resolve(__dirname, './node_modules/fs/'),
        "nebula-pb": path.resolve(__dirname, '../../gen/nebula/node/nebula_pb.js'),
        "nebula-web-rpc": path.resolve(__dirname, '../../gen/nebula/nebula_grpc_web_pb.js'),
        "nebula-node-rpc": path.resolve(__dirname, '../../gen/nebula/node/nebula_grpc_pb.js'),
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
};

const webConfig = {
    mode: "production",
    target: "web",
    entry: {
        web: './client.js',
    },
    output: {
        filename: './[name]/main.js',
        path: path.resolve(__dirname, 'dist'),
        library: 'NebulaClient',
        libraryExport: 'default'
    },
    resolve: resolveConfig
};

const nodeConfig = {
    mode: "production",
    target: "node",
    context: __dirname,
    node: false,
    externals: [nodeExternals()],
    entry: {
        node: './node.js'
    },
    output: {
        filename: './[name]/main.js',
        path: path.resolve(__dirname, 'dist'),
        library: '',
        libraryTarget: 'commonjs',
        libraryExport: 'default'
    },
    resolve: resolveConfig
};

module.exports = [webConfig, nodeConfig];