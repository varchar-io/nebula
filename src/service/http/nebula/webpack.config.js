var path = require('path');
var nodeExternals = require('webpack-node-externals');

const resolveConfig = {
    alias: {
        "google-protobuf": path.resolve(__dirname, './node_modules/google-protobuf/'),
        "grpc-web": path.resolve(__dirname, './node_modules/grpc-web/'),
        "grpc": path.resolve(__dirname, './node_modules/grpc/'),
        "fs": path.resolve(__dirname, './node_modules/fs/'),
        "nebula-pb": path.resolve(__dirname, '../../gen/nebula/nodejs/nebula_pb.js'),
        "nebula-web-rpc": path.resolve(__dirname, '../../gen/nebula/nebula_grpc_web_pb.js'),
        "nebula-node-rpc": path.resolve(__dirname, '../../gen/nebula/nodejs/nebula_grpc_pb.js'),
        "jquery": path.resolve(__dirname, './s/jquery.min.js'),
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
        library: 'neb',
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
        node: './n/node.js'
    },
    output: {
        // UMD works fine too, such as library='neb', libraryTarget='umd'
        // for commonjs, we rename the file ext as cjs for working better in ESM env.
        // note that, webpack doesn't crunch 'cjs' but 'js', so the file will be larger
        filename: './[name]/main.cjs',
        path: path.resolve(__dirname, 'dist'),
        library: '',
        libraryTarget: 'commonjs',
        libraryExport: 'default'
    },
    resolve: resolveConfig
};

module.exports = [webConfig, nodeConfig];