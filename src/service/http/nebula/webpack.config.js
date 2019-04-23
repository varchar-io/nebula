var path = require('path');

module.exports = {
    mode: "development",
    resolve: {
        alias: {
            "google-protobuf": path.resolve(__dirname, './node_modules/google-protobuf/'),
            "grpc-web": path.resolve(__dirname, './node_modules/grpc-web/'),
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