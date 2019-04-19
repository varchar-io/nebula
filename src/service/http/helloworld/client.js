const {
    HelloRequest,
    RepeatHelloRequest,
    HelloReply
} = require('../../gen/helloworld/helloworld_pb.js');

const {
    GreeterClient
} = require('../../gen/helloworld/helloworld_grpc_web_pb.js');

var client = new GreeterClient("http://localhost:8080");

// simple unary call
var request = new HelloRequest();
request.setName('World');
console.log("the name set above:" + request.getName());

client.sayHello(request, {}, (err, response) => {
    console.log(response.getMessage());
});


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