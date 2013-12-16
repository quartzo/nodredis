var nodredis = require('./nodredis');
var cl = new nodredis.connect();
cl.on("connect", function () {
    console.log('Connected');
    cl.cmd('get', 'test1', console.log);
    cl.cmd('get', 'test2', console.log);
    cl.cmd('get', 'test3', console.log);
    cl.cmd('get', 'test4', console.log);
    cl.cmd('get', 'test5', console.log);
    cl.cmd('get', 'test6', console.log);
});
cl.on("end",function () {
    console.log("connection ended");
    process.exit();
});
setTimeout(function () {
    cl.cmd('get', 'test7', console.log);
    cl.cmd('get', 'test8', function (err,msg) {
        console.log(err,msg);
        process.exit();
    });
}, 100);
