var nodredis = require('./nodredis');
//var cl = new nodredis.connect();
var cl = new nodredis.connect('[mymaster]localhost:36379,localhost:26379');
console.log(['is it connected?', cl.connected]);
cl.on("connect", function () {
    console.log('Connected');
    console.log(['is it connected?', cl.connected]);
});
cl.on("end",function () {
    console.log("connection ended");
    process.exit();
});
cl.cmd('setex', 'test', 120, "One Two Three");
cl.cmd('get', 'test', console.log);
cl.cmdbin('set', 'test', Buffer('c0c1c2c3c400010203','hex'));
cl.cmdbin('get', 'test', console.log);
cl.cmdbin('del', 'test', function(err, res) {
    cl.end();
    cl.cmdbin('get', 'test', function(err, res) {
        console.log(err, res);
    });
});
