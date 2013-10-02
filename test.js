var nodredis = require('./nodredis');
var cl = new nodredis.connect();
console.log(['is it connected?', cl.connected]);
cl.on("connect", function () {
      console.log('Connected');
        console.log(['is it connected?', cl.connected]);
});
cl.on("end",function () {
      console.log("connection ended");
});
cl.cmd('setex', 'test', 120, "One Two Three");
cl.cmd('get', 'test', console.log);
cl.cmdbin('set', 'test', Buffer('c0c1c2c3c400010203','hex'));
cl.cmdbin('get', 'test', console.log);
cl.cmdbin('del', 'test', function(err, res) {
    cl.end();
    cl.cmdbin('get', 'test', console.log);
});

