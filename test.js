var nodredis = require('./nodredis');
nodredis.get_master_from_sentinel('mymaster', 
            ['localhost:36379','localhost:26379'], function(hostport) {
    if(!hostport) {
        console.log("sentinel not found");
        process.exit();
    }
    console.log("connecting to "+hostport);
    var cl = new nodredis.connect(hostport);
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
        cl.cmdbin('get', 'test', function(err, res) {
            console.log(err, res);
            process.exit();
        });
    });
});
