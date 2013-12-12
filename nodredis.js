var events = require('events');
var util = require('util');
var net = require('net');

function connect(arg0, arg1) {
    this.connected = false;
    this.ended = false;
    this.tosend = [];
    this._callbacks = [];
    if(/^[0-9]+$/.test(arg0+"")) { // port and host
        var host = arg1; 
        var port = arg0; 
        this._connect_redis(host, port);
        return;
    }
    // string 'host:port' or '[mymaster]host:port,host2:port2
    arg0 = ((arg0||"")+"");
    if(arg0[0] != '[') {
        var hostport = arg0.split(':');
        var host = hostport[0]; 
        var port = hostport[1];
        this._connect_redis(host, port);
        return;
    }
    var mhp = arg0.split(']');
    this._master = mhp[0].substr(1);
    this._sentinels = (mhp[1] || '').split(',');
    this._connect_sentinel();
}

util.inherits(connect, events.EventEmitter);

connect.prototype._connect_redis = function(host, port) {
    var self = this;
    this.cl = net.connect(port || 6379, host || 'localhost',
        function() { //'connect' listener
            self.cl.write(Buffer.concat(self.tosend));
            self.tosend = [];
            if(!self.ended) {
                self.connected = true;
                self.emit('connect');
                self._keep_alive();
            }
        }
    );
    this.cl.on('error', function(err) {
        self.connected = false;
        self.ended = true;
        while(self._callbacks.length) {
            var cb = self._callbacks.shift().cb;
            if(cb) cb(err);
        }
        self.emit('end', err);
    });
    var bufbytes = new Buffer(0);
    var bufelements = [];
    this.cl.on('data', function(data) {
        bufbytes = Buffer.concat([bufbytes, data]);
        parse_elements(bufelements, bufbytes);
        while(has_one_complete_element(bufelements)) {
            var dadcb = self._callbacks.shift();
            var cb = dadcb && dadcb.cb;
            var el = get_complete_element(bufelements, dadcb && dadcb.bin);
            if(!cb) continue;
            if(el[0] == 'e') cb(el[1]);
            else cb(null, el[1]);
        };
    });
    this.cl.on('end', function() {
        self.connected = false;
        self.ended = true;
        while(self._callbacks.length) {
            var cb = self._callbacks.shift().cb;
            if(cb) cb(new Error('redis connection destroyed'));
        }
        self.emit('end');
    });
}

connect.prototype._connect_sentinel = function() {
    var self = this;
    var sentinel = self._sentinels.shift();
    if(sentinel == undefined) {
        self.ended = true;
        var err = new Error("can't find sentinel");
        while(self._callbacks.length) {
            var cb = self._callbacks.shift().cb;
            if(cb) cb(err);
        }
        self.emit('end', err);
        return;
    }
    var sentinelsep = sentinel.split(':');
    var host = sentinelsep[0] || "localhost";
    var port = sentinelsep[1] || 26379;
    var cl;
    var tmout = setTimeout(function() {
        cl.destroy();
        self._connect_sentinel();
    }, 5000);
    cl = net.connect(port, host,
        function() { //'connect' listener
            clearTimeout(tmout);
            cl.write(serialize(['sentinel', 'get-master-addr-by-name', 
                    self._master]));
        }
    );
    cl.on('error', function(err) {
        clearTimeout(tmout);
        self._connect_sentinel();
    });
    cl.on('end', function() {
        clearTimeout(tmout);
        self._connect_sentinel();
    });
    var bufbytes = new Buffer(0);
    var bufelements = [];
    cl.on('data', function(data) {
        bufbytes = Buffer.concat([bufbytes, data]);
        parse_elements(bufelements, bufbytes);
        if(has_one_complete_element(bufelements)) {
            cl.destroy();
            var el = get_complete_element(bufelements, false);
            if(!Array.isArray(el[1])) {
                self._connect_sentinel();
                return;
            }
            var host = el[1][0], port = el[1][1];
            self._connect_redis(host, port);
        };
    });
}

function parse_one_element(buf) {
    var p, el;
    var tp = String.fromCharCode(buf[0]);
    for(p = 0;; p++) {
        if(p >= buf.length) return; // need more data
        if(buf[p] == 10) break; // is \n
    }
    var dadp = p;
    if(buf[dadp-1] == 13) dadp--; // is \r
    p++;
    var dad = buf.slice(1, dadp);
    switch(tp) {
        case '-':
            el = ['e',
                new Error('error: -'+dad.toString('utf8'))];
            break;
        case '+':
            el = ['d', dad];
            break;
        case '$':
            var sz = parseInt(dad.toString('utf8'));
            if(sz <= -1) {
                el = ['d',null];
            } else {
                if(buf.length < p+sz+2)
                    return; // need more data
                el = ['d',buf.slice(p,p+sz)];
                p += sz+2;
            }
            break;
        case '*':
            var sz = parseInt(dad.toString('utf8'));
            if(sz <= '-1') {
                el = ['d',null];
            } else {
                el = ['l',sz];
            };
            break;
        case ':':
            var num = parseInt(dad.toString('utf8'));
            el = ['d', num]; // integer
            break;
        default:
            throw(new Error('redis protocol problems'));
    }
    return [el, buf.slice(p)];
}

function parse_elements(bufelems, bufbytes) {
    for(;;) {
        var elb = parse_one_element(bufbytes);
        if(!elb) break;
        bufelems.push(elb[0]);
        bufbytes = elb[1];
    }
}

function has_one_complete_element(bufelements) {
    // check if there are enough elements
    var needed = 1, pos = 0;
    while(needed) {
        var el = bufelements[pos++];
        if(!el) return false;
        needed--;
        if(el[0] == 'l') needed += el[1];
    }
    return true;
}

function get_complete_element(bufelements, binary_resp) {
    var el = bufelements.shift();
    if(el[0] != 'l') {
        if(!binary_resp && Buffer.isBuffer(el[1]))
            el[1] = el[1].toString('utf8');
        return el;
    }
    var qtd = el[1];
    var resp = [];
    for(; qtd > 0; qtd--) {
        el = get_complete_element(bufelements, binary_resp);
        resp.push(el[1]);
    }
    return ['d', resp];
}

var crlf = '\r\n';

function serialize(elems) {
    var bufs = [];
    var strs = [ '*'+elems.length+crlf ];
    for(var i=0; i<elems.length; i++) {
        var el = elems[i];
        if(Buffer.isBuffer(el)) {
            strs.push('$'+el.length+crlf);
            bufs.push(new Buffer(strs.join('')));
            bufs.push(el);
            strs = [crlf];
        } else {
            el = ''+el;
            strs.push('$'+Buffer.byteLength(el)+crlf+el+crlf);
        }
    };
    bufs.push(new Buffer(strs.join('')));
    return Buffer.concat(bufs);
}

connect.prototype.req_cmd = function(params, cb, binaryresp) {
    if(this.ended) {
        if(cb) return cb(new Error('redis connection closed'));
        return;
    }
    var buf = serialize(params);
    if(this.connected)
        this.cl.write(buf);
    else
        this.tosend.push(buf);
    this._callbacks.push({cb:cb, bin:binaryresp});
}

connect.prototype._keep_alive = function() {
    var self = this;
    if((!this.connected) || this.ended) return;
    this.req_cmd(['ping'], null, false);
    setTimeout(function () {
        self._keep_alive();
    }, 180000);
}

connect.prototype.cmd = function() {
    var args = Array.prototype.slice.call(arguments, 0);
    var cb = null;
    if(typeof args[args.length-1] == 'function') cb = args.pop();
    return this.req_cmd(args, cb, false);
}

connect.prototype.cmdbin = function() {
    var args = Array.prototype.slice.call(arguments, 0);
    var cb = null;
    if(typeof args[args.length-1] == 'function') cb = args.pop();
    return this.req_cmd(args, cb, true);
}

connect.prototype.end = function () {
    if(this.connected) 
        this.cl.end();
    this.ended = true;
    this.connected = false;
};

connect.prototype.destroy = function (fire_callbacks) {
    this.cl.destroy();
    if(fire_callbacks) {
        while(this._callbacks.length) {
            var cb = this._callbacks.shift().cb;
            if(cb) cb(new Error('redis connection closed'));
        }
    }
};

module.exports = {
    connect: connect
};

