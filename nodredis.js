var events = require('events');
var util = require('util');
var net = require('net');

function connect(port, host, options) {
    var self = this;
    this.cl = net.connect(port || 6379, host || 'localhost',
        function() { //'connect' listener
            self.connected = true;
            self.emit('connect');
            self._keep_alive();
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
    this.connected = false;
    this.ended = false;
    this._bufbytes = new Buffer(0);
    this._bufelements = [];
    this._callbacks = [];
    this.cl.on('data', function(data) {
        self._consume_bytes(data);
        while(self._has_element()) {
            var dadcb = self._callbacks.shift();
            var cb = dadcb && dadcb.cb;
            var el = self._get_element(dadcb && dadcb.bin);
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

util.inherits(connect, events.EventEmitter);

connect.prototype._consume_bytes = function(data) {
    this._bufbytes = Buffer.concat([this._bufbytes, data]);
    for(;;) {
        var buf = this._bufbytes;
        var p;
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
                this._bufelements.push(['e',
                    new Error('error: -'+dad.toString('utf8'))]);
                break;
            case '+':
                this._bufelements.push(['d', dad]);
                break;
            case '$':
                var sz = parseInt(dad.toString('utf8'));
                if(sz <= -1) {
                    this._bufelements.push(['d',null]);
                } else {
                    if(buf.length < p+sz+2)
                        return; // need more data
                    this._bufelements.push(['d',buf.slice(p,p+sz)]);
                    p += sz+2;
                }
                break;
            case '*':
                var sz = parseInt(dad.toString('utf8'));
                if(sz <= '-1') {
                    this._bufelements.push(['d',null]);
                } else {
                    this._bufelements.push(['l',sz]);
                };
                break;
            case ':':
                var num = parseInt(dad.toString('utf8'));
                this._bufelements.push(['d', num]); // integer
                break;
            default:
                throw(new Error('redis protocol problems'));
                this.end();
        }
        this._bufbytes = buf.slice(p);
    }
}

connect.prototype._has_element = function() {
    // check if there are enough elements
    var needed = 1, pos = 0;
    while(needed) {
        var el = this._bufelements[pos++];
        if(!el) return false;
        needed--;
        if(el[0] == 'l') needed += el[1];
    }
    return true;
}

connect.prototype._get_element = function(binary_resp) {
    var el = this._bufelements.shift();
    if(el[0] != 'l') {
        if(!binary_resp && Buffer.isBuffer(el[1]))
            el[1] = el[1].toString('utf8');
        return el;
    }
    var qtd = el[1];
    var resp = [];
    for(; qtd > 0; qtd--) {
        el = this._get_element(binary_resp);
        resp.push(el[1]);
    }
    return ['d', resp];
}

var crlf = '\r\n';

connect.prototype.req_cmd = function(params, cb, binaryresp) {
    if(this.ended) {
        if(cb) return cb(new Error('redis connection closed'));
        return;
    }
    var bufs = [];
    var strs = [ '*'+params.length+crlf ];
    for(var i=0; i<params.length; i++) {
        var el = params[i];
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
    this.cl.write(Buffer.concat(bufs));
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
    this.ended = true;
    this.cl.end();
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

