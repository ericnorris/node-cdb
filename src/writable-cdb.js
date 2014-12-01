var EventEmitter = require('events').EventEmitter;

var writable = function(file) {
    // constructor
}

writable.prototype.open = function(callback) {
    // TODO
};

writable.prototype.put = function(key, data, callback) {
    // TODO
}

writable.prototype.close = function(callback) {
    // TODO
}

// extend EventEmitter for emit()
util.inherits(writable, EventEmitter);
