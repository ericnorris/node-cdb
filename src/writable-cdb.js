var events = require('events');
var fs = require('fs');

// Constants
var HEADER_SIZE = 2048;
var TABLE_SIZE  = 256;

// Writable CDB definition
var writable = module.exports = function(file) {
    this._file = file;
    this._recordStream = null;
    this._filePosition = 0;
    this._header = new Array(TABLE_SIZE);
    this._hashtables = new Array(TABLE_SIZE);
};

// extend EventEmitter for emit()
util.inherits(writable, events.EventEmitter);

writable.prototype.open = function(callback) {
    var recordStream = fs.createWriteStream(this._file, {start: HEADER_SIZE}),
        callback = callback || function() {},
        self = this;

    // Set up event handlers
    function fileOpened(fd) {
        self._recordStream = recordStream;
        self._filePosition = HEADER_SIZE;
        self.emit('open');
        callback();
    }

    function error(err) {
        self.emit('error', err);
        callback(err);
    }

    // Listen for events for record stream
    recordStream.once('open', fileOpened);
    recordStream.once('error', error);
};

writable.prototype.put = function(key, data) {
    var recordHeader = new Buffer(8),
        hash = hashKey(key)
        hashtableIndex = hash & 255,
        hashtable = this._hashtables[hashtableIndex] || [];

    recordHeader.writeUInt32LE(key.length, 0);
    recordHeader.writeUInt32LE(data.length, 4);

    this._recordStream.write(recordHeader);
    this._recordStream.write(key);
    this._recordStream.write(data);

    hashtable.push({hash: hash, position = this._filePosition});
    this._hashtables[hashtableIndex] = hashtable;

    this._filePosition += recordHeader.length + key.length + data.length;
};

writable.prototype.close = function(callback) {
    // TODO
};

// Hashing implementation
function hashKey(key) {
    var hash = 5381;

    for (var i = 0, length = key.length; i < length; i++) {
        hash = ((((hash << 5) >>> 0) + hash) ^ key.charCodeAt(i)) >>> 0;
    }

    return hash;
}
