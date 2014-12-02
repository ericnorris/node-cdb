var events = require('events');
var fs = require('fs');

// Constants
var HEADER_SIZE = 2048;

// Writable CDB definition
var writable = module.exports = function(file) {
    this._file = file;
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

writable.prototype.put = function(key, data, callback) {
    // TODO
};

writable.prototype.close = function(callback) {
    // TODO
};
