var fs = require('fs');

// Constants
var HEADER_SIZE = 2048;
var TABLE_SIZE  = 256;

var readable = function(file) {
    this.file = file;
    this.header = new Array(TABLE_SIZE);
    this.hashtables = new Array(TABLE_SIZE);

    this.fd = null;
};

readable.prototype.open = function(callback) {
    var self = this;

    fs.open(this.file, 'r+', readHeader);

    function readHeader(err, fd) {
        if (err) {
            return callback(err);
        }

        self.fd = fd;
        fs.read(fd, new Buffer(HEADER_SIZE), 0, HEADER_SIZE, 0, parseHeader);
    }

    function parseHeader(err, bytesRead, buffer) {
        if (err) {
            return callback(err);
        }

        var i = 0,
            length = TABLE_SIZE,
            bufferPosition = 0,
            position, slotCount;

        for (; i < length; i++) {
            position = buffer.readUInt32LE(bufferPosition);
            slotCount = buffer.readUInt32LE(bufferPosition + 4);

            self.header[i] = {
                position: position,
                slotCount: slotCount
            }

            bufferPosition += 8;
        }

        callback(err);
    }
};

readable.prototype.get = function(key, callback) {
    // TODO
};

readable.prototype.getNext = function(callback) {
    // TODO
};

readable.prototype.close = function(callback) {
    // TODO
};
