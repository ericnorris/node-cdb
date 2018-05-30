'use strict';

var fs = require('fs'),
    _  = require('./cdb-util'),
    HEADER_SIZE = 2048,
    TABLE_SIZE  = 256;

var readable = module.exports = function(file) {
    this.file = file;
    this.header = new Array(TABLE_SIZE);

    this.fd = null;
    this.bookmark = null;
};

readable.prototype.open = function(callback) {
    var self = this;

    fs.open(this.file, 'r', readHeader);

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

        var bufferPosition = 0,
            i, position, slotCount;

        for (i = 0; i < TABLE_SIZE; i++) {
            position = buffer.readUInt32LE(bufferPosition);
            slotCount = buffer.readUInt32LE(bufferPosition + 4);

            self.header[i] = {
                position: position,
                slotCount: slotCount
            };

            bufferPosition += 8;
        }

        callback(null, self);
    }
};

readable.prototype.get = function(key, offset, callback) {
    var hash = _.cdbHash(key),
        hashtableIndex = hash & 255,
        hashtable = this.header[hashtableIndex],
        position = hashtable.position,
        slotCount = hashtable.slotCount,
        slot = (hash >>> 8) % slotCount,
        trueKeyLength = Buffer.byteLength(key),
        self = this,
        hashPosition, recordHash, recordPosition, keyLength, dataLength;

    if (typeof(offset) == 'function') {
        callback = offset;
        offset = 0;
    }

    if (slotCount === 0) {
        return callback(null, null);
    }

    readSlot(slot);

    function readSlot(slot) {
        hashPosition = position + ((slot % slotCount) * 8);

        fs.read(self.fd, new Buffer(8), 0, 8, hashPosition, checkHash);
    }

    function checkHash(err, bytesRead, buffer) {
        if (err) {
            return callback(err);
        }

        recordHash = buffer.readUInt32LE(0);
        recordPosition = buffer.readUInt32LE(4);

        if (recordHash == hash) {
            fs.read(self.fd, new Buffer(8), 0, 8, recordPosition, readKey);
        } else if (recordHash === 0) {
            callback(null, null);
        } else {
            readSlot(++slot);
        }
    }

    function readKey(err, bytesRead, buffer) {
        if (err) {
            return callback(err);
        }

        keyLength = buffer.readUInt32LE(0);
        dataLength = buffer.readUInt32LE(4);

        // In the rare case that there is a hash collision, check the key size
        // to prevent reading in a key that will definitely not match.
        if (keyLength != trueKeyLength) {
            return readSlot(++slot);
        }

        fs.read(self.fd, new Buffer(keyLength), 0, keyLength,
            recordPosition + 8, checkKey);
    }

    function checkKey(err, bytesRead, buffer) {
        if (err) {
            return callback(err);
        }

        if (buffer.toString() == key && offset === 0) {
            fs.read(self.fd, new Buffer(dataLength), 0, dataLength,
                recordPosition + 8 + keyLength, returnData);
        } else if (offset !== 0) {
            offset--;
            readSlot(++slot);
        } else {
            readSlot(++slot);
        }
    }

    function returnData(err, bytesRead, buffer) {
        // Fill out bookmark information so getNext() will work
        self.bookmark = function(newCallback) {
            callback = newCallback;
            readSlot(++slot);
        };

        callback(err, buffer);
    }
};

readable.prototype.getNext = function(callback) {
    if (this.bookmark) {
        this.bookmark(callback);
    }
};

readable.prototype.close = function(callback) {
    fs.close(this.fd, callback);
};
