var fs      = require('fs');
var promise = require('bluebird');
var util    = require('./util');

promise.onPossiblyUnhandledRejection();

var open = promise.promisify(fs.open);
var read = promise.promisify(fs.read);
var close = promise.promisify(fs.close);

var HEADER_SIZE = util.HEADER_SIZE;
var TABLE_SIZE  = util.TABLE_SIZE;
var INT_SIZE    = util.INT_SIZE;
var ENTRY_SIZE  = util.ENTRY_SIZE;

var hashKey        = util.hashKey;
var lookupSubtable = util.lookupSubtable;

function EntryMismatchError(message) {
    this.message = message;
    this.name = "EntryMismatchError";
}
EntryMismatchError.prototype = Object.create(Error.prototype);
EntryMismatchError.prototype.constructor = EntryMismatchError;

var readable = module.exports = function(file) {
    this._file = file;
    this._fd = null;
}

readable.prototype.open = function(callback) {
    function fileOpened(fd) {
        this._fd = fd;
    }

    return open(this._file, 'r').bind(this)
        .then(fileOpened)
        .then(this._readHeader)
        .nodeify(callback);
}

readable.prototype.close = function(callback) {
    if (this._fd) {
        var fd = this._fd;

        this._fd = null;
        return close(fd).nodeify(callback);
    } else {
        throw new Error('cdb not opened');
    }
};

readable.prototype.getRecord = function(key, offset, callback) {
    if (!this._fd) {
        throw new Error('cdb not opened');
    }

    if (typeof offset == 'function') {
        callback = offset;
        offset = 0;
    } else {
        offset = offset || 0;
    }

    var hash = hashKey(key);
    var subtableIndex = lookupSubtable(hash);
    var slot = hash >>> 8;

    function loop(slot) {
        return this._readEntry(subtableIndex, slot).bind(this).then(
            function checkEntry(entry) {
                if (!entry) {
                    throw new Error('Hash not found.');
                } else if (hash != entry.hash) {
                    throw new EntryMismatchError();
                }

                return entry;
            }
        ).then(this._readKey).then(
            function checkKey(entry) {
                if (key != entry.key) {
                    throw new EntryMismatchError();
                } else if (offset != 0) {
                    offset--;
                    throw new EntryMismatchError();
                }

                return entry;
            }
        ).catch(EntryMismatchError, function retryNextEntry(error) {
            return loop.call(this, slot + 1);
        });
    }

    return loop.call(this, slot).then(this._readData).nodeify(callback);
};

readable.prototype._readHeader = function() {
    var header = new Array(TABLE_SIZE);
    var offset = 0;

    return readIntoBuffer(this._fd, HEADER_SIZE, 0).bind(this).then(
        function parseHeader(buffer) {
            for (var i = 0; i < TABLE_SIZE; i++) {
                var position = buffer.readUInt32LE(offset);
                var entries = buffer.readUInt32LE(offset + INT_SIZE);

                header[i] = {position: position, entries: entries};
                offset += ENTRY_SIZE;
            }

            this._header = header;
            return this;
        }
    );
};

readable.prototype._readEntry = function(subtableIndex, slot) {
    var headerEntry = this._header[subtableIndex];
    var subtablePosition = headerEntry.position;
    var numEntries = headerEntry.entries;

    if (numEntries == 0) {
        return promise.resolve(null);
    }

    var slot = slot % numEntries;
    var offset = subtablePosition + (slot * ENTRY_SIZE);

    return readIntoBuffer(this._fd, ENTRY_SIZE, offset).then(
        function bufferToEntry(buffer) {
            var hash = buffer.readUInt32LE(0);
            var position = buffer.readUInt32LE(INT_SIZE);

            return hash ? {hash: hash, position: position, slot: slot} : null;
        }
    );
};

readable.prototype._readKey = function(entry) {
    var position = entry.position;

    return readIntoBuffer(this._fd, INT_SIZE, position).bind(this).then(
        function bufferToKeyLength(buffer) {
            return buffer.readUInt32LE(0);
        }
    ).then(
        function readKey(keyLength) {
            position += INT_SIZE;
            return readIntoBuffer(this._fd, keyLength, position);
        }
    ).then(
        function addKeyToEntry(buffer) {
            entry.key = buffer.toString();
            return entry;
        }
    );
};

readable.prototype._readData = function(entry) {
    var position = entry.position + INT_SIZE + entry.key.length;

    return readIntoBuffer(this._fd, INT_SIZE, position).bind(this).then(
        function bufferToDataLength(buffer) {
            return buffer.readUInt32LE(0);
        }
    ).then(
        function readData(dataLength) {
            position += INT_SIZE;
            return readIntoBuffer(this._fd, dataLength, position);
        }
    ).call('toString');
};

function readIntoBuffer(fd, size, position) {
    return read(fd, new Buffer(size), 0, size, position).get(1);
}
