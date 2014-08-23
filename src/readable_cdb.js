var fs = require('fs');
var promise = require('bluebird');

promise.onPossiblyUnhandledRejection();

var open = promise.promisify(fs.open);
var read = promise.promisify(fs.read);

var HASH_START = 5381;
var HEADER_SIZE = 2048;
var TABLE_SIZE = 256;
var INT_SIZE = 4;
var ENTRY_SIZE = 2 * INT_SIZE;

function readIntoBuffer(fd, size, position) {
    return read(fd, new Buffer(size), 0, size, position).get(1);
}

function parseHeader(buffer) {
    var header = new Array(TABLE_SIZE);
    var offset = 0;

    for (var i = 0; i < TABLE_SIZE; i++) {
        var position = buffer.readUInt32LE(offset);
        var entries = buffer.readUInt32LE(offset + INT_SIZE);

        header[i] = {position: position, entries: entries};
        offset += ENTRY_SIZE;
    }

    return header;
}

function hashKey(key) {
    var hash = HASH_START;
    for (var i = 0, length = key.length; i < length; i++) {
        hash = ((((hash << 5) >>> 0) + hash) ^ key.charCodeAt(i)) >>> 0;
    }
    return hash;
}

function lookupSubtable(hash) {
    return hash & 255;
}

function EntryMismatchError(message) {
    this.message = message;
    this.name = "EntryMismatchError";
    Error.captureStackTrace(this, EntryMismatchError);
}
EntryMismatchError.prototype = Object.create(Error.prototype);
EntryMismatchError.prototype.constructor = EntryMismatchError;

var readable_cdb = module.exports = function(file) {
    this._file = file;

    this._opening = open(file, 'r').bind(this).then(function fileOpened(fd) {
        this._fd = fd;

        return readIntoBuffer(fd, HEADER_SIZE, 0);
    }).then(parseHeader).then(function headerParsed(header) {
        delete this._opening;

        this._header = header;
        return this;
    });
}

readable_cdb.prototype.getRecord = function(key, callback) {
    if (this._opening) {
        return this._opening.call('getRecord', key, callback);
    }

    var hash = hashKey(key);
    var subtableIndex = lookupSubtable(hash);
    var slot = hash >>> 8;

    function loop(slot) {
        return this._readEntry(subtableIndex, slot).bind(this).then(
            function checkEntry(entry) {
                if (!entry) {
                    throw new Error('Hash not found.');
                } else if (entry.hash != hash) {
                    throw new EntryMismatchError('hash ' + entry.hash + ' != ' + hash + ' [' + key + ', ' + subtableIndex + ', ' + entry.slot + ']');
                }

                return entry;
            }
        ).then(this._readKey).then(
            function checkKey(entry) {
                if (entry.key != key) {
                    throw new EntryMismatchError('key ' + entry.key + ' != ' + key);
                }

                return entry;
            }
        ).catch(EntryMismatchError, function retryNextEntry(error) {
            return loop.call(this, slot + 1);
        });
    }

    return loop.call(this, slot).then(this._readData).nodeify(callback);
};

readable_cdb.prototype._readEntry = function(subtableIndex, slot) {
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

readable_cdb.prototype._readKey = function(entry) {
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

readable_cdb.prototype._readData = function(entry) {
    if (entry.key== undefined) {
        console.log('broken entry', entry);
    }
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
