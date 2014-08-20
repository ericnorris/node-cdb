var fs = require('fs');
var promise = require('bluebird');

var open = promise.promisify(fs.open);
var read = promise.promisify(fs.read);

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

var HEADER_SIZE = 2048;
var TABLE_SIZE = 256;
var INT_SIZE = 4;
var ENTRY_SIZE = 2 * INT_SIZE;

var readable_cdb = module.exports = function(file) {
    this._file = file;

    this._opening = open(file, 'r').bind(this).then(function fileOpened(fd) {
        this._fd = fd;

        return readIntoBuffer(fd, HEADER_SIZE, 0);
    }).then(parseHeader).then(function headerParsed(header) {
        this._header = header;

        delete this._opening;
    });
}

readable_cdb.prototype.getRecord = function(key, callback) {
    if (this._opening) {
        this._opening.try(this.getRecord, [key, callback]);
    }

    var hash = hashKey(key);
    var subtableIndex = lookupSubtable(hash);

    var headerEntry = this._header[subtableIndex];
    var subtablePosition = headerEntry.position;
    var numEntries = headerEntry.entries;

    if (numEntries == 0) {
        callback(null, null);
    }

    // get the hashpair, and
    // if hash != hash, retry at next slot
    // else if hash == hash, try reading key
    // get the key, and
    // if key != key, retry hash at the next slot
    // else if key == key, read data
};

readable_cdb.prototype._readEntry = function(subtableIndex, slot) {
    var headerEntry = this._header[subtableIndex];
    var subtablePosition = headerEntry.position;
    var numEntries = headerEntry.entries;
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

readable_cdb.prototype._findHashInSubtable = function(hash, slot) {
    var subtableIndex = lookupSubtable(hash);
    var slot = (slot || (hash >>> 8));

    return this._readEntry(subtableIndex, slot).bind(this).then(
        function checkEntry(entry) {
            if (!entry) {
                throw new Error('Hash not found.');
            } else if (entry.hash != hash) {
                return this._findHashInSubtable(hash, entry.slot + 1);
            } else {
                return entry;
            }
        }
    );
};

readable_cdb.prototype._readKey = function(position) {
    return readIntoBuffer(this._fd, INT_SIZE, position).bind(this).then(
        function bufferToKeyLength(buffer) {
            return buffer.readUInt32LE(0);
        }
    ).then(
        function readKey(keyLength) {
            position += INT_SIZE;
            return readIntoBuffer(this._fd, keyLength, position);
        }
    ).call('toString');
};

readable_cdb.prototype._findKey = function(key) {
    var hash = hashKey(key);
    return findKeyAtSlot();

    function findKeyAtSlot(slot) {
        return this._findHashInSubtable(hash, slot).then(
            function getKeyForEntry(entry) {
                
            }
        );

        return this._findHashInSubtable(hash).then(function getKeyForEntry(entry) {
            return this._readKey(entry.position).then(function checkKey(key) {
                if (key != recordKey) {
                    return this._findHashInSubtable(hash, entry.slot + 1);
                } else {
                    var position = entry.position
                    return this._readData(entry.position + recordKey.length + INT_SIZE)
                }
            });
        });
    }
    // get the key, and
    // if key != key, retry hash at the next slot
    // else if key == key, read data
};