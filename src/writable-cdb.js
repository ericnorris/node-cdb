var fs = require('fs');
var util = require('./util');

var HEADER_SIZE = util.HEADER_SIZE;
var TABLE_SIZE  = util.TABLE_SIZE;
var INT_SIZE    = util.INT_SIZE;
var ENTRY_SIZE  = util.ENTRY_SIZE;

var hashKey = util.hashKey;
var lookupSubtable = util.lookupSubtable;

var writeable_cdb = module.exports = function(file) {
    this._header = new Array(TABLE_SIZE);
    this._subtables = new Array(TABLE_SIZE);

    this._file = file;
    this._filePosition = HEADER_SIZE;
    this._recordStream = fs.createWriteStream(file, {start: HEADER_SIZE});

    for (var i = 0; i < TABLE_SIZE; i++) {
        this._subtables[i] = [];
    }
};

function getBufferForRecord(key, data) {
    var keySize = INT_SIZE + key.length;
    var dataSize = INT_SIZE + data.length;
    var buffer = new Buffer(keySize + dataSize);

    buffer.writeUInt32LE(key.length, 0);
    buffer.write(key, INT_SIZE);
    buffer.writeUInt32LE(data.length, keySize);
    buffer.write(data, keySize + INT_SIZE);
    return buffer;
}

function getBufferForSubtable(subtable) {
    var entries = subtable.length * 2;
    var buffer = new Buffer(entries * ENTRY_SIZE);
    var slots = new Array(entries);

    for (var i = 0, length = subtable.length; i < length; i++) {
        var entry = subtable[i];
        var hash = entry.hash;
        var position = entry.position;

        var slot = (hash >>> 8) % entries;
        var offset = slot * ENTRY_SIZE
        while (slots[slot]) {
            slot = (slot + 1) % entries;
            offset = slot * ENTRY_SIZE;
        }

        slots[slot] = true;
        buffer.writeUInt32LE(hash, offset);
        buffer.writeUInt32LE(position, offset + INT_SIZE);
    }

    return buffer;
}

function getBufferForHeader(headerTable) {
    var buffer = new Buffer(HEADER_SIZE);
    var offset = 0;

    for (var i = 0; i < TABLE_SIZE; i++) {
        var position = headerTable[i].position;
        var entries = headerTable[i].entries;

        buffer.writeUInt32LE(position, offset);
        buffer.writeUInt32LE(entries, offset + INT_SIZE);
        offset += ENTRY_SIZE;
    }

    return buffer;
}

writeable_cdb.prototype.addRecord = function(key, data) {
    var hash = hashKey(key);
    var buffer = getBufferForRecord(key, data);
    var self = this;

    this._recordStream.write(buffer, '', function recordFlushed() {
        var subtableIndex = lookupSubtable(hash);

        self._subtables[subtableIndex].push({hash: hash, position: self._filePosition});
        self._filePosition += buffer.length;
    });
};

writeable_cdb.prototype.finalizeDB = function(callback) {
    var self = this;
    this._recordStream.on('finish', function allRecordsFlushed() {
        self._writeSubtables(callback);
    });

    this._recordStream.end();
};

writeable_cdb.prototype._writeSubtables = function(callback) {
    var self = this;
    var offset = this._filePosition;

    this._subtableStream = fs.createWriteStream(this._file, {flags: 'a'});
    for (var i = 0; i < TABLE_SIZE; i++) {
        var subtable = this._subtables[i];
        var buffer = getBufferForSubtable(subtable);

        this._subtableStream.write(buffer);
        this._header[i] = {position: offset, entries: subtable.length * 2};
        offset += buffer.length;
    }

    this._subtableStream.on('finish', function allSubtablesFlushed() {
        self._writeHeader(callback);
    })

    this._subtableStream.end();
};

writeable_cdb.prototype._writeHeader = function(callback) {
    var buffer = getBufferForHeader(this._header);

    fs.writeFile(this._file, buffer, {flag: 'r+'}, callback);
}
