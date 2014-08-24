var fs = require('fs');
var promise = require('bluebird');
var util = require('./util');

var HEADER_SIZE = util.HEADER_SIZE;
var TABLE_SIZE  = util.TABLE_SIZE;
var INT_SIZE    = util.INT_SIZE;
var ENTRY_SIZE  = util.ENTRY_SIZE;

var hashKey        = util.hashKey;
var lookupSubtable = util.lookupSubtable;

var writable = module.exports = function(file) {
    this._header = new Array(TABLE_SIZE);
    this._subtables = new Array(TABLE_SIZE);
    this._file = file;
    this._recordStream = null;
    this._subtableStream = null;
    this._filePosition = 0;

    for (var i = 0; i < TABLE_SIZE; i++) {
        this._subtables[i] = [];
    }
};

writable.prototype.open = function(callback) {
    var recordStream = fs.createWriteStream(this._file, {start: HEADER_SIZE});
    var self = this;
    var callback = callback || function() {};

    recordStream.once('open', fileOpened);
    recordStream.once('error', error);

    function fileOpened(fd) {
        self._recordStream = recordStream;
        self._filePosition = HEADER_SIZE;
        callback(null, self);
    }

    function error(err) {
        self._recordStream = recordStream;
        callback(err, null);
    }
};

writable.prototype.addRecord = function(key, data) {
    if (!this._recordStream) {
        throw new Error('cdb not opened.');
    }

    var hash = hashKey(key);
    var buffer = getBufferForRecord(key, data);
    var subtableIndex = lookupSubtable(hash);
    var entry = {hash: hash, position: this._filePosition};

    this._subtables[subtableIndex].push(entry);
    this._filePosition += buffer.length;

    return this._recordStream.write(buffer, 'buffer');
};

writable.prototype.close = function(callback) {
    var self = this;

    function closeRecordStream() {
        var deferred = defer();

        self._recordStream.on('finish', function allRecordsFlushed() {
            deferred.resolve();
        });

        self._recordStream.on('error', function streamError(err) {
            deferred.reject(err);
        });

        self._recordStream.end();

        return deferred.promise;
    }

    closeRecordStream().bind(this)
        .then(this._writeSubtables)
        .then(this._writeHeader)
        .nodeify(callback);
}

writable.prototype._writeSubtables = function() {
    var self = this;
    var offset = this._filePosition;
    var deferred = defer();

    this._subtableStream = fs.createWriteStream(this._file, {flags: 'a'});

    this._subtableStream.on('finish', function allSubtablesFlushed() {
        deferred.resolve();
    });

    this._subtableStream.on('error', function streamError(err) {
        deferred.reject();
    });

    for (var i = 0; i < TABLE_SIZE; i++) {
        var subtable = this._subtables[i];
        var buffer = getBufferForSubtable(subtable);

        this._subtableStream.write(buffer);
        this._header[i] = {position: offset, entries: subtable.length * 2};

        offset += buffer.length;
    }

    this._subtableStream.end();
    return deferred.promise;
};

writable.prototype._writeHeader = function(callback) {
    var writeFile = promise.promisify(fs.writeFile);
    var buffer = getBufferForHeader(this._header);

    return writeFile(this._file, buffer, {flag: 'r+'});
}

function defer() {
    var deferred = {};

    deferred.promise = new promise(function(resolve, reject) {
        deferred.resolve = resolve;
        deferred.reject = reject;
    });

    return deferred;
}

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
