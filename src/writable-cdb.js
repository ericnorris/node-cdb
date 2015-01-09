var events = require('events');
var fs = require('fs');
var util = require('util');

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

    hashtable.push({hash: hash, position: this._filePosition});
    this._hashtables[hashtableIndex] = hashtable;

    this._filePosition += recordHeader.length + key.length + data.length;
};

writable.prototype.close = function(callback) {
    var self = this,
        callback = callback || function() {};

    this._recordStream.end();

    this._recordStream.on('finish', openStreamForHashtable);

    function openStreamForHashtable() {
        self._hashtableStream = fs.createWriteStream(self._file,
            {start: self._filePosition, flags: 'r+'});

        self._hashtableStream.once('open', writeHashtables);
        self._hashtableStream.once('error', error);
    }

    function writeHashtables() {
        var i = 0,
            length = self._hashtables.length,
            hashtable, buffer;

        for (; i < length; i++) {
            hashtable = self._hashtables[i] || [];
            buffer = getBufferForHashtable(hashtable);

            self._hashtableStream.write(buffer);
            self._header[i] = {
                position: self._filePosition,
                slots: hashtable.length * 2 // due to a 0.5 load factor
            };

            self._filePosition += buffer.length;
        }

        self._hashtableStream.end();
        self._hashtableStream.on('finish', writeHeader);
    }

    function writeHeader() {
        var buffer = getBufferForHeader(self._header);

        fs.writeFile(self._file, buffer, {flag: 'r+'}, finished);
    }

    function finished() {
        self.emit('finish');
        callback();
    }

    function error(err) {
        self.emit('error', err);
        callback(err);
    }
};

// === Util ===

// Hashing implementation
function hashKey(key) {
    var hash = 5381,
        i = 0,
        length = key.length;

    for (; i < length; i++) {
        hash = ((((hash << 5) >>> 0) + hash) ^ key.charCodeAt(i)) >>> 0;
    }

    return hash;
}


/*
 * Returns an allocated buffer containing the binary representation of a CDB
 * hashtable. Hashtables are linearly probed, and use a load factor of 0.5, so
 * the buffer will have 2n slots for n entries.
 */
function getBufferForHashtable(hashtable) {
    var slotCount = hashtable.length * 2,
        buffer = new Buffer(slotCount * 8), // 8 bytes per (hash, position) pair
        i = 0,
        length = hashtable.length,
        hash, position, slot, bufferPosition;

    // zero out the buffer
    buffer.fill(0);

    for (; i < length; i++) {
        hash = hashtable[i].hash;
        position = hashtable[i].position;

        slot = (hash >>> 8) % slotCount;
        bufferPosition = slot * 8;

        while (buffer.readUInt32LE(bufferPosition) != 0) {
            // this slot is occupied
            slot = (slot + 1) % slotCount;
            bufferPosition = slot * 8;
        }

        buffer.writeUInt32LE(hash, bufferPosition);
        buffer.writeUInt32LE(position, bufferPosition + 4); // 4 bytes per int
    }

    return buffer;
}

/*
 * Returns an allocated buffer containing the binary representation of a CDB
 * header. The header contains 255 (count, position) pairs representing the
 * number of slots and position of the hashtables.
 */
function getBufferForHeader(headerTable) {
    var buffer = new Buffer(HEADER_SIZE),
        bufferPosition = 0,
        i = 0,
        position, slots;

    for (; i < TABLE_SIZE; i++) {
        position = headerTable[i].position;
        slots = headerTable[i].slots;

        buffer.writeUInt32LE(position, bufferPosition);
        buffer.writeUInt32LE(slots, bufferPosition + 4); // 4 bytes per int
        bufferPosition += 8;
    }

    return buffer;
}
