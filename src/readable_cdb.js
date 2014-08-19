var fs = require('fs');

var HASH_START = 5381;
var HEADER_SIZE = 2048;
var TABLE_SIZE = 256;
var INT_SIZE = 4;

var readable_cdb = module.exports = function(file) {
    this.fd = fs.openSync(file, 'r');

    this._readHeader();
};

readable_cdb.prototype._readHeader = function() {
    var header = new Array(TABLE_SIZE);
    var buffer = new Buffer(HEADER_SIZE);
    var offset = 0;

    fs.readSync(this.fd, buffer, 0, HEADER_SIZE);
    for (var i = 0; i < TABLE_SIZE; i++) {
        var position = buffer.readUInt32LE(offset);
        var entries = buffer.readUInt32LE(offset + INT_SIZE);

        header[i] = {position: position, entries: entries};
        offset += (2 * INT_SIZE);
    }
};

