var util = module.exports = {};

util.HASH_START  = 5381;
util.HEADER_SIZE = 2048;
util.TABLE_SIZE  = 256;
util.INT_SIZE    = 4;
util.ENTRY_SIZE  = 2 * util.INT_SIZE;

var HASH_START = util.HASH_START;

util.hashKey = function hashKey(key) {
    var hash = HASH_START;
    for (var i = 0, length = key.length; i < length; i++) {
        hash = ((((hash << 5) >>> 0) + hash) ^ key.charCodeAt(i)) >>> 0;
    }
    return hash;
}

util.lookupSubtable = function lookupSubtable(hash) {
    return hash & 255;
}