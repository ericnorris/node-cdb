'use strict';

var writable = require('./writable-cdb'),
    readable = require('./readable-cdb');

module.exports = {
    writable: writable,
    readable: readable
};
