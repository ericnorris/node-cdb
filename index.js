'use strict';

var writable = require('./src/writable-cdb'),
    readable = require('./src/readable-cdb');

module.exports = {
    writable: writable,
    readable: readable
};
