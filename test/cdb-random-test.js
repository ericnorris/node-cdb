var vows   = require('vows');
var assert = require('assert');
var fs     = require('fs');

var writable = require('../src/writable-cdb');
var readable = require('../src/readable-cdb');

var randomFile = 'test/random';
try {
    fs.unlinkSync(randomFile);
} catch (err) {}

function getRandomInt(min, max) {
  return Math.floor(Math.random() * (max - min)) + min;
}

function getRandomString(minLength, maxLength) {
    var length = getRandomInt(minLength, maxLength);
    var stringArray = [];
    for (var i = 0; i < length; i++) {
        stringArray.push(String.fromCharCode(getRandomInt(97, 122)));
    }

    return stringArray.join('');
}

function generateRandomRecords(count) {
    var randomRecords = {};
    for (var i = 0; i < count; i++) {
        var key = getRandomString(5, 10);
        var data = getRandomString(20, 30);

        if (key in randomRecords) {
            randomRecords[key].push(data);
        } else {
            randomRecords[key] = [data];
        }
    }

    return randomRecords;
}

function iterateOverRecords(records, callback) {
    var keys = Object.keys(records);
    for (var i = 0; i < keys.length; i++) {
        var key = keys[i];
        var data = records[key];
        for (var j = 0; j < data.length; j++) {
            callback(key, j, data[j]);
        }
    }
}

var recordCount = 1000;
var randomRecords = generateRandomRecords(recordCount);

vows.describe('cdb-random-test').addBatch({
    'An opened writable cdb': {
        topic: function() {
            (new  writable(randomFile)).open(this.callback);
        },

        'should not error': function(err, cdb) {
            assert.equal(err, null);
        },

        'should add records without exception': function(err, cdb) {
            assert.doesNotThrow(function() {
                iterateOverRecords(randomRecords, function(key, offset, data) {
                    cdb.addRecord(key, data);
                });
            }, Error);
        },

        'should close': {
            topic: function(cdb) {
                cdb.close(this.callback)
            },

            'without error': function(err, cdb) {
                assert.equal(err, null);
            }
        }
    }
}).addBatch({
    'An opened readable cdb': {
        topic: function() {
            (new readable(randomFile)).open(this.callback);
        },

        'should not error': function(err, cdb) {
            assert.equal(err, null);
        },

        'when searching for existing keys': {
            topic: function(cdb) {
                var found = 0;
                var notFound = 0;
                var count = recordCount;
                var callback = this.callback;

                function checkRecord(expected) {
                    return function(err, data) {
                        if (err || data != expected) {
                            notFound++;
                        } else {
                            found++;
                        }

                        if (--count == 0) {
                            callback(notFound, found);
                        }
                    }
                }

                iterateOverRecords(randomRecords, function(key, offset, data) {
                    cdb.getRecord(key, offset, checkRecord(data));
                });
            },

            'should find all of them': function(notFound, found) {
                assert.equal(notFound, null);
                assert.equal(found, recordCount);
            }
        },

        teardown: function(cdb) {
            cdb.close();
            fs.unlinkSync(randomFile);
        }
    }
}).export(module);
















