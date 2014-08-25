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

var randomRecords = [];
for (var i = 0; i < 1000; i++) {
    var record = {key: getRandomString(5, 10), data: getRandomString(20, 30)}
    randomRecords.push(record);
}

vows.describe('cdb-random-test').addBatch({
    'An opened writable cdb': {
        topic: function() {
            (new  writable(randomFile)).open(this.callback);
        },

        'should not error': function(err, cdb) {
            assert.equal(err, null);
        },

        'should add records without exception': function(err, cdb) {
            for (var i = 0; i < randomRecords.length; i++) {
                var record = randomRecords[i];
                cdb.addRecord(record.key, record.data);
            }
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
                var count = randomRecords.length;
                var callback = this.callback;

                function checkRecord(index) {
                    return function(err, data) {
                        if (err || data != randomRecords[index].data) {
                            notFound++;
                        } else {
                            found++;
                        }

                        if (--count == 0) {
                            callback(notFound, found);
                        }
                    }
                }

                for (var i = 0; i < randomRecords.length; i++) {
                    cdb.getRecord(randomRecords[i].key, checkRecord(i));
                }
            },

            'should find all of them': function(notFound, found) {
                assert.equal(notFound, null);
                assert.equal(found, randomRecords.length);
            }
        },

        teardown: function(cdb) {
            cdb.close();
            fs.unlinkSync(randomFile);
        }
    }
}).export(module);
















