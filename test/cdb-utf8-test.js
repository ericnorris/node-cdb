'use strict';

var vows     = require('vows'),
    assert   = require('assert'),
    fs       = require('fs'),
    writable = require('../src/writable-cdb'),
    readable = require('../src/readable-cdb'),
    tempFile = 'test/tmp';

try {
    fs.unlinkSync(tempFile);
} catch (err) {}

vows.describe('cdb-utf8-test').addBatch({
    'A writable cdb': {
        topic: function() {
            return new writable(tempFile);
        },

        'when opened': {
            topic: function(cdb) {
                cdb.open(this.callback);
            },

            'should write UTF8 characters': {
                topic: function(cdb) {
                    cdb.put('é', 'unicode test');
                    cdb.put('€', 'unicode test');
                    cdb.put('key', 'ᚠᛇᚻ');
                    cdb.put('대한민국', '안성기');

                    cdb.close(this.callback);
                },

                'and close successfully': function(err) {
                    assert.equal(err, null);
                },
            }
        }
    }
}).addBatch({
    'A readable cdb should find that': {
        topic: function() {
            (new readable(tempFile)).open(this.callback);
        },

        'é': {
            topic: function(cdb) {
                cdb.get('é', this.callback);
            },

            'exists': function(err, data) {
                assert.isNull(err);
                assert.isNotNull(data);
            },

            'has the right value': function(err, data) {
                assert.equal(data, 'unicode test');
            }
        },

        '€': {
            topic: function(cdb) {
                cdb.get('€', this.callback);
            },

            'exists': function(err, data) {
                assert.isNull(err);
                assert.isNotNull(data);
            },

            'has the right value': function(err, data) {
                assert.equal(data, 'unicode test');
            }
        },

        'key': {
            topic: function(cdb) {
                cdb.get('key', this.callback);
            },

            'exists': function(err, data) {
                assert.isNull(err);
                assert.isNotNull(data);
            },

            'has the right value': function(err, data) {
                assert.equal(data, 'ᚠᛇᚻ');
            }
        },

        '대한민국': {
            topic: function(cdb) {
                cdb.get('대한민국', this.callback);
            },

            'exists': function(err, data) {
                assert.isNull(err);
                assert.isNotNull(data);
            },

            'has the right value': function(err, data) {
                assert.equal(data, '안성기');
            }
        },

        teardown: function() {
            fs.unlinkSync(tempFile);
        }
    }

}).export(module);
