var vows = require('vows');
var assert = require('assert');
var fs = require('fs');

var writable = require('../src/writable-cdb');
var readable_cdb = require('../src/readable-cdb');

var tempFile = 'test/tmp';
vows.describe('cdb-test').addBatch({
    'A writable cdb': {
        topic: function() {
            try {
                fs.unlinkSync(tempFile);
            } catch (err) {}

            return new writable(tempFile);
        },

        'should not create a file on create': function(cdb) {
            assert.throws(function() {
                fs.statSync(tempFile);
            }, Error);
        },

        'should respond to addRecord': function(cdb) {
            assert.isFunction(cdb.addRecord);
        },

        'should throw an error if not opened': function(cdb) {
            assert.throws(cdb.addRecord, Error);
        },

        'when opened': {
            topic: function(cdb) {
                cdb.open(this.callback);
            },

            'should not error': function(err, cdb) {
                assert.isNull(err);
            },

            'should create a file': function(err, cdb) {
                assert.isObject(fs.statSync(tempFile));
            },

            'should add records without exception': function(cdb) {
                cdb.addRecord('meow', '0xdeadbeef');
                cdb.addRecord('abcd', 'test1');
                cdb.addRecord('efgh', 'test2');
                cdb.addRecord('ijkl', 'test3');
                cdb.addRecord('mnopqrs', 'test4');
            },

            'should close': {
                topic: function(cdb) {
                    cdb.close(this.callback);
                },

                'without error': function(err) {
                    assert.equal(err, null);
                },

                'and have a file with non-zero size': function(err) {
                    var stat = fs.statSync('test/tmp');
                    assert.isObject(stat);
                    assert.isTrue(stat.size != 0);
                }
            },

        },
    }
}).addBatch({
    'A readable cdb': {
        topic: function() {
            this.callback(null, new readable_cdb('test/tmp'));
        },

        'should find meow': {
            topic: function(cdb) {
                cdb.getRecord('meow', this.callback);
            },

            'without error': function(err, data) {
                assert.equal(err, null);
                assert.notEqual(data, null);
            },

            'and return 0xdeadbeef': function(err, data) {
                assert.equal(data, '0xdeadbeef');
            }
        },

        'when searching for a non-existing key': {
            topic: function(cdb) {
                cdb.getRecord('kitty cat', this.callback);
            },

            'should error': function(err, data) {
                assert.notEqual(err, null);
            },

            'and have a null result': function(err, data) {
                assert.equal(data, null);
            }
        },

        teardown: function(cdb) {
            fs.unlinkSync('test/tmp');
        }
    }
}).export(module);