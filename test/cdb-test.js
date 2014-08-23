var vows = require('vows');
var assert = require('assert');
var fs = require('fs');

var writable_cdb = require('../src/writable_cdb');
var readable_cdb = require('../src/readable_cdb');

vows.describe('cdb-test').addBatch({
    'A writable cdb': {
        topic: function() {
            this.callback(null, new writable_cdb('test/tmp'));
        },

        'should create a file named tmp': function(cdb) {
            assert.isObject(fs.statSync('test/tmp'));
        },
        'should respond to addRecord': function(cdb) {
            assert.isFunction(cdb.addRecord);
        },
        'should add records without exception': function(cdb) {
            cdb.addRecord('meow', '0xdeadbeef');
            cdb.addRecord('abcd', 'test1');
            cdb.addRecord('efgh', 'test2');
            cdb.addRecord('ijkl', 'test3');
            cdb.addRecord('mnopqrs', 'test4');
        },
        'should finalize': {
            topic: function(cdb) {
                cdb.finalizeDB(this.callback);
            },

            'without error': function(err) {
                assert.equal(err, null);
            },

            'and have a file named tmp with non-zero size': function(err) {
                var stat = fs.statSync('test/tmp');
                assert.isObject(stat);
                assert.isTrue(stat.size != 0);
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