'use strict';

var vows     = require('vows'),
    assert   = require('assert'),
    fs       = require('fs'),
    writable = require('../src/writable-cdb'),
    readable = require('../src/readable-cdb'),
    tempFile = 'test/tmp',
    fakeFile = 'test/doesntexist';

try {
    fs.unlinkSync(tempFile);
} catch (err) {}

vows.describe('cdb-test').addBatch({
    'A writable cdb': {
        topic: function() {
            return new writable(tempFile);
        },

        'should not create a file when instantiated': function() {
            assert.throws(function() {
                fs.statSync(tempFile);
            }, Error);
        },

        'should respond to put': function(cdb) {
            assert.isFunction(cdb.put);
        },

        'should throw an error if not opened': function(cdb) {
            assert.throws(cdb.put, Error);
        },

        'when opened': {
            topic: function(cdb) {
                cdb.open(this.callback);
            },

            'should not error': function(err, cdb) {
                assert.equal(err, null);
            },

            'should create a file': function(err, cdb) {
                assert.isObject(fs.statSync(tempFile));
            },

            'should add records without exception': function(cdb) {
                assert.doesNotThrow(function() {
                    cdb.put('meow', '0xdeadbeef');
                    cdb.put('meow', '0xbeefdead');
                    cdb.put('abcd', 'test1');
                    cdb.put('abcd', 'offset_test');
                    cdb.put('efgh', 'test2');
                    cdb.put('ijkl', 'test3');
                    cdb.put('mnopqrs', 'test4');
                }, Error);
            },

            'should close': {
                topic: function(cdb) {
                    cdb.close(this.callback);
                },

                'without error': function(err) {
                    assert.equal(err, null);
                },

                'and have a file with non-zero size': function(err) {
                    var stat = fs.statSync(tempFile);
                    assert.isObject(stat);
                    assert.isTrue(stat.size !== 0);
                }
            },

        },
    }
}).addBatch({
    'A readable cdb': {
        'for a non-existing file': {
            topic: function() {
                return new readable(fakeFile);
            },

            'when opened': {
                topic: function(cdb) {
                    cdb.open(this.callback);
                },

                'should error': function(err, cdb) {
                    assert.notEqual(err, null);
                }
            }
        },

        'for an existing file': {
            topic: function() {
                return new readable(tempFile);
            },

            'when opened': {
                topic: function(cdb) {
                    cdb.open(this.callback);
                },

                'should not error': function(err, cdb) {
                    assert.equal(err, null);
                },

                'should find an existing key': {
                    topic: function(cdb) {
                        cdb.get('meow', this.callback);
                    },

                    'without error': function(err, data) {
                        assert.equal(err, null);
                    },

                    'and return the right data': function(err, data) {
                        assert.equal(data, '0xdeadbeef');
                    },

                    'with a duplicate': {
                        topic: function(_, cdb) {
                            cdb.getNext(this.callback);
                        },

                        'that is found via getNext()': function(err, data) {
                            assert.equal(err, null);
                            assert.equal(data, '0xbeefdead');
                        }
                    }
                },

                'should find an existing key at an offset': {
                    topic: function(cdb) {
                        cdb.get('abcd', 1, this.callback);
                    },

                    'without error': function(err, data) {
                        assert.equal(err, null);
                    },

                    'and return the right data': function(err, data) {
                        assert.equal(data, 'offset_test');
                    }
                },

                'should not find a missing key': {
                    topic: function(cdb) {
                        cdb.get('kitty cat', this.callback);
                    },

                    'and should not error': function(err, data) {
                        assert.equal(err, null);
                    },

                    'and should have a null result': function(err, data) {
                        assert.equal(data, null);
                    }
                },
            },

            teardown: function(cdb) {
                cdb.close();
            }
        },

        'for an open existing file': {
            topic: function() {
                (new readable(tempFile)).open(this.callback);
            },

            'when closed': {
                topic: function(cdb) {
                    cdb.close(this.callback);
                },

                'should not error': function(err, _) {
                    assert.equal(err, null);
                }
            }
        },

        teardown: function() {
            fs.unlinkSync(tempFile);
        }
    }
}).addBatch({
    'The CDB package\'s module.exports': {
        topic: function() {
            return require('../');
        },

        'should have a writable CDB': function(index) {
            assert.isFunction(index.writable);
        },

        'should have a readable CDB': function(index) {
            assert.isFunction(index.readable);
        }
    }
}).export(module);
