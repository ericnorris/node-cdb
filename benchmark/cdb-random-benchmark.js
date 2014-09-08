var fs = require('fs');
var writable = require('../src/').writable;
var readable = require('../src/').readable;
var randomFile = 'random';

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
    var randomKeys = {};
    var randomRecords = [];
    for (var i = 0; i < count; i++) {
        var key = getRandomString(5, 10);
        var data = getRandomString(20, 30);

        if (key in randomKeys) {
            randomKeys[key] += 1;
        } else {
            randomKeys[key] = 0;
        }

        randomRecords.push({key: key, data: data, offset: randomKeys[key]});
    }

    return randomRecords;
}

var writetest = {
    start: function(records, callback) {
        this.records = records;
        this.callback = callback;
        this.startTime = Date.now();
        this.cdb = new writable(randomFile);

        var self = this;
        this.cdb.open(function() {
            self.loop();
        });

        return this;
    },

    loop: function() {
        for (var i = 0; i < this.records.length; i++) {
            var record = this.records[i];
            var key = record.key;
            var data = record.key;

            this.cdb.addRecord(key, data);
        }

        var self = this;
        this.cdb.close(function() {
            self.end();
        });
    },

    end: function() {
        var endTime = Date.now();
        var duration = endTime - this.startTime;
        var seconds = duration / 1000;
        var perSecond = Math.floor((this.records.length / seconds) * 100) / 100;

        console.log('addRecord x' + this.records.length + ' in ' + seconds +
                    ' seconds (' + (perSecond) + ' per second).');

        if (this.callback) {
            this.callback();
        }
    }
};

var readtest = {
    start: function(records, callback) {
        this.records = records;
        this.callback = callback;
        this.startTime = Date.now();
        this.cdb = new readable(randomFile);
        this.index = 0;

        var self = this;
        this.cdb.open(function() {
            self.loop();
        });
    },

    loop: function() {
        var self = this;
        if (this.index < this.records.length) {
            var record = this.records[this.index];
            var key = record.key;
            var offset = record.offset;

            this.cdb.getRecord(key, offset, function() {
                self.index += 1;
                self.loop();
            });
        } else {
            this.cdb.close(function() {
                self.end();
            });
        }
    },

    end: function() {
        var endTime = Date.now();
        var duration = endTime - this.startTime;
        var seconds = duration / 1000;
        var perSecond = Math.floor((this.records.length / seconds) * 100) / 100;

        console.log('getRecord x' + this.records.length + ' in ' + seconds +
                    ' seconds (' + (perSecond) + ' per second).');

        if (this.callback) {
            this.callback();
        }
    }
};

function startTest() {
    writetest.start(records, writeTestFinished);
}

function writeTestFinished() {
    readtest.start(records, readTestFinished);
}

function readTestFinished() {
    fs.unlinkSync(randomFile);
}
var recordCount = parseInt(process.argv[2]) || 10000;
var records = generateRandomRecords(recordCount);
startTest();
