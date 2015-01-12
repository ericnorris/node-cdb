// === Setup ===
var writable = require('../src/writable-cdb'),
    readable = require('../src/readable-cdb'),
    fs = require('fs'),
    CDB_FILE = './benchmark.cdb',
    COUNT = 50000,
    records = [],
    keyCount = {},
    recordIndex = 0,
    writeBenchmark, writeCDB, key, data, offset, readCDB;

// Generate records
for (; recordIndex < COUNT; recordIndex++) {
    key = getRandomString(5, 10);
    data = getRandomString(20, 30);
    offset = keyCount[key] || 0;

    records.push({key: key, data: data, offset: offset});
    keyCount[key] = offset + 1;
}

// === Benchmark class ===
var benchmark = function(options) {
    this.name = options.name;
    this.count = options.count;
    this.setup = options.setup;
    this.fn = options.fn;
    this.teardown = options.teardown;
    this.onComplete = options.onComplete;
};

// Process an array of benchmarks sequentially
benchmark.process = function(benchmarkArray, callback) {
    var i = 0,
        length = benchmarkArray.length;

    function runBenchmark() {
        var benchmark = benchmarkArray[i++];

        if (i < length) {
            benchmark.onComplete = runBenchmark;
        } else {
            benchmark.onComplete = callback;
        }

        benchmark.run();
    }

    runBenchmark();
}

benchmark.prototype.run = function() {
    var name = this.name,
        count = this.count,
        fn = this.fn,
        teardown = this.teardown,
        onComplete = this.onComplete,
        i = 0,
        startTime, endTime, duration, seconds, perSecond;

    this.setup(start);

    function start() {
        startTime = Date.now();

        loop();
    }

    function loop() {
        if (i < count) {
            fn(i++, loop);
        } else {
            end();
        }
    }

    function end() {
        endTime = Date.now();
        duration = endTime - startTime;
        seconds = duration / 1000;
        perSecond = Math.floor((count / seconds) * 100) / 100;

        console.log(name + ' x' + count + ' in ' + seconds +
                    ' seconds (' + perSecond + ' per second).');

        teardown(onComplete);
    }
};

// === Benchmarks ===
writeBenchmark = new benchmark({
    'name': 'put()',
    'count': COUNT,

    'setup': function(callback) {
        writeCDB = new writable(CDB_FILE);
        writeCDB.open(callback);
    },

    'fn': function(iteration, callback) {
        var record = records[iteration];

        writeCDB.put(record.key, record.data, callback);
    },

    'teardown': function(callback) {
        writeCDB.close(callback);
    }
});

readBenchmark = new benchmark({
    'name': 'get()',
    'count': COUNT,

    'setup': function(callback) {
        readCDB = new readable(CDB_FILE);
        readCDB.open(callback);
    },

    'fn': function(iteration, callback) {
        var record = records[iteration];

        readCDB.get(record.key, record.offset, callback);
    },

    'teardown': function(callback) {
        readCDB.close(callback);
    }
});

// === Main ===
benchmark.process([writeBenchmark, readBenchmark], function() {
    fs.unlinkSync(CDB_FILE);
});

// === Util ===
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
