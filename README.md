# node-constant-db
A [cdb](http://cr.yp.to/cdb.html) implementation in node.js, supporting both read and write capabilities.

## Installation
`npm install constant-db`

## Changes from v1.0.0
* Renamed `getRecord()` to `get()`
* Renamed `putRecord()` to `put()`
* Added `getNext()`
* Dropped promise support
* Completely rewritten! `get()` calls should be much faster.

## Example
Writable cdb:
```javascript
var writable = require('constant-db').writable;

var writer = new writable('./cdbfile');
writer.open(function cdbOpened(err) {
    writer.put('meow', 'hello world');
    writer.close(function cdbClosed(err) {
        console.log('hooray!');
    });
});
```

Readable cdb:
```javascript
var readable = require('constant-db').readable;

var reader = new readable('./cdbfile');
reader.open(function cdbOpened(err) {
    reader.get('meow', function gotRecord(err, data) {
        console.log(data); // results in 'hello world!'
    });
    reader.close(function cdbClosed(err) {
        console.log('awesome!');
    });
});
```

## Documentation
### Readable cdb
To create a new readable instance:
`new require('constant-db').readable(file);`

`open(callback(err, cdb))`

Opens the file for reading, and immediately caches the header table for the cdb (2048 bytes).

`get(key, [offset], callback(err, data))`

Attempts to find the specified key, and calls the callback with an error (if not found) or the data for that key (if found). If an offset is specified, the cdb will return data for the *nth* record matching that key.

`getNext(callback(err, data))`

Continues the previous `get()` call, finding the next record under the key `get()` was called with. This should be slightly faster than calling `get()` with an offset.

`close(callback(err, cdb))`

Closes the file. No more records can be read after closing.

### Writable cdb
To create a new writable instance:
`new require('constant-cdb').writable(file);`

`open(callback(err, cdb))`

Opens the file for writing. This will overwrite any file that currently exists, or create a new one if necessary.

`put(key, data)`

Writes a record to the cdb.

`close(callback(err, cdb))`

Finalizes the cdb and closes the file. Calling `close()` is necessary to write out the header and subtables required for the cdb!

## Benchmark
`node benchmarks/cdb-random-benchmark.js`
