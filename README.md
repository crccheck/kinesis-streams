Kinesis Streams
===============

[![Build Status](https://travis-ci.org/crccheck/kinesis-streams.svg?branch=master)](https://travis-ci.org/crccheck/kinesis-streams)
[![npm version](https://badge.fury.io/js/kinesis-streams.svg)](https://badge.fury.io/js/kinesis-streams)
[![Test Coverage](https://codeclimate.com/github/crccheck/kinesis-streams/badges/coverage.svg)](https://codeclimate.com/github/crccheck/kinesis-streams/coverage)

There once was a [Kinesis readable stream][kinesis-console-consumer] without a
home, and a Kinesis writable stream without a home, so now they're roommates.


Usage
-----

    npm install [-g] kinesis-streams


Writeable stream
----------------

    const AWS = require('aws-sdk')
    const { KinesisWritable } = require('kinesis-streams')
    const client = AWS.Kinesis()
    const writable = new KinesisWritable(client, 'streamName', options)
    inputStream.pipe(writable)

### Options

* `options.logger` ([optional](#loggers)) [bunyan], [winston], or logger with `debug`, `error` and `info`
* `options.highWaterMark` (default: 16) Buffer this many records before writing to Kinesis
* `options.maxRetries` (default: 3) How many times to attempt a failed Kinesis put
* `options.retryTimeout` (default: 100) The initial retry delay in milliseconds
* `options.wait` (default: 500) How many milliseconds it should periodically flush


Readable stream
---------------

    const AWS = require('aws-sdk')
    const { KinesisReadable } = require('kinesis-streams')
    const client = AWS.Kinesis()
    const reader = new KinesisReadable(client, streamName, options)
    reader.pipe(yourDestinationHere)

### Options

* `options.logger` ([optional](#loggers)) [bunyan], [winston], or logger with `debug`, `error` and `info`
* `options.interval: number` (default: `2000`) Milliseconds between each Kinesis read. The AWS limit is 5 reads / second / shard
* `options.parser: Function` If this is set, this function is applied to the data. Example:

        const reader = new KinesisReadable(client, streamName, {parser: JSON.parse})
        reader.on('data', console.log(data.id))

* And any [getShardIterator] parameter

### Custom events

These events are emitted:

* `checkpoint` This fires when data is received so you can keep track of the last successful sequence read:

        reader.on('checkpoint', (sequenceNumber: string) => {})


Loggers
-------

`KinesisWritable` and `KinesisReadable` both take an optional `logger` option.
If this is omitted, the [debug] logger will be used instead. To see output, set
`DEBUG=kinesis-streams:*` in your environment.


Prior art
---------

The writable stream is based on the interface of [kinesis-write-stream]. The
`checkpoint` event in readable stream is based on [kinesis-readable]. The
readable stream was originally written as a proof of concept in
[kinesis-console-consumer].


License
-------

This package is licensed under Apache License 2.0, but the
`tests/writable.spec.js` and `test/fixture/*` are originally from
[kinesis-write-stream] MIT licensed from Espen Volden.


[bunyan]: https://github.com/trentm/node-bunyan
[debug]: https://github.com/visionmedia/debug
[getShardIterator]: http://docs.aws.amazon.com/AWSJavaScriptSDK/latest/AWS/Kinesis.html#getShardIterator-property
[Kafka quickstart]: http://kafka.apache.org/documentation.html#quickstart_consume
[kinesis-console-consumer]: https://github.com/crccheck/kinesis-console-consumer
[kinesis-readable]: https://github.com/rclark/kinesis-readable
[kinesis-write-stream]: https://github.com/voldern/kinesis-write-stream
[winston]: https://github.com/winstonjs/winston
