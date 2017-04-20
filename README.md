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

* `options.highWaterMark` (default: 16) Buffer this many records before writing to Kinesis
* `options.logger` A [bunyan], [winston], or similar logger that has methods like `debug`, `error` and `info`
* `options.maxRetries` (default: 3) How many times to attempt a failed Kinesis put
* `options.retryTimeout` (default: 100) The initial retry delay in milliseconds
* `options.wait` (default: 500) How many milliseconds it should periodically flush

[bunyan]: https://www.npmjs.com/package/bunyan
[winston]: https://www.npmjs.com/package/winston


Readable stream
---------------

    const AWS = require('aws-sdk')
    const { KinesisReadable } = require('kinesis-streams')
    const client = AWS.Kinesis()
    const reader = new KinesisReadable(client, streamName, options)
    reader.pipe(yourDestinationHere)

### Options

* `options.interval: number` (default: `2000`) Milliseconds between each Kinesis read. Remember limit is 5 reads / second / shard
* `options.parser: Function` If this is set, this function is applied to the data. Example:

        const reader = new KinesisReadable(client, streamName, {parser: JSON.parse})
        reader.on('data', console.log(data.id))

* And any [getShardIterator] parameter

### Custom events

These events are emitted:

* `checkpoint` Inspired by [kinesis-readable], this fires when data is received so you can keep track of the last successful sequence read:

        reader.on('checkpoint', (sequenceNumber: string) => {})


  [Kafka quickstart]: http://kafka.apache.org/documentation.html#quickstart_consume
  [getShardIterator]: http://docs.aws.amazon.com/AWSJavaScriptSDK/latest/AWS/Kinesis.html#getShardIterator-property
  [kinesis-console-consumer]: https://github.com/crccheck/kinesis-console-consumer
  [kinesis-readable]: https://github.com/rclark/kinesis-readable
