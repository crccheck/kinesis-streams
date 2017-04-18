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
