Kinesis Streams
===============

[![Build Status](https://travis-ci.org/crccheck/kinesis-streams.svg?branch=master)](https://travis-ci.org/crccheck/kinesis-streams)
[![npm version](https://badge.fury.io/js/kinesis-streams.svg)](https://badge.fury.io/js/kinesis-streams)
[![Coverage Status](https://coveralls.io/repos/github/crccheck/kinesis-streams/badge.svg?branch=master)](https://coveralls.io/github/crccheck/kinesis-streams?branch=master)

There once was a Kinesis readable stream without a home, and a Kinesis writable
stream without a home, so now they're roommates.

Usage
-----

    npm install [-g] kinesis-streams


Readable stream
---------------

    const AWS = require('aws-sdk')
    const { KinesisStreamReader } = require('kinesis-streams')
    const client = AWS.Kinesis()
    const reader = new KinesisStreamReader(client, streamName, options)
    reader.pipe(yourDestinationHere)

### Options

* `options.interval: number` (default: `2000`) Milliseconds between each Kinesis read. Remember limit is 5 reads / second / shard
* `options.parser: Function` If this is set, this function is applied to the data. Example:

        const client = AWS.Kinesis()
        const reader = new KinesisStreamReader(client, streamName, {parser: JSON.parse})
        reader.on('data', console.log(data.id))

* And any [getShardIterator] parameter

### Custom events

These events are emitted:

* `checkpoint` Inspired by [kinesis-readable], this fires when data is received so you can keep track of the last successful sequence read:

        reader.on('checkpoint', (sequenceNumber: string) => {})


  [Kafka quickstart]: http://kafka.apache.org/documentation.html#quickstart_consume
  [getShardIterator]: http://docs.aws.amazon.com/AWSJavaScriptSDK/latest/AWS/Kinesis.html#getShardIterator-property
  [kinesis-readable]: https://github.com/rclark/kinesis-readable
