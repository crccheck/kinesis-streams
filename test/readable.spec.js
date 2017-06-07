// @flow weak
/* eslint-disable no-new */
const assert = require('assert')
const sinon = require('sinon')

const { AWSPromise } = require('./')
const main = require('../lib/readable')

describe('KinesisReadable', () => {
  let client
  let sandbox

  beforeEach(() => {
    client = {}
    sandbox = sinon.sandbox.create()
  })

  afterEach(() => {
    sandbox.restore()
  })

  describe('getStreams', () => {
    it('returns data from AWS', () => {
      client.listStreams = AWSPromise.resolves('dat data')
      main.getStreams(client)
        .then((data) => {
          assert.strictEqual(data, 'dat data')
        })
    })

    it('handles errors', () => {
      client.listStreams = AWSPromise.rejects('lol error')
      return main.getStreams(client)
        .then((data) => {
          assert.strictEqual(true, false)
        })
        .catch((err) => {
          assert.strictEqual(err, 'lol error')
        })
    })
  })

  describe('constructor', () => {
    it('throws on missing client', () => {
      try {
        // $FlowFixMe deliberate error
        new main.KinesisReadable(undefined, 'stream-name')
        assert.ok(false)
      } catch (err) {
        assert.equal(err.message, 'client is required')
      }
    })

    it('throws on missing streamName', () => {
      try {
        // $FlowFixMe deliberate error
        new main.KinesisReadable(client, undefined)
        assert.ok(false)
      } catch (err) {
        assert.equal(err.message, 'streamName is required')
      }
    })

    it('sets arguments', () => {
      const reader = new main.KinesisReadable(client, 'stream-name', {foo: 'bar'})
      assert.ok(reader)
      assert.equal(reader.streamName, 'stream-name')
      assert.equal(reader.options.foo, 'bar')
      assert.equal(reader.options.interval, 2000)
    })
  })

  describe('getShardId', () => {
    it('throws when there are no shards', () => {
      client.describeStream = AWSPromise.resolves({StreamDescription: {Shards: []}})
      const reader = new main.KinesisReadable(client, 'stream-name')

      return reader.getShardId(client)
        .then((data) => {
          assert.ok(false)
        })
        .catch((err) => {
          assert.strictEqual(err.message, 'No shards!')
        })
    })

    it('gets shard id', () => {
      client.describeStream = AWSPromise.resolves({StreamDescription: {Shards: [{ShardId: 'shard-id'}]}})
      const reader = new main.KinesisReadable(client, 'stream-name')

      return reader.getShardId()
        .then((data) => {
          assert.deepEqual(data, ['shard-id'])
        })
    })

    it('handles errors', () => {
      client.describeStream = AWSPromise.rejects('lol error')
      const reader = new main.KinesisReadable(client, 'stream-name')

      return reader.getShardId()
        .then((data) => {
          assert.ok(false)
        })
        .catch((err) => {
          assert.strictEqual(err, 'lol error')
        })
    })
  })

  describe('getShardIterator', () => {
    it('gets shard iterator', () => {
      const reader = new main.KinesisReadable(client, 'stream-name')
      client.getShardIterator = AWSPromise.resolves({ShardIterator: 'shard iterator'})

      return reader.getShardIterator('shard-id')
        .then((data) => {
          assert.strictEqual(data, 'shard iterator')
        })
    })

    it('handles errors', () => {
      const reader = new main.KinesisReadable(client, 'stream-name')
      client.getShardIterator = AWSPromise.rejects('lol error')

      return reader.getShardIterator('shard-id')
        .then((data) => {
          assert.ok(false)
        })
        .catch((err) => {
          assert.strictEqual(err, 'lol error')
        })
    })
  })

  describe('_startKinesis', () => {
    it('passes shard iterator options ignoring extras', () => {
      client.describeStream = AWSPromise.resolves({StreamDescription: {Shards: [{ShardId: 'shard id'}]}})
      client.getShardIterator = AWSPromise.resolves({ShardIterator: 'shard iterator'})
      sandbox.stub(main.KinesisReadable.prototype, 'readShard')
      const options = {
        foo: 'bar',
        ShardIteratorType: 'SHIT',
        Timestamp: '0',
        StartingSequenceNumber: 'SSN',
      }
      const reader = new main.KinesisReadable(client, 'stream name', options)

      return reader._startKinesis().then(() => {
        const params = client.getShardIterator.args[0][0]
        assert.equal(params.ShardIteratorType, 'SHIT')
        assert.equal(params.Timestamp, '0')
        assert.equal(params.StartingSequenceNumber, 'SSN')
        assert.equal(params.foo, undefined)
      })
    })

    it('emits error when there is an error', () => {
      client.describeStream = AWSPromise.rejects('lol error')
      const reader = new main.KinesisReadable(client, 'stream name', {foo: 'bar'})

      reader.once('error', (err) => {
        assert.equal(err, 'lol error')
      })

      return reader._startKinesis('stream name', {})
    })

    // $FlowFixMe
    xit('logs when there is an error', () => {
      client.describeStream = AWSPromise.rejects('lol error')
      const reader = new main.KinesisReadable(client, 'stream name', {foo: 'bar'})

      return reader._startKinesis('stream name', {})
        .then(() => {
          assert.equal(console.log.args[0][0], 'lol error')
        })
    })
  })

  describe('readShard', () => {
    it('exits when there is an error preserving iterator', () => {
      client.getRecords = sinon.stub().returns({promise: () => Promise.reject(new Error('mock error'))})
      const reader = new main.KinesisReadable(client, 'stream name', {foo: 'bar'})

      reader.once('error', (err) => {
        assert.equal(err.message, 'mock error')
      })

      return reader.readShard('shard-iterator-1')
        .then(() => {
          assert(reader.iterators.has('shard-iterator-1'))
        })
    })

    it('exits when shard is closed', () => {
      client.getRecords = sinon.stub().returns({promise: () => Promise.resolve({Records: []})})
      const reader = new main.KinesisReadable(client, 'stream name', {foo: 'bar'})

      reader.once('error', () => {
        assert.ok(false, 'this should never run')
      })


      return reader.readShard('shard-iterator-2')
        .then(() => {
          assert.equal(reader.iterators.size, 0)
        })
    })

    it('continues to read open shard', () => {
      const record = {
        Data: '',
        SequenceNumber: 'seq-1',
      }
      const getRecords = sinon.stub()
      getRecords.onCall(0).returns({promise: () => Promise.resolve({Records: [record], NextShardIterator: 'shard-iterator-4'})})
      getRecords.onCall(1).returns({promise: () => Promise.resolve({Records: []})})
      client.getRecords = getRecords
      const reader = new main.KinesisReadable(client, 'stream name', {interval: 0})

      reader.once('error', () => {
        assert.ok(false, 'this should never run')
      })
      reader.once('checkpoint', (seq) => {
        assert.equal(seq, 'seq-1')
      })

      return reader.readShard('shard-iterator-3')
        .then(() => {
          assert.strictEqual(getRecords.callCount, 2)
          console.log('waited', getRecords.callCount)
        })
    })

    it('parses incoming records', () => {
      const record = {
        Data: '{"foo":"bar"}',
        SequenceNumber: 'seq-1',
      }
      client.getRecords = sinon.stub().returns({ promise: () => Promise.resolve({ Records: [record] }) })
      const reader = new main.KinesisReadable(client, 'stream name', {
        parser: JSON.parse,
      })

      return reader.readShard('shard-iterator-5')
        .then(() => {
          assert.ok(reader._readableState.objectMode)
          assert.equal(reader._readableState.buffer.length, 1)
          if (reader._readableState.buffer.head) {
            assert.deepEqual(reader._readableState.buffer.head.data, {foo: 'bar'})
          } else {
            // NODE4
            assert.deepEqual(reader._readableState.buffer[0], {foo: 'bar'})
          }
        })
    })

    it('parser exceptions are passed through', () => {
      const record = {
        Data: '{"foo":"bar"}',
        SequenceNumber: 'seq-1',
      }
      client.getRecords = sinon.stub().returns({ promise: () => Promise.resolve({ Records: [record] }) })
      const reader = new main.KinesisReadable(client, 'stream name', {
        parser: () => { throw new Error('lolwut') },
      })

      return reader.readShard('shard-iterator-6')
        .then(() => {
          assert(false, 'reader should have thrown')
        })
        .catch((err) => {
          assert.equal(err.message, 'lolwut')
        })
    })
  })

  describe('_read', () => {
    it('only calls _startKinesis once', () => {
      const reader = new main.KinesisReadable(client, 'stream name', {foo: 'bar'})
      sandbox.stub(reader, '_startKinesis').returns(Promise.resolve())

      reader._read()
      reader._read()

      assert.equal(reader._startKinesis.callCount, 1)
    })
  })
})
