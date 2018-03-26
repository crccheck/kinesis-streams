// @flow weak
/* eslint-disable no-new */
const assert = require('assert')
const errcode = require('err-code')
const sinon = require('sinon')

const { AWSPromise } = require('./')
const main = require('../lib/readable')

let _expected = 0

function expect (n/*: number */) {
  _expected = n
}

// Object.keys(assert)
const methods = ['deepEqual', 'deepStrictEqual', 'doesNotThrow', 'equal', 'fail', 'ifError', 'notDeepEqual', 'notDeepStrictEqual', 'notEqual', 'notStrictEqual', 'ok', 'strictEqual', 'throws']

beforeEach(function () {
  methods.forEach((x) => assert[x].restore && assert[x].restore()) // needed when running --watch
  methods.forEach((x) => sinon.spy(assert, x))
  this.actual = 0
  _expected = 0
})

afterEach(function () {
  // assert(_expected, 'Expected at least one assertion')
  if (_expected) {
    const actual = methods.reduce((a, b) => assert[b].callCount + a, 0)
    // eslint-disable-next-line no-mixed-operators
    assert.equal(actual, _expected, `Expected ${_expected} assertion${_expected !== 1 && 's' || ''}, but saw ${actual}`)
  }
  methods.forEach((x) => assert[x].restore())
})

describe('KinesisReadable', () => {
  let client
  let sandbox

  beforeEach(() => {
    client = {constructor: {__super__: {serviceIdentifier: 'kinesis'}}}
    sandbox = sinon.sandbox.create()
  })

  afterEach(() => {
    sandbox.restore()
  })

  describe('getStreams', () => {
    it('returns data from AWS', () => {
      expect(1)
      client.listStreams = AWSPromise.resolves('dat data')
      main.getStreams(client)
        .then((data) => {
          assert.strictEqual(data, 'dat data')
        })
    })

    it('handles errors', () => {
      expect(1)
      client.listStreams = AWSPromise.rejects(new Error('lol error'))
      return main.getStreams(client)
        .then((data) => {
          assert.strictEqual(true, false)
        })
        .catch((err) => {
          assert.strictEqual(err.message, 'lol error')
        })
    })
  })

  describe('constructor', () => {
    it('throws on missing client', () => {
      expect(1)
      try {
        // $FlowFixMe deliberate error
        new main.KinesisReadable(undefined, 'stream-name')
        assert.ok(false)
      } catch (err) {
        assert.equal(err.message, 'client is required')
      }
    })

    it('throws on missing streamName', () => {
      expect(1)
      try {
        // $FlowFixMe deliberate error
        new main.KinesisReadable(client, undefined)
        assert.ok(false)
      } catch (err) {
        assert.equal(err.message, 'streamName is required')
      }
    })

    it('sets arguments', () => {
      expect(4)
      const reader = new main.KinesisReadable(client, 'stream-name', {foo: 'bar'})
      assert.ok(reader)
      assert.equal(reader.streamName, 'stream-name')
      assert.equal(reader.options.foo, 'bar')
      assert.equal(reader.options.interval, 2000)
    })
  })

  describe('getShardId', () => {
    it('throws when there are no shards', () => {
      expect(1)
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
      expect(1)
      client.describeStream = AWSPromise.resolves({StreamDescription: {Shards: [{ShardId: 'shard-id'}]}})
      const reader = new main.KinesisReadable(client, 'stream-name')

      return reader.getShardId()
        .then((data) => {
          assert.deepEqual(data, ['shard-id'])
        })
    })

    it('handles errors', () => {
      expect(1)
      client.describeStream = AWSPromise.rejects(new Error('lol error'))
      const reader = new main.KinesisReadable(client, 'stream-name')

      return reader.getShardId()
        .then((data) => {
          assert.ok(false)
        })
        .catch((err) => {
          assert.strictEqual(err.message, 'lol error')
        })
    })
  })

  describe('getShardIterator', () => {
    it('gets shard iterator', () => {
      expect(1)
      const reader = new main.KinesisReadable(client, 'stream-name')
      client.getShardIterator = AWSPromise.resolves({ShardIterator: 'shard iterator'})

      return reader.getShardIterator('shard-id')
        .then((data) => {
          assert.strictEqual(data, 'shard iterator')
        })
    })

    it('handles errors', () => {
      expect(1)
      const reader = new main.KinesisReadable(client, 'stream-name')
      client.getShardIterator = AWSPromise.rejects(new Error('lol error'))

      return reader.getShardIterator('shard-id')
        .then((data) => {
          assert.ok(false)
        })
        .catch((err) => {
          assert.strictEqual(err.message, 'lol error')
        })
    })
  })

  describe('_startKinesis', () => {
    it('passes shard iterator options ignoring extras', () => {
      expect(4)
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
      expect(1)
      client.describeStream = AWSPromise.rejects(new Error('lol error'))
      const reader = new main.KinesisReadable(client, 'stream name', {foo: 'bar'})

      reader.once('error', (err) => {
        assert.equal(err.message, 'lol error')
      })

      return reader._startKinesis('stream name', {})
    })

    // $FlowFixMe
    xit('logs when there is an error', () => {
      client.describeStream = AWSPromise.rejects(new Error('lol error'))
      const reader = new main.KinesisReadable(client, 'stream name', {foo: 'bar'})

      return reader._startKinesis('stream name', {})
        .then(() => {
          assert.equal(console.log.args[0][0].message, 'lol error')
        })
    })
  })

  describe('readShard', () => {
    it('exits when shard is closed', () => {
      expect(1)
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
      expect(2)
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

    it('retries read failures', () => {
      expect(2)
      const record = {
        Data: '',
        SequenceNumber: 'seq-1',
      }
      const getRecords = sinon.stub()
      const awsError = errcode(new Error(), {retryable: true})
      getRecords.onCall(0).returns({promise: () => Promise.reject(awsError)})
      getRecords.onCall(1).returns({promise: () => Promise.resolve({Records: [record], NextShardIterator: 'shard-iterator-4'})})
      getRecords.onCall(2).returns({promise: () => Promise.resolve({Records: []})})
      client.getRecords = getRecords
      const reader = new main.KinesisReadable(client, 'stream name', {interval: 0})

      reader.once('error', (err) => {
        assert.ok(false, err)
      })
      reader.once('checkpoint', (seq) => {
        assert.equal(seq, 'seq-1')
      })

      return reader.readShard('shard-iterator-3')
        .then(() => {
          assert.strictEqual(getRecords.callCount, 3)
        })
    })

    it('parses incoming records', () => {
      expect(3)
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
      expect(1)
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
      expect(1)
      const reader = new main.KinesisReadable(client, 'stream name', {foo: 'bar'})
      sandbox.stub(reader, '_startKinesis').returns(Promise.resolve())

      reader._read()
      reader._read()

      assert.equal(reader._startKinesis.callCount, 1)
    })
  })
})
