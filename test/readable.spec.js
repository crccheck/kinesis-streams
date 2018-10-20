// @flow weak
/* eslint-disable no-new */
const assert = require('assert')
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
    assert.strictEqual(actual, _expected, `Expected ${_expected} assertion${_expected !== 1 && 's' || ''}, but saw ${actual}`)
  }
  sinon.restore()
})

describe('KinesisReadable', () => {
  let client

  beforeEach(() => {
    client = { constructor: { __super__: { serviceIdentifier: 'kinesis' } } }
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
        assert.strictEqual(err.message, 'client is required')
      }
    })

    it('throws on missing streamName', () => {
      expect(1)
      try {
        // $FlowFixMe deliberate error
        new main.KinesisReadable(client, undefined)
        assert.ok(false)
      } catch (err) {
        assert.strictEqual(err.message, 'streamName is required')
      }
    })

    it('sets arguments', () => {
      expect(4)
      const reader = new main.KinesisReadable(client, 'stream-name', { foo: 'bar' })
      assert.ok(reader)
      assert.strictEqual(reader.streamName, 'stream-name')
      assert.strictEqual(reader.options.foo, 'bar')
      assert.strictEqual(reader.options.interval, 2000)
    })
  })

  describe('getShardId', () => {
    it('throws when there are no shards', () => {
      expect(1)
      client.describeStream = AWSPromise.resolves({ StreamDescription: { Shards: [] } })
      const reader = new main.KinesisReadable(client, 'stream-name')

      return reader.getShardId()
        .then((data) => {
          assert.ok(false)
        })
        .catch((err) => {
          assert.strictEqual(err.message, 'No shards!')
        })
    })

    it('gets shard id', () => {
      expect(1)
      client.describeStream = AWSPromise.resolves({ StreamDescription: { Shards: [{ ShardId: 'shard-id' }] } })
      const reader = new main.KinesisReadable(client, 'stream-name')

      return reader.getShardId()
        .then((data) => {
          assert.deepStrictEqual(data, ['shard-id'])
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
    it('gets shard iterator', async () => {
      expect(1)
      const reader = new main.KinesisReadable(client, 'stream-name')
      client.getShardIterator = AWSPromise.resolves({ ShardIterator: 'shard iterator' })

      const data = await reader.getShardIterator('shard-id')
      assert.strictEqual(data, 'shard iterator')
    })

    it('handles errors', async () => {
      expect(1)
      const reader = new main.KinesisReadable(client, 'stream-name')
      client.getShardIterator = AWSPromise.rejects(new Error('lol error'))

      try {
        await reader.getShardIterator('shard-id')
        assert.ok(false)
      } catch (err) {
        assert.strictEqual(err.message, 'lol error')
      }
    })
  })

  describe('_startKinesis', () => {
    it('passes shard iterator options ignoring extras', async () => {
      expect(4)
      client.describeStream = AWSPromise.resolves({ StreamDescription: { Shards: [{ ShardId: 'shard id' }] } })
      client.getShardIterator = AWSPromise.resolves({ ShardIterator: 'shard iterator' })
      sinon.stub(main.KinesisReadable.prototype, 'readShard')
      const options = {
        foo: 'bar',
        ShardIteratorType: 'SHIT',
        Timestamp: '0',
        StartingSequenceNumber: 'SSN',
      }
      const reader = new main.KinesisReadable(client, 'stream name', options)

      await reader._startKinesis()

      const params = client.getShardIterator.args[0][0]
      assert.strictEqual(params.ShardIteratorType, 'SHIT')
      assert.strictEqual(params.Timestamp, '0')
      assert.strictEqual(params.StartingSequenceNumber, 'SSN')
      assert.strictEqual(params.foo, undefined)
    })

    it('emits error when there is an error', async () => {
      expect(1)
      client.describeStream = AWSPromise.rejects(new Error('lol error'))
      const reader = new main.KinesisReadable(client, 'stream name', { foo: 'bar' })

      reader.once('error', (err) => {
        assert.strictEqual(err.message, 'lol error')
      })

      await reader._startKinesis()
    })
  })

  describe('readShard', () => {
    it('exits when shard is closed', async () => {
      expect(1)
      client.getRecords = AWSPromise.resolves({ Records: [] })
      const reader = new main.KinesisReadable(client, 'stream name', { foo: 'bar' })

      reader.once('error', () => {
        assert.ok(false, 'this should never run')
      })

      await reader.readShard('shard-iterator-2')
      assert.strictEqual(reader.iterators.size, 0)
    })

    it('continues to read open shard', async () => {
      expect(2)
      const record = {
        Data: '',
        SequenceNumber: 'seq-1',
      }
      const getRecords = sinon.stub()
      getRecords.onCall(0).returns({ promise: () => Promise.resolve({ Records: [record], NextShardIterator: 'shard-iterator-4' }) })
      getRecords.onCall(1).returns({ promise: () => Promise.resolve({ Records: [] }) })
      client.getRecords = getRecords
      const reader = new main.KinesisReadable(client, 'stream name', { interval: 0 })
      // $FlowFixMe
      reader._read = () => {}
      reader.once('error', () => {
        assert(false)
      })
      reader.once('checkpoint', (seq) => {
        assert.strictEqual(seq, 'seq-1')
      })

      await reader.readShard('shard-iterator-3')

      assert.strictEqual(getRecords.callCount, 2)
    })

    it('parses incoming records', async () => {
      expect(3)
      const record = {
        Data: '{"foo":"bar"}',
        SequenceNumber: 'seq-1',
      }
      client.getRecords = sinon.stub().returns({ promise: () => Promise.resolve({ Records: [record] }) })
      const reader = new main.KinesisReadable(client, 'stream name', {
        parser: JSON.parse,
      })
      // $FlowFixMe
      reader._read = () => {}

      await reader.readShard('shard-iterator-5')

      assert.ok(reader._readableState.objectMode)
      assert.strictEqual(reader._readableState.buffer.length, 1)
      assert.deepStrictEqual(reader._readableState.buffer.head.data, { foo: 'bar' })
    })

    it('emits errors', async () => {
      expect(1)
      client.getRecords = sinon.stub().returns({ promise: () => Promise.reject(new Error('AWS is down')) })
      const reader = new main.KinesisReadable(client, 'stream name', {})

      reader.once('error', (err) => {
        assert.strictEqual(err.message, 'AWS is down')
      })

      await reader.readShard('shard-iterator-5')
    })

    it('parser exceptions are passed through', async () => {
      expect(1)
      const record = {
        Data: '{"foo":"bar"}',
        SequenceNumber: 'seq-1',
      }
      client.getRecords = sinon.stub().returns({ promise: () => Promise.resolve({ Records: [record] }) })
      const reader = new main.KinesisReadable(client, 'stream name', {
        parser: () => { throw new Error('lolwut') },
      })

      try {
        await reader.readShard('shard-iterator-6')
        assert(0)
      } catch (err) {
        assert.strictEqual(err.message, 'lolwut')
      }
    })
  })

  describe('_read', () => {
    it('only calls _startKinesis once', () => {
      expect(1)
      const reader = new main.KinesisReadable(client, 'stream name', { foo: 'bar' })
      sinon.stub(reader, '_startKinesis').returns(Promise.resolve())

      reader._read()
      reader._read()

      assert.strictEqual(reader._startKinesis.callCount, 1)
    })
  })
})
