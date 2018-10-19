// @flow weak
/* eslint-disable no-new,no-unused-expressions */
const assert = require('assert')
const chai = require('chai')
const sinon = require('sinon')
const sinonChai = require('sinon-chai')
const streamArray = require('stream-array')

const AWSPromise = require('./').AWSPromise
const recordsFixture = require('./fixture/records')
const successResponseFixture = require('./fixture/success-response')
const failedResponseFixture = require('./fixture/failed-response')
const successAfterFailedResponseFixture = require('./fixture/success-after-failed-response')
const writeFixture = require('./fixture/write-fixture')
const { KinesisWritable } = require('../')

chai.use(sinonChai)

const expect = chai.expect


describe('KinesisWritable', function () {
  let client
  let stream

  beforeEach(function () {
    client = {
      putRecords: sinon.stub(),
      constructor: {
        __super__: {
          serviceIdentifier: 'TestClient',
        },
      },
    }

    stream = new KinesisWritable(client, 'streamName', {
      highWaterMark: 6,
    })
  })

  afterEach(function () {
    sinon.restore()
  })

  describe('constructor', function () {
    it('throws on missing client', () => {
      try {
        new KinesisWritable()
        assert.ok(false)
      } catch (err) {
        assert.strictEqual(err.message, 'client is required')
      }
    })

    it('throws on missing streamName', () => {
      try {
        new KinesisWritable({})
        assert.ok(false)
      } catch (err) {
        assert.strictEqual(err.message, 'streamName is required')
      }
    })

    it('should correct highWaterMark above 500', function () {
      const stream = new KinesisWritable({}, 'test', { highWaterMark: 501 })
      expect(stream.collectionMaxCount).to.equal(500)
    })
  })

  describe('getPartitionKey', function () {
    it('should be called with the current record being added', function (done) {
      client.putRecords = AWSPromise.resolves(successResponseFixture)
      sinon.stub(stream, 'getPartitionKey').returns('1234')

      stream.on('finish', () => {
        expect(stream.getPartitionKey).to.have.been.calledWith(recordsFixture[0])
        done()
      })

      streamArray([recordsFixture[0]]).pipe(stream)
    })

    it('should use custom getPartitionKey if defined', function (done) {
      client.putRecords = AWSPromise.resolves(successResponseFixture)

      stream.getPartitionKey = function () {
        return 'custom-partition'
      }

      sinon.spy(stream, 'getPartitionKey')

      stream.on('finish', () => {
        assert.strictEqual(stream.getPartitionKey(), 'custom-partition')
        done()
      })

      streamArray(recordsFixture).pipe(stream)
    })
  })

  describe('getQueueSpliceCount', function () {
    it('limits small records to collectionMaxCount', () => {
      const stream = new KinesisWritable(client, 'streamName', {
        highWaterMark: 5,
      })
      const record = { Data: '' }
      stream.queue = [record, record, record, record, record, record]
      assert.strictEqual(stream.getQueueSpliceCount(), 5)
    })

    it('limits big records to collectionMaxSize', () => {
      const stream = new KinesisWritable(client, 'streamName', {
        highWaterMark: 500,
      })
      const record = { Data: ' ' }
      stream.queue = [record, record, record, record, record, record]
      stream.collectionMaxSize = 5
      assert.strictEqual(stream.getQueueSpliceCount(), 5)
    })
  })

  describe('_write', function () {
    it('should write to Kinesis when stream is closed', function (done) {
      client.putRecords = AWSPromise.resolves(successResponseFixture)
      sinon.stub(stream, 'getPartitionKey').returns('1234')

      stream.on('finish', () => {
        expect(client.putRecords).to.have.been.calledOnce

        expect(client.putRecords).to.have.been.calledWith({
          Records: writeFixture,
          StreamName: 'streamName',
        })

        done()
      })

      streamArray(recordsFixture).pipe(stream)
    })

    it('should do nothing if there is nothing in the queue when the stream is closed', function (done) {
      client.putRecords = AWSPromise.resolves(successResponseFixture)

      stream.on('finish', () => {
        expect(client.putRecords).to.have.been.calledOnce

        done()
      })

      for (var i = 0; i < 6; i++) {
        stream.write(recordsFixture)
      }

      stream.end()
    })

    it('should buffer records up to highWaterMark', function (done) {
      client.putRecords = AWSPromise.resolves(successResponseFixture)

      for (var i = 0; i < 4; i++) {
        stream.write(recordsFixture[0])
      }

      stream.write(recordsFixture[0], () => {
        expect(client.putRecords).to.not.have.been.called

        stream.write(recordsFixture[0], () => {
          expect(client.putRecords).to.have.been.calledOnce

          done()
        })
      })
    })

    it('should retry failed records', function (done) {
      let putRecordsCount = 0
      sinon.stub(stream, 'getPartitionKey').returns('1234')

      client.putRecords = AWSPromise.resolves(failedResponseFixture)
      client.putRecords.onCall(1).returns({ promise: () => Promise.resolve(successAfterFailedResponseFixture) })
      stream.once('error', () => {
        expect(stream.queue).to.deep.equal([ { someKey: 2 }, { someKey: 4 } ])
      })
      stream.on('kinesis.putRecords', () => putRecordsCount++)
      stream.on('finish', () => {
        assert.strictEqual(client.putRecords.callCount, 2)

        expect(client.putRecords.secondCall).to.have.been.calledWith({
          Records: [{ Data: '{"someKey":2}', PartitionKey: '1234' }, { Data: '{"someKey":4}', PartitionKey: '1234' }],
          StreamName: 'streamName',
        })

        expect(putRecordsCount).to.equal(2)

        done()
      })

      streamArray(recordsFixture).pipe(stream)
    })
  })
})
