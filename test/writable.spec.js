// @flow weak
/* eslint-disable no-new,no-unused-expressions */
const assert = require('assert')
const chai = require('chai')
const sinon = require('sinon')
const sinonChai = require('sinon-chai')
const streamArray = require('stream-array')
const _ = require('lodash')

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
  let sandbox
  let stream

  beforeEach(function () {
    sandbox = sinon.sandbox.create()

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
    sandbox.restore()
  })

  describe('constructor', function () {
    it('throws on missing client', () => {
      try {
        new KinesisWritable()
        assert.ok(false)
      } catch (err) {
        assert.equal(err.message, 'client is required')
      }
    })

    it('throws on missing streamName', () => {
      try {
        new KinesisWritable({})
        assert.ok(false)
      } catch (err) {
        assert.equal(err.message, 'streamName is required')
      }
    })

    it('should correct highWaterMark above 500', function () {
      const stream = new KinesisWritable({}, 'test', { highWaterMark: 501 })
      expect(stream.highWaterMark).to.equal(500)
    })
  })

  describe('getPartitionKey', function () {
    // $FlowFixMe
    xit('should return a random partition key padded to 4 digits', function () {
      var kinesis = new KinesisWritable({}, 'foo')

      sandbox.stub(_, 'random').returns(10)

      assert.equal(kinesis.getPartitionKey(), '0010')

      _.random.returns(1000)

      assert.equal(kinesis.getPartitionKey(), '0010')
    })

    it('should be called with the current record being added', function (done) {
      client.putRecords = AWSPromise.resolves(successResponseFixture)
      sandbox.stub(stream, 'getPartitionKey').returns('1234')

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

      sandbox.spy(stream, 'getPartitionKey')

      stream.on('finish', () => {
        assert.equal(stream.getPartitionKey(), 'custom-partition')
        done()
      })

      streamArray(recordsFixture).pipe(stream)
    })
  })

  describe('_write', function () {
    it('should write to Kinesis when stream is closed', function (done) {
      client.putRecords = AWSPromise.resolves(successResponseFixture)
      sandbox.stub(stream, 'getPartitionKey').returns('1234')

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

    it('should emit error on Kinesis error', function (done) {
      client.putRecords = AWSPromise.rejects('Fail')

      stream.on('error', (err) => {
        assert.strictEqual(err.message, 'Fail')
        assert.strictEqual(stream.queue.length, 1)

        done()
      })

      stream.end({ foo: 'bar' })
    })

    it('should retry failed putRecords requests', function (done) {
      sandbox.stub(stream, 'getPartitionKey').returns('1234')

      client.putRecords = AWSPromise.rejects({retryable: true})
      client.putRecords.onCall(2).returns({promise: () => Promise.resolve(successResponseFixture)})

      stream.on('finish', () => {
        assert.equal(client.putRecords.callCount, 3)

        expect(client.putRecords.secondCall).to.have.been.calledWith({
          Records: writeFixture,
          StreamName: 'streamName',
        })

        done()
      })

      streamArray(recordsFixture).pipe(stream)
    })

    it('should retry failed records', function (done) {
      let putRecordsCount = 0
      sandbox.stub(stream, 'getPartitionKey').returns('1234')

      client.putRecords = AWSPromise.resolves(failedResponseFixture)
      client.putRecords.onCall(1).returns({promise: () => Promise.resolve(successAfterFailedResponseFixture)})
      stream.once('error', () => {
        expect(stream.queue).to.deep.equal([ { someKey: 2 }, { someKey: 4 } ])
      })
      stream.on('kinesis.putRecords', () => putRecordsCount++)
      stream.on('finish', () => {
        assert.equal(client.putRecords.callCount, 2)

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
