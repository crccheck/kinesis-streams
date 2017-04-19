/* eslint-disable no-new,no-unused-expressions */
const chai = require('chai')
const sinon = require('sinon')
const sinonChai = require('sinon-chai')
const streamArray = require('stream-array')
const _ = require('lodash')
const recordsFixture = require('./fixture/records')
const successResponseFixture = require('./fixture/success-response')
const failedResponseFixture = require('./fixture/failed-response')
const successAfterFailedResponseFixture = require('./fixture/success-after-failed-response')
const writeFixture = require('./fixture/write-fixture')
const { KinesisWritable } = require('../')

chai.use(sinonChai)

const expect = chai.expect

// Convenience wrapper around Promise to reduce test boilerplate
const AWSPromise = {
  resolves: (value) => {
    return sinon.stub().returns({
      promise: () => Promise.resolve(value),
    })
  },
  rejects: (value) => {
    return sinon.stub().returns({
      promise: () => Promise.reject(value),
    })
  },
}

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
    it('should throw error on missing client', function () {
      expect(function () {
        new KinesisWritable()
      }).to.Throw(Error, 'client is required')
    })

    it('should throw error on missing streamName', function () {
      expect(function () {
        new KinesisWritable({})
      }).to.Throw(Error, 'streamName is required')
    })

    it('should correct highWaterMark above 500', function () {
      const stream = new KinesisWritable({}, 'test', { highWaterMark: 501 })
      expect(stream.highWaterMark).to.equal(500)
    })
  })

  describe('getPartitionKey', function () {
    xit('should return a random partition key padded to 4 digits', function () {
      var kinesis = new KinesisWritable({}, 'foo')

      sandbox.stub(_, 'random').returns(10)

      expect(kinesis.getPartitionKey()).to.eq('0010')

      _.random.returns(1000)

      expect(kinesis.getPartitionKey()).to.eq('1000')
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
        expect(stream.getPartitionKey).to.have.returned('custom-partition')
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
        expect(err.message).to.equal('Fail')

        done()
      })

      stream.end({ foo: 'bar' })
    })

    it('should retry failed putRecords requests', function (done) {
      sandbox.stub(stream, 'getPartitionKey').returns('1234')

      client.putRecords = AWSPromise.rejects({retryable: true})
      client.putRecords.onCall(2).returns({promise: () => Promise.resolve(successResponseFixture)})

      stream.on('finish', () => {
        expect(client.putRecords).to.have.been.calledThrice

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
        expect(client.putRecords).to.have.been.calledTwice

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
