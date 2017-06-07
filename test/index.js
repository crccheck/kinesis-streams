// Convenience wrapper around Promise to reduce test boilerplate
const sinon = require('sinon')

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


exports.AWSPromise = AWSPromise
