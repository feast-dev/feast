'use strict'

var hastUtilRaw = require('hast-util-raw')

module.exports = rehypeRaw

function rehypeRaw(options) {
  return transform
  function transform(tree, file) {
    return hastUtilRaw(tree, file, options)
  }
}
