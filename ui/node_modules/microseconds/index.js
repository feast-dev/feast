'use strict'

const now = require('./now')
const parse = require('./parse')

const since = (nano) => now() - nano

exports.now = now
exports.since = since
exports.parse = parse
