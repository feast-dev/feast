'use strict';

var relative = require('./source-relative');

/**
 * Codec for relative paths with respect to the source directory.
 * @type {{name:string, decode: function, encode: function, root: function}}
 */
module.exports = {
  name  : 'sourceRootRelative',
  decode: decode,
  encode: encode,
  root  : relative.root
};

/**
 * Decode the given uri.
 * Any path with leading slash is tested against source directory.
 * @this {{options: object}} A loader or compilation
 * @param {string} uri A source uri to decode
 * @returns {boolean|string} False where unmatched else the decoded path
 */
function decode(uri) {
  /* jshint validthis:true */
  return uri.startsWith('/') && relative.decode.call(this, uri.slice(1));
}

/**
 * Encode the given file path.
 * @this {{options: object}} A loader or compilation
 * @param {string} absolute An absolute file path to encode
 * @returns {string} A uri with leading slash
 */
function encode(absolute) {
  /* jshint validthis:true */
  return '/' + relative.encode.call(this, absolute);
}
