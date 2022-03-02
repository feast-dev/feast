'use strict';

var path = require('path'),
    fs   = require('fs');

/**
 * Codec for relative paths with respect to the source directory.
 * @type {{name:string, decode: function, encode: function, root: function}}
 */
module.exports = {
  name  : 'sourceRelative',
  decode: decode,
  encode: encode,
  root  : root
};

/**
 * Decode the given uri.
 * Any path without leading slash is tested against source directory.
 * @this {{options: object}} A loader or compilation
 * @param {string} uri A source uri to decode
 * @returns {boolean|string} False where unmatched else the decoded path
 */
function decode(uri) {
  /* jshint validthis:true */
  var base    = !uri.startsWith('/') && this.context,
      absFile = !!base && path.normalize(path.join(base, uri)),
      isValid = !!absFile && fs.existsSync(absFile) && fs.statSync(absFile).isFile();
  return isValid && absFile;
}

/**
 * Encode the given file path.
 * @this {{options: object}} A loader or compilation
 * @param {string} absolute An absolute file path to encode
 * @returns {string} A uri without leading slash
 */
function encode(absolute) {
  /* jshint validthis:true */
  return path.relative(this.context, absolute);
}

/**
 * The source-map root where relevant.
 * @this {{options: object}} A loader or compilation
 * @returns {string|undefined} The source-map root applicable to any encoded uri
 */
function root() {
  /* jshint validthis:true */
  return this.context;
}
