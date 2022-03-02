"use strict";

var _interopRequireDefault = require("@babel/runtime/helpers/interopRequireDefault");

Object.defineProperty(exports, "__esModule", {
  value: true
});
exports.isDomainSecure = void 0;

var _slicedToArray2 = _interopRequireDefault(require("@babel/runtime/helpers/slicedToArray"));

/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */
var isElasticDomain = /(https?:\/\/(.+?\.)?elastic\.co((\/|\?)[A-Za-z0-9\-\._~:\/\?#\[\]@!$&'\(\)\*\+,;\=]*)?)/g; // In order for the domain to be secure the regex
// has to match _and_ the lengths of the match must
// be _exact_ since URL's can have other URL's as
// path or query params!

var isDomainSecure = function isDomainSecure() {
  var url = arguments.length > 0 && arguments[0] !== undefined ? arguments[0] : '';
  var matches = url.match(isElasticDomain);

  if (!matches) {
    return false;
  }

  var _matches = (0, _slicedToArray2.default)(matches, 1),
      match = _matches[0];

  return match.length === url.length;
};

exports.isDomainSecure = isDomainSecure;