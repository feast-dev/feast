"use strict";

var _interopRequireDefault = require("@babel/runtime/helpers/interopRequireDefault");

Object.defineProperty(exports, "__esModule", {
  value: true
});
exports.validateHref = validateHref;

var _urlParse = _interopRequireDefault(require("url-parse"));

/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */
function validateHref(href) {
  // check href and treat it as invalid if it uses the javascript: protocol
  var parts = new _urlParse.default(href); // eslint-disable-next-line no-script-url

  return parts.protocol !== 'javascript:';
}