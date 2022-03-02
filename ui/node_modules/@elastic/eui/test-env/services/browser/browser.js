"use strict";

Object.defineProperty(exports, "__esModule", {
  value: true
});
exports.Browser = void 0;

/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */
// We don't normally use 'I' prefixes, this file is an exception
var BrowserImpl = {
  isEventSupported: function isEventSupported(name, element) {
    return "on".concat(name) in element;
  }
};
var Browser = Object.freeze(BrowserImpl);
exports.Browser = Browser;