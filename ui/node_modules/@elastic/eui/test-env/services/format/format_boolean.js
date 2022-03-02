"use strict";

Object.defineProperty(exports, "__esModule", {
  value: true
});
exports.formatBoolean = void 0;

var _predicate = require("../predicate");

/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */
var formatBoolean = function formatBoolean(value) {
  var _ref = arguments.length > 1 && arguments[1] !== undefined ? arguments[1] : {},
      _ref$yes = _ref.yes,
      yes = _ref$yes === void 0 ? 'Yes' : _ref$yes,
      _ref$no = _ref.no,
      no = _ref$no === void 0 ? 'No' : _ref$no,
      _ref$nil = _ref.nil,
      nil = _ref$nil === void 0 ? '' : _ref$nil;

  if ((0, _predicate.isNil)(value)) {
    return nil;
  }

  return value ? yes : no;
};

exports.formatBoolean = formatBoolean;