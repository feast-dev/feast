"use strict";

var _interopRequireDefault = require("@babel/runtime/helpers/interopRequireDefault");

Object.defineProperty(exports, "__esModule", {
  value: true
});
exports.getDateMode = getDateMode;
exports.toAbsoluteString = toAbsoluteString;
exports.toRelativeString = toRelativeString;
exports.INVALID_DATE = exports.DATE_MODES = void 0;

var _datemath = _interopRequireDefault(require("@elastic/datemath"));

var _relative_utils = require("./relative_utils");

var _moment = _interopRequireDefault(require("moment"));

/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */
var DATE_MODES = {
  ABSOLUTE: 'absolute',
  RELATIVE: 'relative',
  NOW: 'now'
};
exports.DATE_MODES = DATE_MODES;
var INVALID_DATE = 'invalid_date';
exports.INVALID_DATE = INVALID_DATE;

function getDateMode(value) {
  if (value === 'now') {
    return DATE_MODES.NOW;
  }

  if (value.includes('now')) {
    return DATE_MODES.RELATIVE;
  }

  return DATE_MODES.ABSOLUTE;
}

function toAbsoluteString(value) {
  var roundUp = arguments.length > 1 && arguments[1] !== undefined ? arguments[1] : false;

  var valueAsMoment = _datemath.default.parse(value, {
    roundUp: roundUp
  });

  if (!valueAsMoment) {
    return value;
  }

  if (!(0, _moment.default)(valueAsMoment).isValid()) {
    return INVALID_DATE;
  }

  return valueAsMoment.toISOString();
}

function toRelativeString(value) {
  return (0, _relative_utils.toRelativeStringFromParts)((0, _relative_utils.parseRelativeParts)(value));
}