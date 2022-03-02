"use strict";

Object.defineProperty(exports, "__esModule", {
  value: true
});
exports.formatAuto = void 0;

var _predicate = require("../predicate");

var _format_boolean = require("./format_boolean");

var _format_date = require("./format_date");

var _format_number = require("./format_number");

var _format_text = require("./format_text");

/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */
var formatAuto = function formatAuto(value) {
  if ((0, _predicate.isNil)(value) || (0, _predicate.isNaN)(value)) {
    return '';
  }

  if ((0, _predicate.isString)(value)) {
    return (0, _format_text.formatText)(value);
  }

  if ((0, _predicate.isDate)(value)) {
    return (0, _format_date.formatDate)(value);
  }

  if ((0, _predicate.isBoolean)(value)) {
    return (0, _format_boolean.formatBoolean)(value);
  }

  if ((0, _predicate.isNumber)(value)) {
    return (0, _format_number.formatNumber)(value);
  }

  if ((0, _predicate.isArray)(value)) {
    return Array.isArray(value) ? value.map(function (item) {
      return formatAuto(item);
    }).join(', ') : formatAuto(value);
  } // TODO not sure if we want that.. the (+) is that we show something, the (-) is that it's very technical


  return JSON.stringify(value);
};

exports.formatAuto = formatAuto;