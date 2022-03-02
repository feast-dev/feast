"use strict";

Object.defineProperty(exports, "__esModule", {
  value: true
});
exports.formatText = void 0;

var _predicate = require("../predicate");

/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */
var formatText = function formatText(value) {
  var options = arguments.length > 1 && arguments[1] !== undefined ? arguments[1] : {
    nil: ''
  };
  return (0, _predicate.isNil)(value) ? options.nil : value.toString();
};

exports.formatText = formatText;