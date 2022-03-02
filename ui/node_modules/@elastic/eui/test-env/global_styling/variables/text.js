"use strict";

var _interopRequireDefault = require("@babel/runtime/helpers/interopRequireDefault");

Object.defineProperty(exports, "__esModule", {
  value: true
});
exports.fontSize = void 0;

var _slicedToArray2 = _interopRequireDefault(require("@babel/runtime/helpers/slicedToArray"));

var _theme = require("../../services/theme");

var _typography = require("./_typography");

/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */
var fontSize = _typography.SCALES.reduce(function (acc, elem) {
  acc[elem] = {
    fontSize: (0, _theme.computed)(function (_ref) {
      var _ref2 = (0, _slicedToArray2.default)(_ref, 1),
          scale = _ref2[0];

      return "".concat(scale, "rem");
    }, ["font.scale.".concat(elem)]),
    lineHeight: (0, _theme.computed)(function (_ref3) {
      var _ref4 = (0, _slicedToArray2.default)(_ref3, 2),
          base = _ref4[0],
          font = _ref4[1];

      return (0, _theme.lineHeightFromBaseline)(base, font, font.scale[elem]);
    }, ['base', 'font'])
  };
  return acc;
}, {});

exports.fontSize = fontSize;