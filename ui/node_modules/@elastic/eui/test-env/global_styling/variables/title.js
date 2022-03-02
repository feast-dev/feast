"use strict";

var _interopRequireDefault = require("@babel/runtime/helpers/interopRequireDefault");

Object.defineProperty(exports, "__esModule", {
  value: true
});
exports.title = void 0;

var _slicedToArray2 = _interopRequireDefault(require("@babel/runtime/helpers/slicedToArray"));

var _utils = require("../../services/theme/utils");

var _typography = require("./_typography");

/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */
var titlesPartial = {
  xxxs: {
    fontWeight: 'bold',
    letterSpacing: undefined
  },
  xxs: {
    fontWeight: 'bold',
    letterSpacing: undefined
  },
  xs: {
    fontWeight: 'bold',
    letterSpacing: undefined
  },
  s: {
    fontWeight: 'bold',
    letterSpacing: undefined
  },
  m: {
    fontWeight: 'semiBold',
    letterSpacing: '-.02em'
  },
  l: {
    fontWeight: 'medium',
    letterSpacing: '-.025em'
  },
  xl: {
    fontWeight: 'light',
    letterSpacing: '-.04em'
  },
  xxl: {
    fontWeight: 'light',
    letterSpacing: '-.03em'
  }
};

var title = _typography.SCALES.reduce(function (acc, size) {
  acc[size] = {
    fontSize: (0, _utils.computed)(function (_ref) {
      var _ref2 = (0, _slicedToArray2.default)(_ref, 1),
          fontSize = _ref2[0].fontSize;

      return fontSize;
    }, ["font.size.".concat(size)]),
    lineHeight: (0, _utils.computed)(function (_ref3) {
      var _ref4 = (0, _slicedToArray2.default)(_ref3, 1),
          lineHeight = _ref4[0].lineHeight;

      return lineHeight;
    }, ["font.size.".concat(size)]),
    color: (0, _utils.computed)(function (_ref5) {
      var _ref6 = (0, _slicedToArray2.default)(_ref5, 1),
          color = _ref6[0];

      return color;
    }, ['colors.title']),
    fontWeight: (0, _utils.computed)(function (_ref7) {
      var _ref8 = (0, _slicedToArray2.default)(_ref7, 1),
          fontWeight = _ref8[0];

      return fontWeight;
    }, ["font.weight.".concat(titlesPartial[size].fontWeight)]),
    letterSpacing: titlesPartial[size].letterSpacing
  };
  return acc;
}, {});

exports.title = title;