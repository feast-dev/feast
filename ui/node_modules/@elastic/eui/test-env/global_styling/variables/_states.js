"use strict";

var _interopRequireDefault = require("@babel/runtime/helpers/interopRequireDefault");

Object.defineProperty(exports, "__esModule", {
  value: true
});
exports.focus = void 0;

var _slicedToArray2 = _interopRequireDefault(require("@babel/runtime/helpers/slicedToArray"));

var _utils = require("../../services/theme/utils");

var _size = require("../../services/theme/size");

var _color = require("../../services/color");

/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */
var focus = {
  color: (0, _utils.computed)(function (_ref) {
    var colors = _ref.colors;
    return (0, _color.transparentize)(colors.primary, 0.3);
  }),
  transparency: {
    LIGHT: 0.1,
    DARK: 0.3
  },
  backgroundColor: {
    LIGHT: (0, _utils.computed)(function (_ref2) {
      var _ref3 = (0, _slicedToArray2.default)(_ref2, 2),
          primary = _ref3[0],
          transparency = _ref3[1];

      return (0, _color.tint)(primary, 1 - transparency);
    }, ['colors.primary', 'focus.transparency']),
    DARK: (0, _utils.computed)(function (_ref4) {
      var _ref5 = (0, _slicedToArray2.default)(_ref4, 2),
          primary = _ref5[0],
          transparency = _ref5[1];

      return (0, _color.shade)(primary, 1 - transparency);
    }, ['colors.primary', 'focus.transparency'])
  },
  // Sizing
  widthLarge: (0, _utils.computed)((0, _size.sizeToPixel)(0.25)),
  width: (0, _utils.computed)((0, _size.sizeToPixel)(0.125)),
  // Outline
  outline: {
    'box-shadow': (0, _utils.computed)(function (_ref6) {
      var _ref7 = (0, _slicedToArray2.default)(_ref6, 2),
          color = _ref7[0],
          width = _ref7[1];

      return "0 0 0 ".concat(width, " ").concat(color);
    }, ['focus.color', 'focus.width'])
  }
};
exports.focus = focus;