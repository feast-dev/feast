"use strict";

var _interopRequireDefault = require("@babel/runtime/helpers/interopRequireDefault");

Object.defineProperty(exports, "__esModule", {
  value: true
});
exports.border = void 0;

var _slicedToArray2 = _interopRequireDefault(require("@babel/runtime/helpers/slicedToArray"));

var _utils = require("../../services/theme/utils");

var _size = require("../../services/theme/size");

/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */
var border = {
  color: (0, _utils.computed)(function (_ref) {
    var _ref2 = (0, _slicedToArray2.default)(_ref, 1),
        lightShade = _ref2[0];

    return lightShade;
  }, ['colors.lightShade']),
  width: {
    thin: '1px',
    thick: '2px'
  },
  radius: {
    medium: (0, _utils.computed)((0, _size.sizeToPixel)(0.25)),
    small: (0, _utils.computed)((0, _size.sizeToPixel)(0.125))
  },
  thin: (0, _utils.computed)(function (_ref3) {
    var _ref4 = (0, _slicedToArray2.default)(_ref3, 2),
        width = _ref4[0],
        color = _ref4[1];

    return "".concat(width.thin, " solid ").concat(color);
  }, ['border.width', 'border.color']),
  thick: (0, _utils.computed)(function (_ref5) {
    var _ref6 = (0, _slicedToArray2.default)(_ref5, 2),
        width = _ref6[0],
        color = _ref6[1];

    return "".concat(width.thick, " solid ").concat(color);
  }, ['border.width', 'border.color']),
  editable: (0, _utils.computed)(function (_ref7) {
    var _ref8 = (0, _slicedToArray2.default)(_ref7, 2),
        width = _ref8[0],
        color = _ref8[1];

    return "".concat(width.thick, " dotted ").concat(color);
  }, ['border.width', 'border.color'])
};
exports.border = border;