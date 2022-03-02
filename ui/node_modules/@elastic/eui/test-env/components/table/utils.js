"use strict";

var _interopRequireDefault = require("@babel/runtime/helpers/interopRequireDefault");

Object.defineProperty(exports, "__esModule", {
  value: true
});
exports.resolveWidthAsStyle = exports.WARNING_MESSAGE = void 0;

var _defineProperty2 = _interopRequireDefault(require("@babel/runtime/helpers/defineProperty"));

var _objectWithoutProperties2 = _interopRequireDefault(require("@babel/runtime/helpers/objectWithoutProperties"));

function ownKeys(object, enumerableOnly) { var keys = Object.keys(object); if (Object.getOwnPropertySymbols) { var symbols = Object.getOwnPropertySymbols(object); if (enumerableOnly) symbols = symbols.filter(function (sym) { return Object.getOwnPropertyDescriptor(object, sym).enumerable; }); keys.push.apply(keys, symbols); } return keys; }

function _objectSpread(target) { for (var i = 1; i < arguments.length; i++) { var source = arguments[i] != null ? arguments[i] : {}; if (i % 2) { ownKeys(Object(source), true).forEach(function (key) { (0, _defineProperty2.default)(target, key, source[key]); }); } else if (Object.getOwnPropertyDescriptors) { Object.defineProperties(target, Object.getOwnPropertyDescriptors(source)); } else { ownKeys(Object(source)).forEach(function (key) { Object.defineProperty(target, key, Object.getOwnPropertyDescriptor(source, key)); }); } } return target; }

/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */
var WARNING_MESSAGE = 'Two `width` properties were provided. Provide only one of `style.width` or `width` to avoid conflicts.';
exports.WARNING_MESSAGE = WARNING_MESSAGE;

var resolveWidthAsStyle = function resolveWidthAsStyle() {
  var style = arguments.length > 0 && arguments[0] !== undefined ? arguments[0] : {};
  var width = arguments.length > 1 ? arguments[1] : undefined;
  var styleWidth = style.width,
      styleRest = (0, _objectWithoutProperties2.default)(style, ["width"]);
  var attrWidth = width;

  if (attrWidth != null && (typeof attrWidth === 'number' || !isNaN(Number(attrWidth))) // transform {number} or unitless 'number' to px string
  ) {
      attrWidth = "".concat(attrWidth, "px");
    }

  if (styleWidth && attrWidth) {
    console.warn(WARNING_MESSAGE);
  }

  return _objectSpread(_objectSpread({}, styleRest), {}, {
    width: attrWidth || styleWidth
  });
};

exports.resolveWidthAsStyle = resolveWidthAsStyle;