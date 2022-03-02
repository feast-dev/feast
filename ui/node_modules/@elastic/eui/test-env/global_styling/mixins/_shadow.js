"use strict";

var _interopRequireDefault = require("@babel/runtime/helpers/interopRequireDefault");

Object.defineProperty(exports, "__esModule", {
  value: true
});
exports.useOverflowShadow = exports.useSlightShadowActive = exports.useSlightShadowHover = exports.useBottomShadowLarge = exports.useBottomShadow = exports.useBottomShadowFlat = exports.useBottomShadowMedium = exports.useBottomShadowSmall = exports.useSlightShadow = void 0;

var _chromaJs = _interopRequireDefault(require("chroma-js"));

var _hooks = require("../../services/theme/hooks");

var _color2 = require("../../services/color");

/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

/**
 * NOTE: These were quick conversions of their Sass counterparts.
 *       They have yet to be used/tested.
 */
var useSlightShadow = function useSlightShadow() {
  var _ref = arguments.length > 0 && arguments[0] !== undefined ? arguments[0] : {},
      color = _ref.color,
      opacity = _ref.opacity;

  var _useEuiTheme = (0, _hooks.useEuiTheme)(),
      colors = _useEuiTheme.euiTheme.colors;

  var rgba = (0, _chromaJs.default)(color || colors.shadow).alpha(opacity || 0.3).css();
  return "box-shadow: 0 2px 2px -1px ".concat(rgba, ";");
};

exports.useSlightShadow = useSlightShadow;

var useBottomShadowSmall = function useBottomShadowSmall() {
  var _ref2 = arguments.length > 0 && arguments[0] !== undefined ? arguments[0] : {},
      color = _ref2.color,
      opacity = _ref2.opacity;

  var _useEuiTheme2 = (0, _hooks.useEuiTheme)(),
      colors = _useEuiTheme2.euiTheme.colors;

  var rgba = (0, _chromaJs.default)(color || colors.shadow).alpha(opacity || 0.3).css();
  return "\n  box-shadow:\n    0 2px 2px -1px ".concat(rgba, ",\n    0 1px 5px -2px ").concat(rgba, ";\n  ");
};

exports.useBottomShadowSmall = useBottomShadowSmall;

var useBottomShadowMedium = function useBottomShadowMedium() {
  var _ref3 = arguments.length > 0 && arguments[0] !== undefined ? arguments[0] : {},
      color = _ref3.color,
      opacity = _ref3.opacity;

  var _useEuiTheme3 = (0, _hooks.useEuiTheme)(),
      colors = _useEuiTheme3.euiTheme.colors;

  var rgba = (0, _chromaJs.default)(color || colors.shadow).alpha(opacity || 0.2).css();
  return "\n  box-shadow:\n    0 6px 12px -1px ".concat(rgba, ",\n    0 4px 4px -1px ").concat(rgba, ",\n    0 2px 2px 0 ").concat(rgba, ";\n  ");
}; // Similar to shadow medium but without the bottom depth. Useful for popovers
// that drop UP rather than DOWN.


exports.useBottomShadowMedium = useBottomShadowMedium;

var useBottomShadowFlat = function useBottomShadowFlat() {
  var _ref4 = arguments.length > 0 && arguments[0] !== undefined ? arguments[0] : {},
      color = _ref4.color,
      opacity = _ref4.opacity;

  var _useEuiTheme4 = (0, _hooks.useEuiTheme)(),
      colors = _useEuiTheme4.euiTheme.colors;

  var rgba = (0, _chromaJs.default)(color || colors.shadow).alpha(opacity || 0.2).css();
  return "\n  box-shadow:\n    0 0 12px -1px ".concat(rgba, ",\n    0 0 4px -1px ").concat(rgba, ",\n    0 0 2px 0 ").concat(rgba, ";\n  ");
}; // adjustBorder allows the border color to match the drop shadow better so that there's better
// distinction between element bounds and the shadow (crisper borders)


exports.useBottomShadowFlat = useBottomShadowFlat;

var useBottomShadow = function useBottomShadow() {
  var _ref5 = arguments.length > 0 && arguments[0] !== undefined ? arguments[0] : {},
      _color = _ref5.color,
      opacity = _ref5.opacity,
      adjustBorders = _ref5.adjustBorders;

  var _useEuiTheme5 = (0, _hooks.useEuiTheme)(),
      _useEuiTheme5$euiThem = _useEuiTheme5.euiTheme,
      border = _useEuiTheme5$euiThem.border,
      colors = _useEuiTheme5$euiThem.colors;

  var color = _color || colors.shadow;
  var rgba = (0, _chromaJs.default)(color).alpha(opacity || 0.2).css();
  var adjustedBorders = adjustBorders && !((0, _color2.lightness)(border.color) < 50) ? "\n  border-color: ".concat((0, _color2.tint)(color, 0.75), ";\n  border-top-color: ").concat((0, _color2.tint)(color, 0.8), ";\n  border-bottom-color: ").concat((0, _color2.tint)(color, 0.55), ";\n  ") : '';
  return "\n  box-shadow:\n    0 12px 24px 0 ".concat(rgba, ",\n    0 6px 12px 0 ").concat(rgba, ",\n    0 4px 4px 0 ").concat(rgba, ",\n    0 2px 2px 0 ").concat(rgba, ";\n  ").concat(adjustedBorders, "\n  ");
};

exports.useBottomShadow = useBottomShadow;

var useBottomShadowLarge = function useBottomShadowLarge() {
  var _ref6 = arguments.length > 0 && arguments[0] !== undefined ? arguments[0] : {},
      _color = _ref6.color,
      opacity = _ref6.opacity,
      adjustBorders = _ref6.adjustBorders,
      reverse = _ref6.reverse;

  var _useEuiTheme6 = (0, _hooks.useEuiTheme)(),
      _useEuiTheme6$euiThem = _useEuiTheme6.euiTheme,
      border = _useEuiTheme6$euiThem.border,
      colors = _useEuiTheme6$euiThem.colors;

  var color = _color || colors.shadow;
  var rgba = (0, _chromaJs.default)(color).alpha(opacity || 0.1).css(); // Never adjust borders if the border color is already on the dark side (dark theme)

  var adjustedBorders = adjustBorders && !((0, _color2.lightness)(border.color) < 50) ? "\n    border-color: ".concat((0, _color2.tint)(color, 0.75), ";\n    border-top-color: ").concat((0, _color2.tint)(color, 0.8), ";\n    border-bottom-color: ").concat((0, _color2.tint)(color, 0.55), ";\n    ") : '';

  if (reverse) {
    return "\n    box-shadow:\n      0 -40px 64px 0 ".concat(rgba, ",\n      0 -24px 32px 0 ").concat(rgba, ",\n      0 -16px 16px 0 ").concat(rgba, ",\n      0 -8px 8px 0 ").concat(rgba, ";\n      ").concat(adjustedBorders, "\n    ");
  } else {
    return "\n    box-shadow:\n      0 40px 64px 0 ".concat(rgba, ",\n      0 24px 32px 0 ").concat(rgba, ",\n      0 16px 16px 0 ").concat(rgba, ",\n      0 8px 8px 0 ").concat(rgba, ",\n      0 4px 4px 0 ").concat(rgba, ",\n      0 2px 2px 0 ").concat(rgba, ";\n      ").concat(adjustedBorders, "\n    ");
  }
};

exports.useBottomShadowLarge = useBottomShadowLarge;

var useSlightShadowHover = function useSlightShadowHover() {
  var _ref7 = arguments.length > 0 && arguments[0] !== undefined ? arguments[0] : {},
      color = _ref7.color,
      _opacity = _ref7.opacity;

  var _useEuiTheme7 = (0, _hooks.useEuiTheme)(),
      colors = _useEuiTheme7.euiTheme.colors;

  var opacity = _opacity || 0.3;
  var rgba1 = (0, _chromaJs.default)(color || colors.shadow).alpha(opacity).css();
  var rgba2 = (0, _chromaJs.default)(color || colors.shadow).alpha(opacity / 2).css();
  return "\n  box-shadow:\n    0 4px 8px 0 ".concat(rgba2, ",\n    0 2px 2px -1px ").concat(rgba1, ";\n  ");
};

exports.useSlightShadowHover = useSlightShadowHover;
var useSlightShadowActive = useSlightShadowHover;
exports.useSlightShadowActive = useSlightShadowActive;

var useOverflowShadow = function useOverflowShadow() {
  var _ref8 = arguments.length > 0 && arguments[0] !== undefined ? arguments[0] : {},
      _direction = _ref8.direction,
      _side = _ref8.side;

  var direction = _direction || 'y';
  var side = _side || 'both';

  var _useEuiTheme8 = (0, _hooks.useEuiTheme)(),
      size = _useEuiTheme8.euiTheme.size;

  var hideHeight = "calc(".concat(size.base, " * 0.75 * 1.25)");
  var gradientStart = "\n  ".concat((0, _color2.transparentize)('red', 0.9), " 0%,\n  ").concat((0, _color2.transparentize)('red', 0), " ").concat(hideHeight, ";\n  ");
  var gradientEnd = "\n  ".concat((0, _color2.transparentize)('red', 0), " calc(100% - ").concat(hideHeight, "),\n  ").concat((0, _color2.transparentize)('red', 0.9), " 100%;\n  ");
  var gradient = '';

  if (side) {
    if (side === 'both') {
      gradient = "".concat(gradientStart, ", ").concat(gradientEnd);
    } else if (side === 'start') {
      gradient = "".concat(gradientStart);
    } else {
      gradient = "".concat(gradientEnd);
    }
  }

  if (direction === 'y') {
    return "mask-image: linear-gradient(to bottom, ".concat(gradient, ")");
  } else {
    return "mask-image: linear-gradient(to right, ".concat(gradient, ")");
  }
};

exports.useOverflowShadow = useOverflowShadow;