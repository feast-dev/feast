"use strict";

var _interopRequireDefault = require("@babel/runtime/helpers/interopRequireDefault");

Object.defineProperty(exports, "__esModule", {
  value: true
});
exports.font = exports.fontWeight = exports.fontBase = exports.SCALES = exports.fontScale = void 0;

var _defineProperty2 = _interopRequireDefault(require("@babel/runtime/helpers/defineProperty"));

var _slicedToArray2 = _interopRequireDefault(require("@babel/runtime/helpers/slicedToArray"));

var _common = require("../../components/common");

var _utils = require("../../services/theme/utils");

function ownKeys(object, enumerableOnly) { var keys = Object.keys(object); if (Object.getOwnPropertySymbols) { var symbols = Object.getOwnPropertySymbols(object); if (enumerableOnly) symbols = symbols.filter(function (sym) { return Object.getOwnPropertyDescriptor(object, sym).enumerable; }); keys.push.apply(keys, symbols); } return keys; }

function _objectSpread(target) { for (var i = 1; i < arguments.length; i++) { var source = arguments[i] != null ? arguments[i] : {}; if (i % 2) { ownKeys(Object(source), true).forEach(function (key) { (0, _defineProperty2.default)(target, key, source[key]); }); } else if (Object.getOwnPropertyDescriptors) { Object.defineProperties(target, Object.getOwnPropertyDescriptors(source)); } else { ownKeys(Object(source)).forEach(function (key) { Object.defineProperty(target, key, Object.getOwnPropertyDescriptor(source, key)); }); } } return target; }

/*
 * Font scale
 */
// Typographic scale -- loosely based on Major Third (1.250)
var fontScale = {
  xxxs: 0.5625,
  xxs: 0.6875,
  xs: 0.75,
  s: 0.875,
  m: 1,
  l: 1.25,
  xl: 1.75,
  xxl: 2.125
};
exports.fontScale = fontScale;
var SCALES = (0, _common.keysOf)(fontScale);
exports.SCALES = SCALES;
// Families & base font settings
var fontBase = {
  family: "'Inter UI', BlinkMacSystemFont, Helvetica, Arial, sans-serif",
  familyCode: "'Roboto Mono', Menlo, Courier, monospace",
  // Careful using ligatures. Code editors like ACE will often error because of width calculations
  featureSettings: "'calt' 1, 'kern' 1, 'liga' 1",
  baseline: (0, _utils.computed)(function (_ref) {
    var _ref2 = (0, _slicedToArray2.default)(_ref, 1),
        base = _ref2[0];

    return base / 4;
  }, ['base']),
  lineHeightMultiplier: 1.5
};
/*
 * Font weights
 */

exports.fontBase = fontBase;
var fontWeight = {
  light: 300,
  regular: 400,
  medium: 500,
  semiBold: 600,
  bold: 700
};
/*
 * Font
 */

exports.fontWeight = fontWeight;

var font = _objectSpread(_objectSpread({}, fontBase), {}, {
  scale: fontScale,
  weight: fontWeight,
  body: {
    scale: 'm',
    weight: 'regular',
    letterSpacing: '-.005em'
  }
});

exports.font = font;