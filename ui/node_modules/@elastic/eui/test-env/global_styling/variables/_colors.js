"use strict";

var _interopRequireDefault = require("@babel/runtime/helpers/interopRequireDefault");

Object.defineProperty(exports, "__esModule", {
  value: true
});
exports.colors = exports.dark_colors = exports.dark_shades = exports.light_colors = exports.text_colors = exports.special_colors = exports.shade_colors = exports.brand_text_colors = exports.brand_colors = void 0;

var _defineProperty2 = _interopRequireDefault(require("@babel/runtime/helpers/defineProperty"));

var _slicedToArray2 = _interopRequireDefault(require("@babel/runtime/helpers/slicedToArray"));

var _color = require("../../services/color");

var _utils = require("../../services/theme/utils");

var _contrast = require("../../services/color/contrast");

function ownKeys(object, enumerableOnly) { var keys = Object.keys(object); if (Object.getOwnPropertySymbols) { var symbols = Object.getOwnPropertySymbols(object); if (enumerableOnly) symbols = symbols.filter(function (sym) { return Object.getOwnPropertyDescriptor(object, sym).enumerable; }); keys.push.apply(keys, symbols); } return keys; }

function _objectSpread(target) { for (var i = 1; i < arguments.length; i++) { var source = arguments[i] != null ? arguments[i] : {}; if (i % 2) { ownKeys(Object(source), true).forEach(function (key) { (0, _defineProperty2.default)(target, key, source[key]); }); } else if (Object.getOwnPropertyDescriptors) { Object.defineProperties(target, Object.getOwnPropertyDescriptors(source)); } else { ownKeys(Object(source)).forEach(function (key) { Object.defineProperty(target, key, Object.getOwnPropertyDescriptor(source, key)); }); } } return target; }

/*
 * LIGHT THEME
 * Only split up in the light theme to access the keys by section in the docs
 */
var brand_colors = {
  primary: '#006BB4',
  accent: '#DD0A73',
  success: '#017D73',
  warning: '#F5A700',
  danger: '#BD271E'
};
exports.brand_colors = brand_colors;
var brand_text_colors = {
  primaryText: (0, _utils.computed)((0, _contrast.makeHighContrastColor)('colors.primary')),
  accentText: (0, _utils.computed)((0, _contrast.makeHighContrastColor)('colors.accent')),
  successText: (0, _utils.computed)((0, _contrast.makeHighContrastColor)('colors.success')),
  warningText: (0, _utils.computed)((0, _contrast.makeHighContrastColor)('colors.warning')),
  dangerText: (0, _utils.computed)((0, _contrast.makeHighContrastColor)('colors.danger'))
};
exports.brand_text_colors = brand_text_colors;
var shade_colors = {
  emptyShade: '#FFF',
  lightestShade: '#F5F7FA',
  lightShade: '#D3DAE6',
  mediumShade: '#98A2B3',
  darkShade: '#69707D',
  darkestShade: '#343741',
  fullShade: '#000'
};
exports.shade_colors = shade_colors;
var special_colors = {
  body: (0, _utils.computed)(function (_ref) {
    var _ref2 = (0, _slicedToArray2.default)(_ref, 1),
        lightestShade = _ref2[0];

    return (0, _color.tint)(lightestShade, 0.5);
  }, ['colors.lightestShade']),
  highlight: '#FFFCDD',
  disabled: (0, _utils.computed)(function (_ref3) {
    var _ref4 = (0, _slicedToArray2.default)(_ref3, 1),
        darkestShade = _ref4[0];

    return (0, _color.tint)(darkestShade, 0.7);
  }, ['colors.darkestShade']),
  disabledText: (0, _utils.computed)((0, _contrast.makeDisabledContrastColor)('colors.disabled')),
  shadow: (0, _utils.computed)(function (_ref5) {
    var colors = _ref5.colors;
    return (0, _color.shade)((0, _color.saturate)(colors.mediumShade, 0.25), 0.5);
  })
};
exports.special_colors = special_colors;
var text_colors = {
  text: (0, _utils.computed)((0, _contrast.makeHighContrastColor)('colors.darkestShade')),
  title: (0, _utils.computed)(function (_ref6) {
    var _ref7 = (0, _slicedToArray2.default)(_ref6, 1),
        _ref7$ = _ref7[0],
        text = _ref7$.text,
        body = _ref7$.body;

    return (0, _contrast.makeHighContrastColor)((0, _color.shade)(text, 0.5))(body);
  }, ['colors']),
  subdued: (0, _utils.computed)((0, _contrast.makeHighContrastColor)('colors.mediumShade')),
  link: (0, _utils.computed)(function (_ref8) {
    var _ref9 = (0, _slicedToArray2.default)(_ref8, 1),
        primaryText = _ref9[0];

    return primaryText;
  }, ['colors.primaryText'])
};
exports.text_colors = text_colors;

var light_colors = _objectSpread(_objectSpread(_objectSpread(_objectSpread(_objectSpread({}, brand_colors), shade_colors), special_colors), brand_text_colors), text_colors);
/*
 * DARK THEME
 */


exports.light_colors = light_colors;
var dark_shades = {
  emptyShade: '#1D1E24',
  lightestShade: '#25262E',
  lightShade: '#343741',
  mediumShade: '#535966',
  darkShade: '#98A2B3',
  darkestShade: '#D4DAE5',
  fullShade: '#FFF'
};
exports.dark_shades = dark_shades;

var dark_colors = _objectSpread(_objectSpread(_objectSpread({
  // Brand
  primary: '#1BA9F5',
  accent: '#F990C0',
  success: '#7DE2D1',
  warning: '#FFCE7A',
  danger: '#F66'
}, dark_shades), {}, {
  // Special
  body: (0, _utils.computed)(function (_ref10) {
    var _ref11 = (0, _slicedToArray2.default)(_ref10, 1),
        lightestShade = _ref11[0];

    return (0, _color.shade)(lightestShade, 0.45);
  }, ['colors.lightestShade']),
  highlight: '#2E2D25',
  disabled: (0, _utils.computed)(function (_ref12) {
    var _ref13 = (0, _slicedToArray2.default)(_ref12, 1),
        darkestShade = _ref13[0];

    return (0, _color.tint)(darkestShade, 0.7);
  }, ['colors.darkestShade']),
  disabledText: (0, _utils.computed)((0, _contrast.makeDisabledContrastColor)('colors.disabled')),
  shadow: (0, _utils.computed)(function (_ref14) {
    var colors = _ref14.colors;
    return (0, _color.shade)((0, _color.saturate)(colors.mediumShade, 0.25), 0.5);
  })
}, brand_text_colors), {}, {
  // Text
  text: '#DFE5EF',
  title: (0, _utils.computed)(function (_ref15) {
    var _ref16 = (0, _slicedToArray2.default)(_ref15, 1),
        text = _ref16[0];

    return text;
  }, ['colors.text']),
  subdued: (0, _utils.computed)((0, _contrast.makeHighContrastColor)('colors.mediumShade')),
  link: (0, _utils.computed)(function (_ref17) {
    var _ref18 = (0, _slicedToArray2.default)(_ref17, 1),
        primaryText = _ref18[0];

    return primaryText;
  }, ['colors.primaryText'])
});
/*
 * FULL
 */


exports.dark_colors = dark_colors;
var colors = {
  ghost: '#FFF',
  ink: '#000',
  LIGHT: light_colors,
  DARK: dark_colors
};
exports.colors = colors;