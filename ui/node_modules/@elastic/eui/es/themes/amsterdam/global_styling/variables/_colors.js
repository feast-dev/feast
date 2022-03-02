function _slicedToArray(arr, i) { return _arrayWithHoles(arr) || _iterableToArrayLimit(arr, i) || _unsupportedIterableToArray(arr, i) || _nonIterableRest(); }

function _nonIterableRest() { throw new TypeError("Invalid attempt to destructure non-iterable instance.\nIn order to be iterable, non-array objects must have a [Symbol.iterator]() method."); }

function _unsupportedIterableToArray(o, minLen) { if (!o) return; if (typeof o === "string") return _arrayLikeToArray(o, minLen); var n = Object.prototype.toString.call(o).slice(8, -1); if (n === "Object" && o.constructor) n = o.constructor.name; if (n === "Map" || n === "Set") return Array.from(o); if (n === "Arguments" || /^(?:Ui|I)nt(?:8|16|32)(?:Clamped)?Array$/.test(n)) return _arrayLikeToArray(o, minLen); }

function _arrayLikeToArray(arr, len) { if (len == null || len > arr.length) len = arr.length; for (var i = 0, arr2 = new Array(len); i < len; i++) { arr2[i] = arr[i]; } return arr2; }

function _iterableToArrayLimit(arr, i) { if (typeof Symbol === "undefined" || !(Symbol.iterator in Object(arr))) return; var _arr = []; var _n = true; var _d = false; var _e = undefined; try { for (var _i = arr[Symbol.iterator](), _s; !(_n = (_s = _i.next()).done); _n = true) { _arr.push(_s.value); if (i && _arr.length === i) break; } } catch (err) { _d = true; _e = err; } finally { try { if (!_n && _i["return"] != null) _i["return"](); } finally { if (_d) throw _e; } } return _arr; }

function _arrayWithHoles(arr) { if (Array.isArray(arr)) return arr; }

function ownKeys(object, enumerableOnly) { var keys = Object.keys(object); if (Object.getOwnPropertySymbols) { var symbols = Object.getOwnPropertySymbols(object); if (enumerableOnly) symbols = symbols.filter(function (sym) { return Object.getOwnPropertyDescriptor(object, sym).enumerable; }); keys.push.apply(keys, symbols); } return keys; }

function _objectSpread(target) { for (var i = 1; i < arguments.length; i++) { var source = arguments[i] != null ? arguments[i] : {}; if (i % 2) { ownKeys(Object(source), true).forEach(function (key) { _defineProperty(target, key, source[key]); }); } else if (Object.getOwnPropertyDescriptors) { Object.defineProperties(target, Object.getOwnPropertyDescriptors(source)); } else { ownKeys(Object(source)).forEach(function (key) { Object.defineProperty(target, key, Object.getOwnPropertyDescriptor(source, key)); }); } } return target; }

function _defineProperty(obj, key, value) { if (key in obj) { Object.defineProperty(obj, key, { value: value, enumerable: true, configurable: true, writable: true }); } else { obj[key] = value; } return obj; }

/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */
import { shade, tint } from '../../../../services/color';
import { computed } from '../../../../services/theme/utils';
import { makeHighContrastColor, makeDisabledContrastColor } from '../../../../services/color/contrast';
import { brand_text_colors, shade_colors, dark_shades } from '../../../../global_styling/variables/_colors';
/*
 * LIGHT THEME
 */

export var light_colors_ams = _objectSpread(_objectSpread(_objectSpread({
  // Brand
  primary: '#07C',
  accent: '#F04E98',
  success: '#00BFB3',
  warning: '#FEC514',
  danger: '#BD271E'
}, shade_colors), {}, {
  lightestShade: '#f0f4fb',
  // Special
  body: computed(function (_ref) {
    var _ref2 = _slicedToArray(_ref, 1),
        lightestShade = _ref2[0];

    return tint(lightestShade, 0.5);
  }, ['colors.lightestShade']),
  highlight: computed(function (_ref3) {
    var _ref4 = _slicedToArray(_ref3, 1),
        warning = _ref4[0];

    return tint(warning, 0.9);
  }, ['colors.warning']),
  disabled: '#ABB4C4',
  disabledText: computed(makeDisabledContrastColor('colors.disabled')),
  shadow: computed(function (_ref5) {
    var colors = _ref5.colors;
    return colors.ink;
  })
}, brand_text_colors), {}, {
  // Text
  text: computed(function (_ref6) {
    var _ref7 = _slicedToArray(_ref6, 1),
        darkestShade = _ref7[0];

    return darkestShade;
  }, ['colors.darkestShade']),
  title: computed(function (_ref8) {
    var _ref9 = _slicedToArray(_ref8, 1),
        text = _ref9[0];

    return shade(text, 0.5);
  }, ['colors.text']),
  subdued: computed(makeHighContrastColor('colors.darkShade')),
  link: computed(function (_ref10) {
    var _ref11 = _slicedToArray(_ref10, 1),
        primaryText = _ref11[0];

    return primaryText;
  }, ['colors.primaryText'])
});
/*
 * DARK THEME
 */

export var dark_colors_ams = _objectSpread(_objectSpread(_objectSpread({
  // Brand
  primary: '#36A2EF',
  accent: '#F68FBE',
  success: '#7DDED8',
  warning: '#F3D371',
  danger: '#F86B63'
}, dark_shades), {}, {
  // Special
  body: computed(function (_ref12) {
    var _ref13 = _slicedToArray(_ref12, 1),
        lightestShade = _ref13[0];

    return shade(lightestShade, 0.45);
  }, ['colors.lightestShade']),
  highlight: '#2E2D25',
  disabled: '#515761',
  disabledText: computed(makeDisabledContrastColor('colors.disabled')),
  shadow: computed(function (_ref14) {
    var colors = _ref14.colors;
    return colors.ink;
  })
}, brand_text_colors), {}, {
  // Text
  text: '#DFE5EF',
  title: computed(function (_ref15) {
    var _ref16 = _slicedToArray(_ref15, 1),
        text = _ref16[0];

    return text;
  }, ['colors.text']),
  subdued: computed(makeHighContrastColor('colors.mediumShade')),
  link: computed(function (_ref17) {
    var _ref18 = _slicedToArray(_ref17, 1),
        primaryText = _ref18[0];

    return primaryText;
  }, ['colors.primaryText'])
});
/*
 * FULL
 */

export var colors_ams = {
  ghost: '#FFF',
  ink: '#000',
  LIGHT: light_colors_ams,
  DARK: dark_colors_ams
};