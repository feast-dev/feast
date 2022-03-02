function ownKeys(object, enumerableOnly) { var keys = Object.keys(object); if (Object.getOwnPropertySymbols) { var symbols = Object.getOwnPropertySymbols(object); if (enumerableOnly) symbols = symbols.filter(function (sym) { return Object.getOwnPropertyDescriptor(object, sym).enumerable; }); keys.push.apply(keys, symbols); } return keys; }

function _objectSpread(target) { for (var i = 1; i < arguments.length; i++) { var source = arguments[i] != null ? arguments[i] : {}; if (i % 2) { ownKeys(Object(source), true).forEach(function (key) { _defineProperty(target, key, source[key]); }); } else if (Object.getOwnPropertyDescriptors) { Object.defineProperties(target, Object.getOwnPropertyDescriptors(source)); } else { ownKeys(Object(source)).forEach(function (key) { Object.defineProperty(target, key, Object.getOwnPropertyDescriptor(source, key)); }); } } return target; }

function _defineProperty(obj, key, value) { if (key in obj) { Object.defineProperty(obj, key, { value: value, enumerable: true, configurable: true, writable: true }); } else { obj[key] = value; } return obj; }

function _slicedToArray(arr, i) { return _arrayWithHoles(arr) || _iterableToArrayLimit(arr, i) || _unsupportedIterableToArray(arr, i) || _nonIterableRest(); }

function _nonIterableRest() { throw new TypeError("Invalid attempt to destructure non-iterable instance.\nIn order to be iterable, non-array objects must have a [Symbol.iterator]() method."); }

function _unsupportedIterableToArray(o, minLen) { if (!o) return; if (typeof o === "string") return _arrayLikeToArray(o, minLen); var n = Object.prototype.toString.call(o).slice(8, -1); if (n === "Object" && o.constructor) n = o.constructor.name; if (n === "Map" || n === "Set") return Array.from(o); if (n === "Arguments" || /^(?:Ui|I)nt(?:8|16|32)(?:Clamped)?Array$/.test(n)) return _arrayLikeToArray(o, minLen); }

function _arrayLikeToArray(arr, len) { if (len == null || len > arr.length) len = arr.length; for (var i = 0, arr2 = new Array(len); i < len; i++) { arr2[i] = arr[i]; } return arr2; }

function _iterableToArrayLimit(arr, i) { if (typeof Symbol === "undefined" || !(Symbol.iterator in Object(arr))) return; var _arr = []; var _n = true; var _d = false; var _e = undefined; try { for (var _i = arr[Symbol.iterator](), _s; !(_n = (_s = _i.next()).done); _n = true) { _arr.push(_s.value); if (i && _arr.length === i) break; } } catch (err) { _d = true; _e = err; } finally { try { if (!_n && _i["return"] != null) _i["return"](); } finally { if (_d) throw _e; } } return _arr; }

function _arrayWithHoles(arr) { if (Array.isArray(arr)) return arr; }

/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */
import { keysOf } from '../../components/common';
import { computed } from '../../services/theme/utils';
/*
 * Font scale
 */
// Typographic scale -- loosely based on Major Third (1.250)

export var fontScale = {
  xxxs: 0.5625,
  xxs: 0.6875,
  xs: 0.75,
  s: 0.875,
  m: 1,
  l: 1.25,
  xl: 1.75,
  xxl: 2.125
};
export var SCALES = keysOf(fontScale);
// Families & base font settings
export var fontBase = {
  family: "'Inter UI', BlinkMacSystemFont, Helvetica, Arial, sans-serif",
  familyCode: "'Roboto Mono', Menlo, Courier, monospace",
  // Careful using ligatures. Code editors like ACE will often error because of width calculations
  featureSettings: "'calt' 1, 'kern' 1, 'liga' 1",
  baseline: computed(function (_ref) {
    var _ref2 = _slicedToArray(_ref, 1),
        base = _ref2[0];

    return base / 4;
  }, ['base']),
  lineHeightMultiplier: 1.5
};
/*
 * Font weights
 */

export var fontWeight = {
  light: 300,
  regular: 400,
  medium: 500,
  semiBold: 600,
  bold: 700
};
/*
 * Font
 */

export var font = _objectSpread(_objectSpread({}, fontBase), {}, {
  scale: fontScale,
  weight: fontWeight,
  body: {
    scale: 'm',
    weight: 'regular',
    letterSpacing: '-.005em'
  }
});