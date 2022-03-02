import _defineProperty from "@babel/runtime/helpers/defineProperty";
import _slicedToArray from "@babel/runtime/helpers/slicedToArray";

function ownKeys(object, enumerableOnly) { var keys = Object.keys(object); if (Object.getOwnPropertySymbols) { var symbols = Object.getOwnPropertySymbols(object); if (enumerableOnly) symbols = symbols.filter(function (sym) { return Object.getOwnPropertyDescriptor(object, sym).enumerable; }); keys.push.apply(keys, symbols); } return keys; }

function _objectSpread(target) { for (var i = 1; i < arguments.length; i++) { var source = arguments[i] != null ? arguments[i] : {}; if (i % 2) { ownKeys(Object(source), true).forEach(function (key) { _defineProperty(target, key, source[key]); }); } else if (Object.getOwnPropertyDescriptors) { Object.defineProperties(target, Object.getOwnPropertyDescriptors(source)); } else { ownKeys(Object(source)).forEach(function (key) { Object.defineProperty(target, key, Object.getOwnPropertyDescriptor(source, key)); }); } } return target; }

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