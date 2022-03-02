import _slicedToArray from "@babel/runtime/helpers/slicedToArray";
import _defineProperty from "@babel/runtime/helpers/defineProperty";

function ownKeys(object, enumerableOnly) { var keys = Object.keys(object); if (Object.getOwnPropertySymbols) { var symbols = Object.getOwnPropertySymbols(object); if (enumerableOnly) symbols = symbols.filter(function (sym) { return Object.getOwnPropertyDescriptor(object, sym).enumerable; }); keys.push.apply(keys, symbols); } return keys; }

function _objectSpread(target) { for (var i = 1; i < arguments.length; i++) { var source = arguments[i] != null ? arguments[i] : {}; if (i % 2) { ownKeys(Object(source), true).forEach(function (key) { _defineProperty(target, key, source[key]); }); } else if (Object.getOwnPropertyDescriptors) { Object.defineProperties(target, Object.getOwnPropertyDescriptors(source)); } else { ownKeys(Object(source)).forEach(function (key) { Object.defineProperty(target, key, Object.getOwnPropertyDescriptor(source, key)); }); } } return target; }

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
import { title } from '../../../../global_styling/variables/title';
import { SCALES } from '../../../../global_styling/variables/_typography';
import { computed } from '../../../../services/theme/utils'; // For Amsterdam, change all font-weights to bold and remove letter-spacing

export var title_ams = SCALES.reduce(function (acc, elem) {
  acc[elem] = _objectSpread(_objectSpread({}, title[elem]), {}, {
    fontWeight: computed(function (_ref) {
      var _ref2 = _slicedToArray(_ref, 1),
          fontWeight = _ref2[0];

      return fontWeight;
    }, ['font.weight.bold']),
    letterSpacing: undefined
  });
  return acc;
}, {});