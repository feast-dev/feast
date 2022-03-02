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
import { computed } from '../../../../services/theme/utils';
import { transparentize } from '../../../../services/color';
import { focus } from '../../../../global_styling/variables/_states';
/**
 * NOTE: These were quick conversions of their Sass counterparts.
 *       They have yet to be used/tested.
 */

export var focus_ams = _objectSpread(_objectSpread({}, focus), {}, {
  color: 'currentColor',
  transparency: {
    LIGHT: 0.9,
    DARK: 0.7
  },
  backgroundColor: computed(function (_ref) {
    var colors = _ref.colors,
        focus = _ref.focus;
    return transparentize(colors.primary, focus.transparency);
  }),
  // Outline
  outline: {
    outline: computed(function (_ref2) {
      var focus = _ref2.focus;
      return "".concat(focus.width, " solid ").concat(focus.color);
    })
  }
});