import _slicedToArray from "@babel/runtime/helpers/slicedToArray";

/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */
import { computed } from '../../services/theme/utils';
import { sizeToPixel } from '../../services/theme/size';
import { shade, tint, transparentize } from '../../services/color';
export var focus = {
  color: computed(function (_ref) {
    var colors = _ref.colors;
    return transparentize(colors.primary, 0.3);
  }),
  transparency: {
    LIGHT: 0.1,
    DARK: 0.3
  },
  backgroundColor: {
    LIGHT: computed(function (_ref2) {
      var _ref3 = _slicedToArray(_ref2, 2),
          primary = _ref3[0],
          transparency = _ref3[1];

      return tint(primary, 1 - transparency);
    }, ['colors.primary', 'focus.transparency']),
    DARK: computed(function (_ref4) {
      var _ref5 = _slicedToArray(_ref4, 2),
          primary = _ref5[0],
          transparency = _ref5[1];

      return shade(primary, 1 - transparency);
    }, ['colors.primary', 'focus.transparency'])
  },
  // Sizing
  widthLarge: computed(sizeToPixel(0.25)),
  width: computed(sizeToPixel(0.125)),
  // Outline
  outline: {
    'box-shadow': computed(function (_ref6) {
      var _ref7 = _slicedToArray(_ref6, 2),
          color = _ref7[0],
          width = _ref7[1];

      return "0 0 0 ".concat(width, " ").concat(color);
    }, ['focus.color', 'focus.width'])
  }
};