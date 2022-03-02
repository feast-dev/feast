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
export var border = {
  color: computed(function (_ref) {
    var _ref2 = _slicedToArray(_ref, 1),
        lightShade = _ref2[0];

    return lightShade;
  }, ['colors.lightShade']),
  width: {
    thin: '1px',
    thick: '2px'
  },
  radius: {
    medium: computed(sizeToPixel(0.25)),
    small: computed(sizeToPixel(0.125))
  },
  thin: computed(function (_ref3) {
    var _ref4 = _slicedToArray(_ref3, 2),
        width = _ref4[0],
        color = _ref4[1];

    return "".concat(width.thin, " solid ").concat(color);
  }, ['border.width', 'border.color']),
  thick: computed(function (_ref5) {
    var _ref6 = _slicedToArray(_ref5, 2),
        width = _ref6[0],
        color = _ref6[1];

    return "".concat(width.thick, " solid ").concat(color);
  }, ['border.width', 'border.color']),
  editable: computed(function (_ref7) {
    var _ref8 = _slicedToArray(_ref7, 2),
        width = _ref8[0],
        color = _ref8[1];

    return "".concat(width.thick, " dotted ").concat(color);
  }, ['border.width', 'border.color'])
};