import _objectWithoutProperties from "@babel/runtime/helpers/objectWithoutProperties";

/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */
import React from 'react';
import { getFixedLinearGradient } from '../utils';
import { EuiScreenReaderOnly } from '../../accessibility/screen_reader';
import { jsx as ___EmotionJSX } from "@emotion/react";
export var EuiColorPaletteDisplayFixed = function EuiColorPaletteDisplayFixed(_ref) {
  var palette = _ref.palette,
      title = _ref.title,
      rest = _objectWithoutProperties(_ref, ["palette", "title"]);

  var fixedGradient = getFixedLinearGradient(palette);
  var paletteStops = fixedGradient.map(function (item, index) {
    return ___EmotionJSX("span", {
      style: {
        backgroundColor: item.color,
        width: item.width
      },
      key: "".concat(item.color, "-").concat(index)
    });
  });
  return ___EmotionJSX("span", rest, title && ___EmotionJSX(EuiScreenReaderOnly, null, ___EmotionJSX("span", null, title)), ___EmotionJSX("span", {
    // aria-hidden="true" is to ensure color blocks are ignored by screen readers,
    // and the only accessible text for options is the EuiScreenReaderOnly {title}
    "aria-hidden": "true",
    className: "euiColorPaletteDisplayFixed__bleedArea"
  }, paletteStops));
};