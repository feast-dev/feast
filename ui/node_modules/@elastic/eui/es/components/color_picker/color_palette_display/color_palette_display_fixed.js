function _objectWithoutProperties(source, excluded) { if (source == null) return {}; var target = _objectWithoutPropertiesLoose(source, excluded); var key, i; if (Object.getOwnPropertySymbols) { var sourceSymbolKeys = Object.getOwnPropertySymbols(source); for (i = 0; i < sourceSymbolKeys.length; i++) { key = sourceSymbolKeys[i]; if (excluded.indexOf(key) >= 0) continue; if (!Object.prototype.propertyIsEnumerable.call(source, key)) continue; target[key] = source[key]; } } return target; }

function _objectWithoutPropertiesLoose(source, excluded) { if (source == null) return {}; var target = {}; var sourceKeys = Object.keys(source); var key, i; for (i = 0; i < sourceKeys.length; i++) { key = sourceKeys[i]; if (excluded.indexOf(key) >= 0) continue; target[key] = source[key]; } return target; }

/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */
import React from 'react';
import PropTypes from "prop-types";
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
EuiColorPaletteDisplayFixed.propTypes = {
  className: PropTypes.string,
  "aria-label": PropTypes.string,
  "data-test-subj": PropTypes.string,

  /**
     * Array of color `strings` or an array of #ColorStop. The stops must be numbers in an ordered range.
     */
  palette: PropTypes.oneOfType([PropTypes.arrayOf(PropTypes.string.isRequired).isRequired, PropTypes.arrayOf(PropTypes.shape({
    stop: PropTypes.number.isRequired,
    color: PropTypes.string.isRequired
  }).isRequired).isRequired]).isRequired
};