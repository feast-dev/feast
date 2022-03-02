function _extends() { _extends = Object.assign || function (target) { for (var i = 1; i < arguments.length; i++) { var source = arguments[i]; for (var key in source) { if (Object.prototype.hasOwnProperty.call(source, key)) { target[key] = source[key]; } } } return target; }; return _extends.apply(this, arguments); }

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
import classNames from 'classnames';
import { keysOf } from '../common';
import { EuiIcon } from '../icon';
import { EuiFlexGroup, EuiFlexItem } from '../flex';
import { jsx as ___EmotionJSX } from "@emotion/react";
var sizeToClassNameMap = {
  xs: 'euiHealth--textSizeXS',
  s: 'euiHealth--textSizeS',
  m: 'euiHealth--textSizeM',
  inherit: 'euiHealth--textSizeInherit'
};
export var TEXT_SIZES = keysOf(sizeToClassNameMap);
export var EuiHealth = function EuiHealth(_ref) {
  var children = _ref.children,
      className = _ref.className,
      color = _ref.color,
      _ref$textSize = _ref.textSize,
      textSize = _ref$textSize === void 0 ? 's' : _ref$textSize,
      rest = _objectWithoutProperties(_ref, ["children", "className", "color", "textSize"]);

  var classes = classNames('euiHealth', textSize ? sizeToClassNameMap[textSize] : null, className);
  return ___EmotionJSX("div", _extends({
    className: classes
  }, rest), ___EmotionJSX(EuiFlexGroup, {
    gutterSize: "xs",
    alignItems: "center",
    responsive: false
  }, ___EmotionJSX(EuiFlexItem, {
    grow: false
  }, ___EmotionJSX(EuiIcon, {
    type: "dot",
    color: color
  })), ___EmotionJSX(EuiFlexItem, {
    grow: false
  }, children)));
};
EuiHealth.propTypes = {
  className: PropTypes.string,
  "aria-label": PropTypes.string,
  "data-test-subj": PropTypes.string,

  /**
       * Sets the color of the dot icon.
       * It accepts any `IconColor`: `default`, `primary`, `success`, `accent`, `warning`, `danger`, `text`,
       * `subdued` or `ghost`; or any valid CSS color value as a `string`
       */
  color: PropTypes.oneOfType([PropTypes.string.isRequired, PropTypes.oneOf(["default", "primary", "success", "accent", "warning", "danger", "text", "subdued", "ghost", "inherit"]).isRequired]),

  /**
       * Matches the text scales of EuiText.
       * The `inherit` style will get its font size from the parent element
       */
  textSize: PropTypes.any
};