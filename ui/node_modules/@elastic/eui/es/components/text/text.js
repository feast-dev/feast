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
import { EuiTextColor } from './text_color';
import { EuiTextAlign } from './text_align';
import { jsx as ___EmotionJSX } from "@emotion/react";
var textSizeToClassNameMap = {
  xs: 'euiText--extraSmall',
  s: 'euiText--small',
  m: 'euiText--medium',
  relative: 'euiText--relative'
};
export var TEXT_SIZES = keysOf(textSizeToClassNameMap);
export var EuiText = function EuiText(_ref) {
  var _ref$size = _ref.size,
      size = _ref$size === void 0 ? 'm' : _ref$size,
      color = _ref.color,
      _ref$grow = _ref.grow,
      grow = _ref$grow === void 0 ? true : _ref$grow,
      textAlign = _ref.textAlign,
      children = _ref.children,
      className = _ref.className,
      rest = _objectWithoutProperties(_ref, ["size", "color", "grow", "textAlign", "children", "className"]);

  var classes = classNames('euiText', textSizeToClassNameMap[size], className, {
    'euiText--constrainedWidth': !grow
  });
  var optionallyAlteredText;

  if (color) {
    optionallyAlteredText = ___EmotionJSX(EuiTextColor, {
      color: color,
      component: "div"
    }, children);
  }

  if (textAlign) {
    optionallyAlteredText = ___EmotionJSX(EuiTextAlign, {
      textAlign: textAlign
    }, optionallyAlteredText || children);
  }

  return ___EmotionJSX("div", _extends({
    className: classes
  }, rest), optionallyAlteredText || children);
};
EuiText.propTypes = {
  className: PropTypes.string,
  "aria-label": PropTypes.string,
  "data-test-subj": PropTypes.string,
  textAlign: PropTypes.oneOf(["left", "right", "center"]),

  /**
       * Determines the text size. Choose `relative` to control the `font-size` based on the value of a parent container.
       */
  size: PropTypes.oneOf(["xs", "s", "m", "relative"]),

  /**
       * Any of our named colors or a `hex`, `rgb` or `rgba` value.
       */
  color: PropTypes.oneOfType([PropTypes.oneOf(["default", "subdued", "success", "accent", "danger", "warning", "ghost", "inherit"]).isRequired, PropTypes.any.isRequired]),
  grow: PropTypes.bool
};