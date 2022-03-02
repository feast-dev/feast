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
import { jsx as ___EmotionJSX } from "@emotion/react";
export var EuiRangeThumb = function EuiRangeThumb(_ref) {
  var className = _ref.className,
      min = _ref.min,
      max = _ref.max,
      value = _ref.value,
      disabled = _ref.disabled,
      showInput = _ref.showInput,
      showTicks = _ref.showTicks,
      onClick = _ref.onClick,
      onMouseDown = _ref.onMouseDown,
      tabIndex = _ref.tabIndex,
      rest = _objectWithoutProperties(_ref, ["className", "min", "max", "value", "disabled", "showInput", "showTicks", "onClick", "onMouseDown", "tabIndex"]);

  var classes = classNames('euiRangeThumb', {
    'euiRangeThumb--hasTicks': showTicks
  }, className);
  var commonAttrs = {
    className: classes,
    role: 'slider',
    'aria-valuemin': min,
    'aria-valuemax': max,
    'aria-valuenow': Number(value),
    'aria-disabled': !!disabled,
    tabIndex: showInput || !!disabled ? -1 : tabIndex || 0
  };
  return onClick || onMouseDown ? ___EmotionJSX("button", _extends({
    type: "button",
    onClick: onClick,
    onMouseDown: onMouseDown,
    disabled: disabled
  }, commonAttrs, rest)) : ___EmotionJSX("div", _extends({}, commonAttrs, rest));
};
EuiRangeThumb.propTypes = {
  min: PropTypes.number.isRequired,
  max: PropTypes.number.isRequired,
  value: PropTypes.oneOfType([PropTypes.number.isRequired, PropTypes.string.isRequired]),
  disabled: PropTypes.bool,
  showInput: PropTypes.bool,
  showTicks: PropTypes.bool,
  className: PropTypes.string,
  "aria-label": PropTypes.string,
  "data-test-subj": PropTypes.string
};