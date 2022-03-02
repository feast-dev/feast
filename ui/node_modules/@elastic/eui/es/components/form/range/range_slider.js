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
import React, { forwardRef } from 'react';
import PropTypes from "prop-types";
import classNames from 'classnames';
import { jsx as ___EmotionJSX } from "@emotion/react";
export var EuiRangeSlider = /*#__PURE__*/forwardRef(function (_ref, ref) {
  var className = _ref.className,
      disabled = _ref.disabled,
      id = _ref.id,
      max = _ref.max,
      min = _ref.min,
      name = _ref.name,
      step = _ref.step,
      onChange = _ref.onChange,
      tabIndex = _ref.tabIndex,
      value = _ref.value,
      style = _ref.style,
      showTicks = _ref.showTicks,
      showRange = _ref.showRange,
      hasFocus = _ref.hasFocus,
      compressed = _ref.compressed,
      rest = _objectWithoutProperties(_ref, ["className", "disabled", "id", "max", "min", "name", "step", "onChange", "tabIndex", "value", "style", "showTicks", "showRange", "hasFocus", "compressed"]);

  var classes = classNames('euiRangeSlider', {
    'euiRangeSlider--hasTicks': showTicks,
    'euiRangeSlider--hasFocus': hasFocus,
    'euiRangeSlider--hasRange': showRange,
    'euiRangeSlider--compressed': compressed
  }, className);
  return ___EmotionJSX("input", _extends({
    ref: ref,
    type: "range",
    id: id,
    name: name,
    className: classes,
    min: min,
    max: max,
    step: step,
    value: value,
    disabled: disabled,
    onChange: onChange,
    style: style,
    tabIndex: tabIndex
  }, rest));
});
EuiRangeSlider.propTypes = {
  className: PropTypes.string,
  "aria-label": PropTypes.string,
  "data-test-subj": PropTypes.string,
  id: PropTypes.string,
  name: PropTypes.string,
  min: PropTypes.number.isRequired,
  max: PropTypes.number.isRequired,
  step: PropTypes.number,
  compressed: PropTypes.bool,
  hasFocus: PropTypes.bool,
  showRange: PropTypes.bool,
  showTicks: PropTypes.bool,
  disabled: PropTypes.bool,
  tabIndex: PropTypes.number,
  onChange: PropTypes.any
};
EuiRangeSlider.displayName = 'EuiRangeSlider';