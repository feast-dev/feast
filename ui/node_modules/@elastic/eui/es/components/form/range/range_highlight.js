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
export var EuiRangeHighlight = function EuiRangeHighlight(_ref) {
  var className = _ref.className,
      hasFocus = _ref.hasFocus,
      showTicks = _ref.showTicks,
      lowerValue = _ref.lowerValue,
      upperValue = _ref.upperValue,
      max = _ref.max,
      min = _ref.min,
      compressed = _ref.compressed,
      background = _ref.background,
      onClick = _ref.onClick;
  // Calculate the width the range based on value
  // const rangeWidth = (value - min) / (max - min);
  var leftPosition = (lowerValue - min) / (max - min);
  var rangeWidth = (upperValue - lowerValue) / (max - min);
  var rangeWidthStyle = {
    background: background,
    marginLeft: "".concat(leftPosition * 100, "%"),
    width: "".concat(rangeWidth * 100, "%")
  };
  var classes = classNames('euiRangeHighlight', {
    'euiRangeHighlight--hasTicks': showTicks,
    'euiRangeHighlight--compressed': compressed
  }, className);
  var progressClasses = classNames('euiRangeHighlight__progress', {
    'euiRangeHighlight__progress--hasFocus': hasFocus
  });
  return ___EmotionJSX("div", {
    className: classes,
    onClick: onClick
  }, ___EmotionJSX("div", {
    className: progressClasses,
    style: rangeWidthStyle
  }));
};
EuiRangeHighlight.propTypes = {
  className: PropTypes.string,
  background: PropTypes.string,
  compressed: PropTypes.bool,
  hasFocus: PropTypes.bool,
  showTicks: PropTypes.bool,
  lowerValue: PropTypes.number.isRequired,
  upperValue: PropTypes.number.isRequired,
  max: PropTypes.number.isRequired,
  min: PropTypes.number.isRequired,
  onClick: PropTypes.func
};