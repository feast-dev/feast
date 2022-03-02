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
import classNames from 'classnames';
import PropTypes from "prop-types";
import React from 'react';
import { EuiStepHorizontal } from './step_horizontal';
import { jsx as ___EmotionJSX } from "@emotion/react";
export var EuiStepsHorizontal = function EuiStepsHorizontal(_ref) {
  var className = _ref.className,
      steps = _ref.steps,
      rest = _objectWithoutProperties(_ref, ["className", "steps"]);

  var classes = classNames('euiStepsHorizontal', className);
  return ___EmotionJSX("ol", _extends({
    className: classes
  }, rest), steps.map(function (stepProps, index) {
    var isCurrent = stepProps.isSelected || stepProps.status === 'current' ? {
      'aria-current': 'step'
    } : {};
    return ___EmotionJSX("li", _extends({
      key: index,
      className: "euiStepHorizontal__item"
    }, isCurrent), ___EmotionJSX(EuiStepHorizontal, _extends({
      step: index + 1
    }, stepProps)));
  }));
};
EuiStepsHorizontal.propTypes = {
  /**
     * An array of `EuiStepHorizontal` objects excluding the `step` prop
     */
  steps: PropTypes.arrayOf(PropTypes.any.isRequired).isRequired,
  className: PropTypes.string,
  "aria-label": PropTypes.string,
  "data-test-subj": PropTypes.string
};