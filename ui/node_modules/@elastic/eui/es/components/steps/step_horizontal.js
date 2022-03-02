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
import { EuiStepNumber } from './step_number';
import { useI18nCompleteStep, useI18nCurrentStep, useI18nDisabledStep, useI18nIncompleteStep, useI18nStep, useI18nWarningStep } from './step_strings';
import { jsx as ___EmotionJSX } from "@emotion/react";
export var EuiStepHorizontal = function EuiStepHorizontal(_ref) {
  var className = _ref.className,
      _ref$step = _ref.step,
      step = _ref$step === void 0 ? 1 : _ref$step,
      title = _ref.title,
      isSelected = _ref.isSelected,
      isComplete = _ref.isComplete,
      onClick = _ref.onClick,
      disabled = _ref.disabled,
      _ref$status = _ref.status,
      status = _ref$status === void 0 ? 'incomplete' : _ref$status,
      rest = _objectWithoutProperties(_ref, ["className", "step", "title", "isSelected", "isComplete", "onClick", "disabled", "status"]);

  var buttonTitle = useI18nStep({
    number: step,
    title: title
  });
  var completeTitle = useI18nCompleteStep({
    number: step,
    title: title
  });
  var disabledTitle = useI18nDisabledStep({
    number: step,
    title: title
  });
  var incompleteTitle = useI18nIncompleteStep({
    number: step,
    title: title
  });
  var warningTitle = useI18nWarningStep({
    number: step,
    title: title
  });
  var currentTitle = useI18nCurrentStep({
    number: step,
    title: title
  });
  if (disabled) status = 'disabled';else if (isComplete) status = 'complete';else if (isSelected) status = 'current';
  var classes = classNames('euiStepHorizontal', className, {
    'euiStepHorizontal-isSelected': status === 'current',
    'euiStepHorizontal-isComplete': status === 'complete',
    'euiStepHorizontal-isIncomplete': status === 'incomplete',
    'euiStepHorizontal-isDisabled': status === 'disabled'
  });
  var stepTitle = buttonTitle;
  if (status === 'disabled') stepTitle = disabledTitle;
  if (status === 'complete') stepTitle = completeTitle;
  if (status === 'incomplete') stepTitle = incompleteTitle;
  if (status === 'warning') stepTitle = warningTitle;
  if (status === 'current') stepTitle = currentTitle;

  var onStepClick = function onStepClick(event) {
    if (!disabled) onClick(event);
  };

  return ___EmotionJSX("button", _extends({
    className: classes,
    title: stepTitle,
    onClick: onStepClick,
    disabled: disabled
  }, rest), ___EmotionJSX(EuiStepNumber, {
    className: "euiStepHorizontal__number",
    status: status,
    number: step
  }), ___EmotionJSX("span", {
    className: "euiStepHorizontal__title"
  }, title));
};
EuiStepHorizontal.propTypes = {
  /**
     * **DEPRECATED: Use `status = 'current'` instead**
     * Adds to the line before the indicator for showing current progress
     */
  isSelected: PropTypes.bool,

  /**
     * **DEPRECATED: Use `status = 'complete'` instead**
     * Adds to the line after the indicator for showing current progress
     */
  isComplete: PropTypes.bool,
  onClick: PropTypes.func.isRequired,

  /**
     * Makes the whole step button disabled.
     */
  disabled: PropTypes.bool,

  /**
     * The number of the step in the list of steps
     */
  step: PropTypes.number,
  title: PropTypes.string,

  /**
     * Visual representation of the step number indicator.
     * May replace the number provided in props.step with alternate styling.
     * The `isSelected`, `isComplete`, and `disabled` props will override these.
     */
  status: PropTypes.any,
  className: PropTypes.string,
  "aria-label": PropTypes.string,
  "data-test-subj": PropTypes.string
};