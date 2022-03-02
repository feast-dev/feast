import _extends from "@babel/runtime/helpers/extends";
import _objectWithoutProperties from "@babel/runtime/helpers/objectWithoutProperties";

/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */
import classNames from 'classnames';
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