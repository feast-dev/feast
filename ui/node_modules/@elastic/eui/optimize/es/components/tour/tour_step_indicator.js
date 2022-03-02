import _extends from "@babel/runtime/helpers/extends";
import _objectWithoutProperties from "@babel/runtime/helpers/objectWithoutProperties";

/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */
import React from 'react';
import classNames from 'classnames';
import { keysOf } from '../common';
import { EuiIcon } from '../icon';
import { EuiI18n } from '../i18n';
import { jsx as ___EmotionJSX } from "@emotion/react";
var statusToClassNameMap = {
  complete: 'euiTourStepIndicator--complete',
  incomplete: 'euiTourStepIndicator--incomplete',
  active: 'euiTourStepIndicator--active'
};
export var STATUS = keysOf(statusToClassNameMap);
export var EuiTourStepIndicator = function EuiTourStepIndicator(_ref) {
  var className = _ref.className,
      number = _ref.number,
      status = _ref.status,
      rest = _objectWithoutProperties(_ref, ["className", "number", "status"]);

  var classes = classNames('euiTourStepIndicator', status ? statusToClassNameMap[status] : undefined, className);
  var indicatorIcon;

  if (status === 'active') {
    indicatorIcon = ___EmotionJSX(EuiI18n, {
      token: "euiTourStepIndicator.isActive",
      default: "active"
    }, function (isActive) {
      return ___EmotionJSX(EuiIcon, {
        type: "dot",
        className: "euiStepNumber__icon",
        "aria-label": isActive,
        color: "success",
        "aria-current": "step"
      });
    });
  } else if (status === 'complete') {
    indicatorIcon = ___EmotionJSX(EuiI18n, {
      token: "euiTourStepIndicator.isComplete",
      default: "complete"
    }, function (isComplete) {
      return ___EmotionJSX(EuiIcon, {
        type: "dot",
        className: "euiStepNumber__icon",
        "aria-label": isComplete,
        color: "subdued"
      });
    });
  } else if (status === 'incomplete') {
    indicatorIcon = ___EmotionJSX(EuiI18n, {
      token: "euiTourStepIndicator.isIncomplete",
      default: "incomplete"
    }, function (isIncomplete) {
      return ___EmotionJSX(EuiIcon, {
        type: "dot",
        className: "euiStepNumber__icon",
        "aria-label": isIncomplete,
        color: "subdued"
      });
    });
  }

  return ___EmotionJSX(EuiI18n, {
    token: "euiTourStepIndicator.ariaLabel",
    default: "Step {number} {status}",
    values: {
      status: status,
      number: number
    }
  }, function (ariaLabel) {
    return ___EmotionJSX("li", _extends({
      className: classes,
      "aria-label": ariaLabel
    }, rest), indicatorIcon);
  });
};