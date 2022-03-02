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
EuiTourStepIndicator.propTypes = {
  number: PropTypes.number.isRequired,
  status: PropTypes.oneOf(["complete", "incomplete", "active"]).isRequired,
  className: PropTypes.string,
  "aria-label": PropTypes.string,
  "data-test-subj": PropTypes.string
};