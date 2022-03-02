import _extends from "@babel/runtime/helpers/extends";
import _objectWithoutProperties from "@babel/runtime/helpers/objectWithoutProperties";

/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */
import React, { Fragment, cloneElement } from 'react';
import classNames from 'classnames';
import { EuiText } from '../text';
import { jsx as ___EmotionJSX } from "@emotion/react";
export var EuiDatePickerRange = function EuiDatePickerRange(_ref) {
  var children = _ref.children,
      className = _ref.className,
      startDateControl = _ref.startDateControl,
      endDateControl = _ref.endDateControl,
      _ref$iconType = _ref.iconType,
      iconType = _ref$iconType === void 0 ? true : _ref$iconType,
      fullWidth = _ref.fullWidth,
      isCustom = _ref.isCustom,
      readOnly = _ref.readOnly,
      rest = _objectWithoutProperties(_ref, ["children", "className", "startDateControl", "endDateControl", "iconType", "fullWidth", "isCustom", "readOnly"]);

  var classes = classNames('euiDatePickerRange', {
    'euiDatePickerRange--fullWidth': fullWidth,
    'euiDatePickerRange--readOnly': readOnly
  }, className);
  var startControl = startDateControl;
  var endControl = endDateControl;

  if (!isCustom) {
    startControl = /*#__PURE__*/cloneElement(startDateControl, {
      fullWidth: fullWidth,
      readOnly: readOnly,
      iconType: typeof iconType === 'boolean' ? undefined : iconType,
      showIcon: !!iconType,
      className: classNames('euiDatePickerRange__start', startDateControl.props.className)
    });
    endControl = /*#__PURE__*/cloneElement(endDateControl, {
      showIcon: false,
      fullWidth: fullWidth,
      readOnly: readOnly,
      popoverPlacement: 'bottom-end',
      className: classNames('euiDatePickerRange__end', endDateControl.props.className)
    });
  }

  return ___EmotionJSX("div", _extends({
    className: classes
  }, rest), children ? children : ___EmotionJSX(Fragment, null, startControl, ___EmotionJSX(EuiText, {
    className: "euiDatePickerRange__delimeter",
    size: "s",
    color: "subdued"
  }, "\u2192"), endControl));
};