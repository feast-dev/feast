import _extends from "@babel/runtime/helpers/extends";
import _objectWithoutProperties from "@babel/runtime/helpers/objectWithoutProperties";

/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */
import React, { cloneElement } from 'react';
import classNames from 'classnames';
import { EuiText } from '../../text';
import { EuiFormControlLayout } from './form_control_layout';
import { jsx as ___EmotionJSX } from "@emotion/react";
export var EuiFormControlLayoutDelimited = function EuiFormControlLayoutDelimited(_ref) {
  var startControl = _ref.startControl,
      endControl = _ref.endControl,
      _ref$delimiter = _ref.delimiter,
      delimiter = _ref$delimiter === void 0 ? '→' : _ref$delimiter,
      className = _ref.className,
      rest = _objectWithoutProperties(_ref, ["startControl", "endControl", "delimiter", "className"]);

  var classes = classNames('euiFormControlLayoutDelimited', className);
  return ___EmotionJSX(EuiFormControlLayout, _extends({
    className: classes
  }, rest), addClassesToControl(startControl), ___EmotionJSX(EuiText, {
    className: "euiFormControlLayoutDelimited__delimeter",
    size: "s",
    color: "subdued"
  }, delimiter), addClassesToControl(endControl));
};

function addClassesToControl(control) {
  return /*#__PURE__*/cloneElement(control, {
    className: classNames(control.props.className, 'euiFormControlLayoutDelimited__input')
  });
}