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
import { jsx as ___EmotionJSX } from "@emotion/react";
export var EuiFormLabel = function EuiFormLabel(_ref) {
  var _ref$type = _ref.type,
      type = _ref$type === void 0 ? 'label' : _ref$type,
      isFocused = _ref.isFocused,
      isInvalid = _ref.isInvalid,
      isDisabled = _ref.isDisabled,
      children = _ref.children,
      className = _ref.className,
      rest = _objectWithoutProperties(_ref, ["type", "isFocused", "isInvalid", "isDisabled", "children", "className"]);

  var classes = classNames('euiFormLabel', className, {
    'euiFormLabel-isFocused': isFocused,
    'euiFormLabel-isInvalid': isInvalid,
    'euiFormLabel-isDisabled': isDisabled
  });

  if (type === 'legend') {
    return ___EmotionJSX("legend", _extends({
      className: classes
    }, rest), children);
  } else {
    return ___EmotionJSX("label", _extends({
      className: classes
    }, rest), children);
  }
};