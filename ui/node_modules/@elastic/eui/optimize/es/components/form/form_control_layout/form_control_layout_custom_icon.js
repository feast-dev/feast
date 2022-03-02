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
import { EuiIcon } from '../../icon';
import { jsx as ___EmotionJSX } from "@emotion/react";
export var EuiFormControlLayoutCustomIcon = function EuiFormControlLayoutCustomIcon(_ref) {
  var className = _ref.className,
      onClick = _ref.onClick,
      type = _ref.type,
      iconRef = _ref.iconRef,
      size = _ref.size,
      rest = _objectWithoutProperties(_ref, ["className", "onClick", "type", "iconRef", "size"]);

  var classes = classNames('euiFormControlLayoutCustomIcon', className, {
    'euiFormControlLayoutCustomIcon--clickable': onClick
  });

  if (onClick) {
    return ___EmotionJSX("button", _extends({
      type: "button",
      onClick: onClick,
      className: classes,
      ref: iconRef
    }, rest), ___EmotionJSX(EuiIcon, {
      className: "euiFormControlLayoutCustomIcon__icon",
      "aria-hidden": "true",
      size: size,
      type: type
    }));
  }

  return ___EmotionJSX("span", _extends({
    className: classes,
    ref: iconRef
  }, rest), ___EmotionJSX(EuiIcon, {
    className: "euiFormControlLayoutCustomIcon__icon",
    "aria-hidden": "true",
    size: size,
    type: type
  }));
};