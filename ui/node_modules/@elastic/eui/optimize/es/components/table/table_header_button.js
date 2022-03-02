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
import { EuiInnerText } from '../inner_text';
import { EuiIcon } from '../icon';
import { jsx as ___EmotionJSX } from "@emotion/react";
export var EuiTableHeaderButton = function EuiTableHeaderButton(_ref) {
  var children = _ref.children,
      className = _ref.className,
      iconType = _ref.iconType,
      rest = _objectWithoutProperties(_ref, ["children", "className", "iconType"]);

  var classes = classNames('euiTableHeaderButton', className); // Add an icon to the button if one exists.

  var buttonIcon;

  if (iconType) {
    buttonIcon = ___EmotionJSX(EuiIcon, {
      className: "euiTableHeaderButton__icon",
      type: iconType,
      size: "m",
      "aria-hidden": "true"
    });
  }

  return ___EmotionJSX("button", _extends({
    type: "button",
    className: classes
  }, rest), ___EmotionJSX(EuiInnerText, null, function (ref, innerText) {
    return ___EmotionJSX("span", {
      title: innerText,
      ref: ref
    }, children);
  }), buttonIcon);
};