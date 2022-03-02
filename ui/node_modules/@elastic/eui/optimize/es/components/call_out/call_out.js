import _extends from "@babel/runtime/helpers/extends";
import _objectWithoutProperties from "@babel/runtime/helpers/objectWithoutProperties";

/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */
import React, { forwardRef } from 'react';
import classNames from 'classnames';
import { keysOf } from '../common';
import { EuiIcon } from '../icon';
import { EuiText } from '../text';
import { jsx as ___EmotionJSX } from "@emotion/react";
var colorToClassNameMap = {
  primary: 'euiCallOut--primary',
  success: 'euiCallOut--success',
  warning: 'euiCallOut--warning',
  danger: 'euiCallOut--danger'
};
export var COLORS = keysOf(colorToClassNameMap);
export var HEADINGS = ['h1', 'h2', 'h3', 'h4', 'h5', 'h6', 'p'];
var sizeToClassNameMap = {
  s: 'euiCallOut--small',
  m: ''
};
export var EuiCallOut = /*#__PURE__*/forwardRef(function (_ref, ref) {
  var title = _ref.title,
      _ref$color = _ref.color,
      color = _ref$color === void 0 ? 'primary' : _ref$color,
      _ref$size = _ref.size,
      size = _ref$size === void 0 ? 'm' : _ref$size,
      iconType = _ref.iconType,
      children = _ref.children,
      className = _ref.className,
      heading = _ref.heading,
      rest = _objectWithoutProperties(_ref, ["title", "color", "size", "iconType", "children", "className", "heading"]);

  var classes = classNames('euiCallOut', colorToClassNameMap[color], sizeToClassNameMap[size], className);
  var headerIcon;

  if (iconType) {
    headerIcon = ___EmotionJSX(EuiIcon, {
      className: "euiCallOutHeader__icon",
      type: iconType,
      size: "m",
      "aria-hidden": "true",
      color: "inherit" // forces the icon to inherit its parent color

    });
  }

  var optionalChildren;

  if (children && size === 's') {
    optionalChildren = ___EmotionJSX(EuiText, {
      size: "xs",
      color: "default"
    }, children);
  } else if (children) {
    optionalChildren = ___EmotionJSX(EuiText, {
      size: "s",
      color: "default"
    }, children);
  }

  var H = heading ? "".concat(heading) : 'span';
  var header;

  if (title) {
    header = ___EmotionJSX("div", {
      className: "euiCallOutHeader"
    }, headerIcon, ___EmotionJSX(H, {
      className: "euiCallOutHeader__title"
    }, title));
  }

  return ___EmotionJSX("div", _extends({
    className: classes,
    ref: ref
  }, rest), header, optionalChildren);
});
EuiCallOut.displayName = 'EuiCallOut';