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
import { EuiLoadingSpinner } from '../loading';
import { EuiIcon } from '../icon';
import { jsx as ___EmotionJSX } from "@emotion/react";
var iconSideToClassNameMap = {
  left: null,
  right: 'euiButtonContent--iconRight'
};
export var ICON_SIDES = keysOf(iconSideToClassNameMap);
export var EuiButtonContent = function EuiButtonContent(_ref) {
  var children = _ref.children,
      textProps = _ref.textProps,
      _ref$isLoading = _ref.isLoading,
      isLoading = _ref$isLoading === void 0 ? false : _ref$isLoading,
      iconType = _ref.iconType,
      _ref$iconSize = _ref.iconSize,
      iconSize = _ref$iconSize === void 0 ? 'm' : _ref$iconSize,
      _ref$iconSide = _ref.iconSide,
      iconSide = _ref$iconSide === void 0 ? 'left' : _ref$iconSide,
      contentProps = _objectWithoutProperties(_ref, ["children", "textProps", "isLoading", "iconType", "iconSize", "iconSide"]);

  // Add an icon to the button if one exists.
  var buttonIcon;

  if (isLoading) {
    buttonIcon = ___EmotionJSX(EuiLoadingSpinner, {
      className: "euiButtonContent__spinner",
      size: iconSize
    });
  } else if (iconType) {
    buttonIcon = ___EmotionJSX(EuiIcon, {
      className: "euiButtonContent__icon",
      type: iconType,
      size: iconSize,
      color: "inherit" // forces the icon to inherit its parent color

    });
  }

  var contentClassNames = classNames('euiButtonContent', iconSide ? iconSideToClassNameMap[iconSide] : null, contentProps && contentProps.className);
  return ___EmotionJSX("span", _extends({}, contentProps, {
    className: contentClassNames
  }), buttonIcon, ___EmotionJSX("span", textProps, children));
};