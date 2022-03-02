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
import { EuiPanel } from '../../panel/panel';
import { jsx as ___EmotionJSX } from "@emotion/react";
var verticalPositionToClassNameMap = {
  center: 'euiPageContent--verticalCenter'
};
var horizontalPositionToClassNameMap = {
  center: 'euiPageContent--horizontalCenter'
};
export var EuiPageContent = function EuiPageContent(_ref) {
  var verticalPosition = _ref.verticalPosition,
      horizontalPosition = _ref.horizontalPosition,
      _ref$paddingSize = _ref.paddingSize,
      paddingSize = _ref$paddingSize === void 0 ? 'l' : _ref$paddingSize,
      borderRadius = _ref.borderRadius,
      children = _ref.children,
      className = _ref.className,
      _ref$role = _ref.role,
      _role = _ref$role === void 0 ? 'main' : _ref$role,
      rest = _objectWithoutProperties(_ref, ["verticalPosition", "horizontalPosition", "paddingSize", "borderRadius", "children", "className", "role"]);

  var role = _role === null ? undefined : _role;
  var borderRadiusClass = borderRadius === 'none' ? 'euiPageContent--borderRadiusNone' : '';
  var classes = classNames('euiPageContent', borderRadiusClass, verticalPosition ? verticalPositionToClassNameMap[verticalPosition] : null, horizontalPosition ? horizontalPositionToClassNameMap[horizontalPosition] : null, className);
  return ___EmotionJSX(EuiPanel, _extends({
    className: classes,
    paddingSize: paddingSize,
    borderRadius: borderRadius,
    role: role
  }, rest), children);
};