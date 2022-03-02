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
import { EuiIcon } from '../icon';
import { EuiToolTip } from './tool_tip';
import { jsx as ___EmotionJSX } from "@emotion/react";
export var EuiIconTip = function EuiIconTip(_ref) {
  var _ref$type = _ref.type,
      type = _ref$type === void 0 ? 'questionInCircle' : _ref$type,
      _ref$ariaLabel = _ref['aria-label'],
      ariaLabel = _ref$ariaLabel === void 0 ? 'Info' : _ref$ariaLabel,
      color = _ref.color,
      size = _ref.size,
      iconProps = _ref.iconProps,
      _ref$position = _ref.position,
      position = _ref$position === void 0 ? 'top' : _ref$position,
      _ref$delay = _ref.delay,
      delay = _ref$delay === void 0 ? 'regular' : _ref$delay,
      rest = _objectWithoutProperties(_ref, ["type", "aria-label", "color", "size", "iconProps", "position", "delay"]);

  return ___EmotionJSX(EuiToolTip, _extends({
    position: position,
    delay: delay
  }, rest), ___EmotionJSX(EuiIcon, _extends({
    tabIndex: 0,
    type: type,
    color: color,
    size: size,
    "aria-label": ariaLabel
  }, iconProps)));
};