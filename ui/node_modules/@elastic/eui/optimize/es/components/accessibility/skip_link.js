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
import { EuiButton } from '../button/button';
import { EuiScreenReaderOnly } from '../accessibility/screen_reader';
import { jsx as ___EmotionJSX } from "@emotion/react";
export var POSITIONS = ['static', 'fixed', 'absolute'];
export var EuiSkipLink = function EuiSkipLink(_ref) {
  var destinationId = _ref.destinationId,
      tabIndex = _ref.tabIndex,
      _ref$position = _ref.position,
      position = _ref$position === void 0 ? 'static' : _ref$position,
      children = _ref.children,
      className = _ref.className,
      rest = _objectWithoutProperties(_ref, ["destinationId", "tabIndex", "position", "children", "className"]);

  var classes = classNames('euiSkipLink', ["euiSkipLink--".concat(position)], className); // Create the `href` from `destinationId`

  var optionalProps = {};

  if (destinationId) {
    optionalProps = {
      href: "#".concat(destinationId)
    };
  }

  return ___EmotionJSX(EuiScreenReaderOnly, {
    showOnFocus: true
  }, ___EmotionJSX(EuiButton, _extends({
    className: classes,
    tabIndex: position === 'fixed' ? 0 : tabIndex,
    size: "s",
    fill: true
  }, optionalProps, rest), children));
};