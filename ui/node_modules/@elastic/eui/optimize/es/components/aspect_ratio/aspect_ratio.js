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
export var EuiAspectRatio = function EuiAspectRatio(_ref) {
  var children = _ref.children,
      className = _ref.className,
      height = _ref.height,
      width = _ref.width,
      maxWidth = _ref.maxWidth,
      rest = _objectWithoutProperties(_ref, ["children", "className", "height", "width", "maxWidth"]);

  var classes = classNames('euiAspectRatio', className);
  var paddingBottom = "".concat(height / width * 100, "%");

  var content = ___EmotionJSX("div", _extends({
    className: classes
  }, rest, {
    style: {
      paddingBottom: paddingBottom,
      maxWidth: maxWidth ? maxWidth : 'auto'
    }
  }), children);

  var contentwithoptionalwrap = content;

  if (maxWidth) {
    contentwithoptionalwrap = ___EmotionJSX("div", {
      style: {
        maxWidth: maxWidth
      }
    }, content);
  }

  return contentwithoptionalwrap;
};