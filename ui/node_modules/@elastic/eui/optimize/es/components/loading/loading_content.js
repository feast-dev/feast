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
export var EuiLoadingContent = function EuiLoadingContent(_ref) {
  var _ref$lines = _ref.lines,
      lines = _ref$lines === void 0 ? 3 : _ref$lines,
      className = _ref.className,
      rest = _objectWithoutProperties(_ref, ["lines", "className"]);

  var classes = classNames('euiLoadingContent', className);
  var lineElements = [];

  for (var i = 0; i < lines; i++) {
    lineElements.push(___EmotionJSX("span", {
      key: i,
      className: "euiLoadingContent__singleLine"
    }, ___EmotionJSX("span", {
      className: "euiLoadingContent__singleLineBackground"
    })));
  }

  return ___EmotionJSX("span", _extends({
    className: classes
  }, rest), lineElements);
};