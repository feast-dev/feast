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
import { EuiComment } from './comment';
import { jsx as ___EmotionJSX } from "@emotion/react";
export var EuiCommentList = function EuiCommentList(_ref) {
  var children = _ref.children,
      className = _ref.className,
      comments = _ref.comments,
      rest = _objectWithoutProperties(_ref, ["children", "className", "comments"]);

  var classes = classNames('euiCommentList', className);
  var commentElements = null;

  if (comments) {
    commentElements = comments.map(function (item, index) {
      return ___EmotionJSX(EuiComment, _extends({
        key: index
      }, item));
    });
  }

  return ___EmotionJSX("div", _extends({
    className: classes
  }, rest), commentElements, children);
};