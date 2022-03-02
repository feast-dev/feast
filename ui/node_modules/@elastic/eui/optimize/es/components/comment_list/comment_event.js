/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */
import React from 'react';
import { keysOf } from '../common';
import classNames from 'classnames';
import { jsx as ___EmotionJSX } from "@emotion/react";
var typeToClassNameMap = {
  regular: 'euiCommentEvent--regular',
  update: 'euiCommentEvent--update'
};
export var TYPES = keysOf(typeToClassNameMap);
export var EuiCommentEvent = function EuiCommentEvent(_ref) {
  var children = _ref.children,
      className = _ref.className,
      username = _ref.username,
      timestamp = _ref.timestamp,
      _ref$type = _ref.type,
      type = _ref$type === void 0 ? 'regular' : _ref$type,
      event = _ref.event,
      actions = _ref.actions;
  var classes = classNames('euiCommentEvent', typeToClassNameMap[type], className);
  var isFigure = type === 'regular' || type === 'update' && typeof children !== 'undefined';
  var Element = isFigure ? 'figure' : 'div';
  var HeaderElement = isFigure ? 'figcaption' : 'div';
  return ___EmotionJSX(Element, {
    className: classes
  }, ___EmotionJSX(HeaderElement, {
    className: "euiCommentEvent__header"
  }, ___EmotionJSX("div", {
    className: "euiCommentEvent__headerData"
  }, ___EmotionJSX("div", {
    className: "euiCommentEvent__headerUsername"
  }, username), ___EmotionJSX("div", {
    className: "euiCommentEvent__headerEvent"
  }, event), timestamp ? ___EmotionJSX("div", {
    className: "euiCommentEvent__headerTimestamp"
  }, ___EmotionJSX("time", null, timestamp)) : undefined), actions ? ___EmotionJSX("div", {
    className: "euiCommentEvent__headerActions"
  }, actions) : undefined), children ? ___EmotionJSX("div", {
    className: "euiCommentEvent__body"
  }, children) : undefined);
};