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
import { EuiCommentEvent } from './comment_event';
import { EuiCommentTimeline } from './comment_timeline';
import { jsx as ___EmotionJSX } from "@emotion/react";
var typeToClassNameMap = {
  regular: '',
  update: 'euiComment--update'
};
export var EuiComment = function EuiComment(_ref) {
  var children = _ref.children,
      className = _ref.className,
      username = _ref.username,
      event = _ref.event,
      actions = _ref.actions,
      timelineIcon = _ref.timelineIcon,
      _ref$type = _ref.type,
      type = _ref$type === void 0 ? 'regular' : _ref$type,
      timestamp = _ref.timestamp,
      rest = _objectWithoutProperties(_ref, ["children", "className", "username", "event", "actions", "timelineIcon", "type", "timestamp"]);

  var classes = classNames('euiComment', typeToClassNameMap[type], {
    'euiComment--hasBody': children
  }, className);
  return ___EmotionJSX("div", _extends({
    className: classes
  }, rest), ___EmotionJSX(EuiCommentTimeline, {
    type: type,
    timelineIcon: timelineIcon
  }), ___EmotionJSX(EuiCommentEvent, {
    username: username,
    actions: actions,
    event: event,
    timestamp: timestamp,
    type: type
  }, children));
};