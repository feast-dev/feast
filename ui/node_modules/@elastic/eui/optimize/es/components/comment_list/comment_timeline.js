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
import { EuiIcon } from '../icon';
import { jsx as ___EmotionJSX } from "@emotion/react";
var typeToClassNameMap = {
  regular: 'euiCommentTimeline__icon--regular',
  update: 'euiCommentTimeline__icon--update'
};
export var TYPES = keysOf(typeToClassNameMap);
export var EuiCommentTimeline = function EuiCommentTimeline(_ref) {
  var className = _ref.className,
      timelineIcon = _ref.timelineIcon,
      _ref$type = _ref.type,
      type = _ref$type === void 0 ? 'regular' : _ref$type;
  var classes = classNames('euiCommentTimeline', className);
  var iconClasses = classNames({
    'euiCommentTimeline__icon--default': !timelineIcon || typeof timelineIcon === 'string'
  }, typeToClassNameMap[type]);
  var iconRender;

  if (typeof timelineIcon === 'string') {
    iconRender = ___EmotionJSX(EuiIcon, {
      size: type === 'update' ? 'm' : 'l',
      type: timelineIcon
    });
  } else if (timelineIcon) {
    iconRender = timelineIcon;
  } else {
    iconRender = ___EmotionJSX(EuiIcon, {
      type: type === 'update' ? 'dot' : 'user',
      size: type === 'update' ? 's' : 'l'
    });
  }

  return ___EmotionJSX("div", {
    className: classes
  }, ___EmotionJSX("div", {
    className: "euiCommentTimeline__content"
  }, ___EmotionJSX("div", {
    className: iconClasses
  }, iconRender)));
};