/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */
import React from 'react';
import PropTypes from "prop-types";
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
EuiCommentEvent.propTypes = {
  /**
     * Author of the comment. Display a small icon or avatar with it if needed.
     */
  username: PropTypes.node.isRequired,

  /**
     * Time of occurrence of the event. Its format is set on the consumer's side
     */
  timestamp: PropTypes.node,

  /**
     * Describes the event that took place
     */
  event: PropTypes.node,

  /**
     * Custom actions that the user can perform from the comment's header
     */
  actions: PropTypes.node,

  /**
     * Use "update" when the comment is primarily showing info about actions that the user or the system has performed (e.g. "user1 edited a case").
     */
  type: PropTypes.oneOf(["regular", "update"]),
  className: PropTypes.string,
  "aria-label": PropTypes.string,
  "data-test-subj": PropTypes.string
};