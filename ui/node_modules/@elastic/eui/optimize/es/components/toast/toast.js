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
import { keysOf } from '../common';
import { EuiScreenReaderOnly } from '../accessibility';
import { EuiI18n } from '../i18n';
import { EuiIcon } from '../icon';
import { EuiText } from '../text';
import { jsx as ___EmotionJSX } from "@emotion/react";
var colorToClassNameMap = {
  primary: 'euiToast--primary',
  success: 'euiToast--success',
  warning: 'euiToast--warning',
  danger: 'euiToast--danger'
};
export var COLORS = keysOf(colorToClassNameMap);
export var EuiToast = function EuiToast(_ref) {
  var title = _ref.title,
      color = _ref.color,
      iconType = _ref.iconType,
      onClose = _ref.onClose,
      children = _ref.children,
      className = _ref.className,
      rest = _objectWithoutProperties(_ref, ["title", "color", "iconType", "onClose", "children", "className"]);

  var classes = classNames('euiToast', color ? colorToClassNameMap[color] : null, className);
  var headerClasses = classNames('euiToastHeader', {
    'euiToastHeader--withBody': children
  });
  var headerIcon;

  if (iconType) {
    headerIcon = ___EmotionJSX(EuiIcon, {
      className: "euiToastHeader__icon",
      type: iconType,
      size: "m",
      "aria-hidden": "true"
    });
  }

  var closeButton;

  if (onClose) {
    closeButton = ___EmotionJSX(EuiI18n, {
      token: "euiToast.dismissToast",
      default: "Dismiss toast"
    }, function (dismissToast) {
      return ___EmotionJSX("button", {
        type: "button",
        className: "euiToast__closeButton",
        "aria-label": dismissToast,
        onClick: onClose,
        "data-test-subj": "toastCloseButton"
      }, ___EmotionJSX(EuiIcon, {
        type: "cross",
        size: "m",
        "aria-hidden": "true"
      }));
    });
  }

  var optionalBody;

  if (children) {
    optionalBody = ___EmotionJSX(EuiText, {
      size: "s",
      className: "euiToastBody"
    }, children);
  }

  return ___EmotionJSX("div", _extends({
    className: classes
  }, rest), ___EmotionJSX(EuiScreenReaderOnly, null, ___EmotionJSX("p", null, ___EmotionJSX(EuiI18n, {
    token: "euiToast.newNotification",
    default: "A new notification appears"
  }))), ___EmotionJSX(EuiI18n, {
    token: "euiToast.notification",
    default: "Notification"
  }, function (notification) {
    return ___EmotionJSX("div", {
      className: headerClasses,
      "aria-label": notification,
      "data-test-subj": "euiToastHeader"
    }, headerIcon, ___EmotionJSX("span", {
      className: "euiToastHeader__title"
    }, title));
  }), closeButton, optionalBody);
};