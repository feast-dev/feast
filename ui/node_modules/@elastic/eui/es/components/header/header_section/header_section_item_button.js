function _extends() { _extends = Object.assign || function (target) { for (var i = 1; i < arguments.length; i++) { var source = arguments[i]; for (var key in source) { if (Object.prototype.hasOwnProperty.call(source, key)) { target[key] = source[key]; } } } return target; }; return _extends.apply(this, arguments); }

function _objectWithoutProperties(source, excluded) { if (source == null) return {}; var target = _objectWithoutPropertiesLoose(source, excluded); var key, i; if (Object.getOwnPropertySymbols) { var sourceSymbolKeys = Object.getOwnPropertySymbols(source); for (i = 0; i < sourceSymbolKeys.length; i++) { key = sourceSymbolKeys[i]; if (excluded.indexOf(key) >= 0) continue; if (!Object.prototype.propertyIsEnumerable.call(source, key)) continue; target[key] = source[key]; } } return target; }

function _objectWithoutPropertiesLoose(source, excluded) { if (source == null) return {}; var target = {}; var sourceKeys = Object.keys(source); var key, i; for (i = 0; i < sourceKeys.length; i++) { key = sourceKeys[i]; if (excluded.indexOf(key) >= 0) continue; target[key] = source[key]; } return target; }

/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */
import React, { forwardRef, useImperativeHandle, useRef } from 'react';
import classNames from 'classnames';
import { EuiNotificationBadge } from '../../badge/notification_badge/badge_notification';
import { EuiIcon } from '../../icon';
import { EuiButtonEmpty } from '../../button';
import { EuiHideFor, EuiShowFor } from '../../responsive';
import { jsx as ___EmotionJSX } from "@emotion/react";
export var EuiHeaderSectionItemButton = /*#__PURE__*/forwardRef(function (_ref,
/**
 * Allows for animating with .euiAnimate()
 */
ref) {
  var children = _ref.children,
      className = _ref.className,
      notification = _ref.notification,
      _ref$notificationColo = _ref.notificationColor,
      notificationColor = _ref$notificationColo === void 0 ? 'accent' : _ref$notificationColo,
      rest = _objectWithoutProperties(_ref, ["children", "className", "notification", "notificationColor"]);

  var buttonRef = useRef(null);
  var animationTargetRef = useRef(null);
  useImperativeHandle(ref, function () {
    buttonRef.current.euiAnimate = function () {
      var _animationTargetRef$c;

      var keyframes = [{
        transform: 'rotate(0)',
        offset: 0,
        easing: 'ease-in-out'
      }, {
        transform: 'rotate(30deg)',
        offset: 0.01,
        easing: 'ease-in-out'
      }, {
        transform: 'rotate(-28deg)',
        offset: 0.03,
        easing: 'ease-in-out'
      }, {
        transform: 'rotate(34deg)',
        offset: 0.05,
        easing: 'ease-in-out'
      }, {
        transform: 'rotate(-32deg)',
        offset: 0.07,
        easing: 'ease-in-out'
      }, {
        transform: 'rotate(30deg)',
        offset: 0.09,
        easing: 'ease-in-out'
      }, {
        transform: 'rotate(-28deg)',
        offset: 0.11,
        easing: 'ease-in-out'
      }, {
        transform: 'rotate(26deg)',
        offset: 0.13,
        easing: 'ease-in-out'
      }, {
        transform: 'rotate(-24deg)',
        offset: 0.15,
        easing: 'ease-in-out'
      }, {
        transform: 'rotate(22deg)',
        offset: 0.17,
        easing: 'ease-in-out'
      }, {
        transform: 'rotate(-20deg)',
        offset: 0.19,
        easing: 'ease-in-out'
      }, {
        transform: 'rotate(18deg)',
        offset: 0.21,
        easing: 'ease-in-out'
      }, {
        transform: 'rotate(-16deg)',
        offset: 0.23,
        easing: 'ease-in-out'
      }, {
        transform: 'rotate(14deg)',
        offset: 0.25,
        easing: 'ease-in-out'
      }, {
        transform: 'rotate(-12deg)',
        offset: 0.27,
        easing: 'ease-in-out'
      }, {
        transform: 'rotate(10deg)',
        offset: 0.29,
        easing: 'ease-in-out'
      }, {
        transform: 'rotate(-8deg)',
        offset: 0.31,
        easing: 'ease-in-out'
      }, {
        transform: 'rotate(6deg)',
        offset: 0.33,
        easing: 'ease-in-out'
      }, {
        transform: 'rotate(-4deg)',
        offset: 0.35,
        easing: 'ease-in-out'
      }, {
        transform: 'rotate(2deg)',
        offset: 0.37,
        easing: 'ease-in-out'
      }, {
        transform: 'rotate(-1deg)',
        offset: 0.39,
        easing: 'ease-in-out'
      }, {
        transform: 'rotate(1deg)',
        offset: 0.41,
        easing: 'ease-in-out'
      }, {
        transform: 'rotate(0)',
        offset: 0.43,
        easing: 'ease-in-out'
      }, {
        transform: 'rotate(0)',
        offset: 1,
        easing: 'ease-in-out'
      }];
      (_animationTargetRef$c = animationTargetRef.current) === null || _animationTargetRef$c === void 0 ? void 0 : _animationTargetRef$c.animate(keyframes, {
        duration: 5000
      });
    };

    return buttonRef.current;
  }, []);
  var classes = classNames('euiHeaderSectionItemButton', className);
  var animationClasses = classNames(['euiHeaderSectionItemButton__content']);

  var notificationDot = ___EmotionJSX(EuiIcon, {
    className: "euiHeaderSectionItemButton__notification euiHeaderSectionItemButton__notification--dot",
    color: notificationColor,
    type: "dot",
    size: "l"
  });

  var buttonNotification;

  if (notification === true) {
    buttonNotification = notificationDot;
  } else if (notification) {
    buttonNotification = ___EmotionJSX(React.Fragment, null, ___EmotionJSX(EuiHideFor, {
      sizes: ['xs']
    }, ___EmotionJSX(EuiNotificationBadge, {
      className: "euiHeaderSectionItemButton__notification euiHeaderSectionItemButton__notification--badge",
      color: notificationColor
    }, notification)), ___EmotionJSX(EuiShowFor, {
      sizes: ['xs']
    }, notificationDot));
  }

  return ___EmotionJSX(EuiButtonEmpty, _extends({
    className: classes,
    color: "text",
    buttonRef: buttonRef
  }, rest), ___EmotionJSX("span", {
    ref: animationTargetRef,
    className: animationClasses
  }, children), buttonNotification);
});
EuiHeaderSectionItemButton.displayName = 'EuiHeaderSectionItemButton';