"use strict";

var _interopRequireWildcard = require("@babel/runtime/helpers/interopRequireWildcard");

var _interopRequireDefault = require("@babel/runtime/helpers/interopRequireDefault");

Object.defineProperty(exports, "__esModule", {
  value: true
});
exports.EuiHeaderSectionItemButton = void 0;

var _extends2 = _interopRequireDefault(require("@babel/runtime/helpers/extends"));

var _objectWithoutProperties2 = _interopRequireDefault(require("@babel/runtime/helpers/objectWithoutProperties"));

var _react = _interopRequireWildcard(require("react"));

var _classnames = _interopRequireDefault(require("classnames"));

var _badge_notification = require("../../badge/notification_badge/badge_notification");

var _icon = require("../../icon");

var _button = require("../../button");

var _responsive = require("../../responsive");

var _react2 = require("@emotion/react");

/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */
var EuiHeaderSectionItemButton = /*#__PURE__*/(0, _react.forwardRef)(function (_ref,
/**
 * Allows for animating with .euiAnimate()
 */
ref) {
  var children = _ref.children,
      className = _ref.className,
      notification = _ref.notification,
      _ref$notificationColo = _ref.notificationColor,
      notificationColor = _ref$notificationColo === void 0 ? 'accent' : _ref$notificationColo,
      rest = (0, _objectWithoutProperties2.default)(_ref, ["children", "className", "notification", "notificationColor"]);
  var buttonRef = (0, _react.useRef)(null);
  var animationTargetRef = (0, _react.useRef)(null);
  (0, _react.useImperativeHandle)(ref, function () {
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
  var classes = (0, _classnames.default)('euiHeaderSectionItemButton', className);
  var animationClasses = (0, _classnames.default)(['euiHeaderSectionItemButton__content']);
  var notificationDot = (0, _react2.jsx)(_icon.EuiIcon, {
    className: "euiHeaderSectionItemButton__notification euiHeaderSectionItemButton__notification--dot",
    color: notificationColor,
    type: "dot",
    size: "l"
  });
  var buttonNotification;

  if (notification === true) {
    buttonNotification = notificationDot;
  } else if (notification) {
    buttonNotification = (0, _react2.jsx)(_react.default.Fragment, null, (0, _react2.jsx)(_responsive.EuiHideFor, {
      sizes: ['xs']
    }, (0, _react2.jsx)(_badge_notification.EuiNotificationBadge, {
      className: "euiHeaderSectionItemButton__notification euiHeaderSectionItemButton__notification--badge",
      color: notificationColor
    }, notification)), (0, _react2.jsx)(_responsive.EuiShowFor, {
      sizes: ['xs']
    }, notificationDot));
  }

  return (0, _react2.jsx)(_button.EuiButtonEmpty, (0, _extends2.default)({
    className: classes,
    color: "text",
    buttonRef: buttonRef
  }, rest), (0, _react2.jsx)("span", {
    ref: animationTargetRef,
    className: animationClasses
  }, children), buttonNotification);
});
exports.EuiHeaderSectionItemButton = EuiHeaderSectionItemButton;
EuiHeaderSectionItemButton.displayName = 'EuiHeaderSectionItemButton';