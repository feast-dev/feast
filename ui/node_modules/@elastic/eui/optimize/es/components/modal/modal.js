import _extends from "@babel/runtime/helpers/extends";
import _defineProperty from "@babel/runtime/helpers/defineProperty";
import _objectWithoutProperties from "@babel/runtime/helpers/objectWithoutProperties";

function ownKeys(object, enumerableOnly) { var keys = Object.keys(object); if (Object.getOwnPropertySymbols) { var symbols = Object.getOwnPropertySymbols(object); if (enumerableOnly) symbols = symbols.filter(function (sym) { return Object.getOwnPropertyDescriptor(object, sym).enumerable; }); keys.push.apply(keys, symbols); } return keys; }

function _objectSpread(target) { for (var i = 1; i < arguments.length; i++) { var source = arguments[i] != null ? arguments[i] : {}; if (i % 2) { ownKeys(Object(source), true).forEach(function (key) { _defineProperty(target, key, source[key]); }); } else if (Object.getOwnPropertyDescriptors) { Object.defineProperties(target, Object.getOwnPropertyDescriptors(source)); } else { ownKeys(Object(source)).forEach(function (key) { Object.defineProperty(target, key, Object.getOwnPropertyDescriptor(source, key)); }); } } return target; }

/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */
import React from 'react';
import classnames from 'classnames';
import { keys } from '../../services';
import { EuiButtonIcon } from '../button';
import { EuiFocusTrap } from '../focus_trap';
import { EuiOverlayMask } from '../overlay_mask';
import { EuiI18n } from '../i18n';
import { jsx as ___EmotionJSX } from "@emotion/react";
export var EuiModal = function EuiModal(_ref) {
  var className = _ref.className,
      children = _ref.children,
      initialFocus = _ref.initialFocus,
      onClose = _ref.onClose,
      _ref$maxWidth = _ref.maxWidth,
      maxWidth = _ref$maxWidth === void 0 ? true : _ref$maxWidth,
      style = _ref.style,
      rest = _objectWithoutProperties(_ref, ["className", "children", "initialFocus", "onClose", "maxWidth", "style"]);

  var onKeyDown = function onKeyDown(event) {
    if (event.key === keys.ESCAPE) {
      event.preventDefault();
      event.stopPropagation();
      onClose(event);
    }
  };

  var newStyle;
  var widthClassName;

  if (maxWidth === true) {
    widthClassName = 'euiModal--maxWidth-default';
  } else if (maxWidth !== false) {
    var value = typeof maxWidth === 'number' ? "".concat(maxWidth, "px") : maxWidth;
    newStyle = _objectSpread(_objectSpread({}, style), {}, {
      maxWidth: value
    });
  }

  var classes = classnames('euiModal', widthClassName, className);
  return ___EmotionJSX(EuiOverlayMask, null, ___EmotionJSX(EuiFocusTrap, {
    initialFocus: initialFocus
  }, ___EmotionJSX("div", _extends({
    className: classes,
    onKeyDown: onKeyDown,
    tabIndex: 0,
    style: newStyle || style
  }, rest), ___EmotionJSX(EuiI18n, {
    token: "euiModal.closeModal",
    default: "Closes this modal window"
  }, function (closeModal) {
    return ___EmotionJSX(EuiButtonIcon, {
      iconType: "cross",
      onClick: onClose,
      className: "euiModal__closeIcon",
      color: "text",
      "aria-label": closeModal
    });
  }), ___EmotionJSX("div", {
    className: "euiModal__flex"
  }, children))));
};