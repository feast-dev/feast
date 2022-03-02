import _extends from "@babel/runtime/helpers/extends";
import _defineProperty from "@babel/runtime/helpers/defineProperty";
import _slicedToArray from "@babel/runtime/helpers/slicedToArray";
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
import React, { useState } from 'react';
import classNames from 'classnames';
import { EuiOverlayMask } from '../overlay_mask';
import { EuiIcon } from '../icon';
import { useEuiI18n } from '../i18n';
import { EuiFocusTrap } from '../focus_trap';
import { keys } from '../../services';
import { useInnerText } from '../inner_text';
import { jsx as ___EmotionJSX } from "@emotion/react";
var sizeToClassNameMap = {
  s: 'euiImage--small',
  m: 'euiImage--medium',
  l: 'euiImage--large',
  xl: 'euiImage--xlarge',
  fullWidth: 'euiImage--fullWidth',
  original: 'euiImage--original'
};
var marginToClassNameMap = {
  s: 'euiImage--marginSmall',
  m: 'euiImage--marginMedium',
  l: 'euiImage--marginLarge',
  xl: 'euiImage--marginXlarge'
};
var floatToClassNameMap = {
  left: 'euiImage--floatLeft',
  right: 'euiImage--floatRight'
};
export var SIZES = Object.keys(sizeToClassNameMap);
var fullScreenIconColorMap = {
  light: 'ghost',
  dark: 'default'
};
export var EuiImage = function EuiImage(_ref) {
  var className = _ref.className,
      url = _ref.url,
      src = _ref.src,
      _ref$size = _ref.size,
      size = _ref$size === void 0 ? 'original' : _ref$size,
      caption = _ref.caption,
      hasShadow = _ref.hasShadow,
      allowFullScreen = _ref.allowFullScreen,
      _ref$fullScreenIconCo = _ref.fullScreenIconColor,
      fullScreenIconColor = _ref$fullScreenIconCo === void 0 ? 'light' : _ref$fullScreenIconCo,
      alt = _ref.alt,
      style = _ref.style,
      float = _ref.float,
      margin = _ref.margin,
      rest = _objectWithoutProperties(_ref, ["className", "url", "src", "size", "caption", "hasShadow", "allowFullScreen", "fullScreenIconColor", "alt", "style", "float", "margin"]);

  var _useState = useState(false),
      _useState2 = _slicedToArray(_useState, 2),
      isFullScreenActive = _useState2[0],
      setIsFullScreenActive = _useState2[1];

  var onKeyDown = function onKeyDown(event) {
    if (event.key === keys.ESCAPE) {
      event.preventDefault();
      event.stopPropagation();
      closeFullScreen();
    }
  };

  var closeFullScreen = function closeFullScreen() {
    setIsFullScreenActive(false);
  };

  var openFullScreen = function openFullScreen() {
    setIsFullScreenActive(true);
  };

  var customStyle = _objectSpread({}, style);

  var classes = classNames('euiImage', {
    'euiImage--hasShadow': hasShadow,
    'euiImage--allowFullScreen': allowFullScreen
  }, margin ? marginToClassNameMap[margin] : null, float ? floatToClassNameMap[float] : null, className);

  if (typeof size === 'string' && SIZES.includes(size)) {
    classes = "".concat(classes, " ").concat(sizeToClassNameMap[size]);
  } else {
    classes = "".concat(classes);
    customStyle.maxWidth = size;
    customStyle.maxHeight = size; // Set width back to auto to ensure aspect ratio is kept

    customStyle.width = 'auto';
  }

  var allowFullScreenButtonClasses = 'euiImage__button'; // when the button is not custom we need it to go full width
  // to match the parent '.euiImage' width except when the size is original

  if (typeof size === 'string' && size !== 'original' && SIZES.includes(size)) {
    allowFullScreenButtonClasses = "".concat(allowFullScreenButtonClasses, " euiImage__button--fullWidth");
  } else {
    allowFullScreenButtonClasses = "".concat(allowFullScreenButtonClasses);
  }

  var _useInnerText = useInnerText(),
      _useInnerText2 = _slicedToArray(_useInnerText, 2),
      optionalCaptionRef = _useInnerText2[0],
      optionalCaptionText = _useInnerText2[1];

  var optionalCaption;

  if (caption) {
    optionalCaption = ___EmotionJSX("figcaption", {
      ref: optionalCaptionRef,
      className: "euiImage__caption"
    }, caption);
  }

  var allowFullScreenIcon = ___EmotionJSX(EuiIcon, {
    type: "fullScreen",
    color: fullScreenIconColorMap[fullScreenIconColor],
    className: "euiImage__icon"
  });

  var fullScreenDisplay = ___EmotionJSX(EuiOverlayMask, {
    "data-test-subj": "fullScreenOverlayMask",
    onClick: closeFullScreen
  }, ___EmotionJSX(EuiFocusTrap, {
    clickOutsideDisables: true
  }, ___EmotionJSX(React.Fragment, null, ___EmotionJSX("figure", {
    className: "euiImage euiImage-isFullScreen",
    "aria-label": optionalCaptionText
  }, ___EmotionJSX("button", {
    type: "button",
    "aria-label": useEuiI18n('euiImage.closeImage', 'Close full screen {alt} image', {
      alt: alt
    }),
    className: "euiImage__button",
    "data-test-subj": "deactivateFullScreenButton",
    onClick: closeFullScreen,
    onKeyDown: onKeyDown
  }, ___EmotionJSX("img", _extends({
    src: src || url,
    alt: alt,
    className: "euiImage-isFullScreen__img"
  }, rest))), optionalCaption), ___EmotionJSX(EuiIcon, {
    type: "cross",
    color: "default",
    className: "euiImage-isFullScreenCloseIcon"
  }))));

  var fullscreenLabel = useEuiI18n('euiImage.openImage', 'Open full screen {alt} image', {
    alt: alt
  });

  if (allowFullScreen) {
    return ___EmotionJSX("figure", {
      className: classes,
      "aria-label": optionalCaptionText
    }, ___EmotionJSX("button", {
      type: "button",
      "aria-label": fullscreenLabel,
      className: allowFullScreenButtonClasses,
      "data-test-subj": "activateFullScreenButton",
      onClick: openFullScreen
    }, ___EmotionJSX("img", _extends({
      style: customStyle,
      src: src || url,
      alt: alt,
      className: "euiImage__img"
    }, rest)), allowFullScreenIcon), isFullScreenActive && fullScreenDisplay, optionalCaption);
  } else {
    return ___EmotionJSX("figure", {
      className: classes,
      "aria-label": optionalCaptionText
    }, ___EmotionJSX("img", _extends({
      style: customStyle,
      src: src || url,
      className: "euiImage__img",
      alt: alt
    }, rest)), optionalCaption);
  }
};