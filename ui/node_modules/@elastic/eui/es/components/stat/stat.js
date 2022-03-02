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
import React, { Fragment, createElement } from 'react';
import PropTypes from "prop-types";
import { keysOf } from '../common';
import classNames from 'classnames';
import { EuiText } from '../text';
import { EuiTitle } from '../title/title';
import { EuiScreenReaderOnly } from '../accessibility';
import { EuiI18n } from '../i18n';
import { jsx as ___EmotionJSX } from "@emotion/react";
var colorToClassNameMap = {
  default: null,
  subdued: 'euiStat__title--subdued',
  primary: 'euiStat__title--primary',
  success: 'euiStat__title--success',
  danger: 'euiStat__title--danger',
  accent: 'euiStat__title--accent'
};
export var COLORS = keysOf(colorToClassNameMap);
var textAlignToClassNameMap = {
  left: 'euiStat--leftAligned',
  center: 'euiStat--centerAligned',
  right: 'euiStat--rightAligned'
};
export var isColorClass = function isColorClass(input) {
  return colorToClassNameMap.hasOwnProperty(input);
};
export var ALIGNMENTS = keysOf(textAlignToClassNameMap);
export var EuiStat = function EuiStat(_ref) {
  var children = _ref.children,
      className = _ref.className,
      description = _ref.description,
      _ref$isLoading = _ref.isLoading,
      isLoading = _ref$isLoading === void 0 ? false : _ref$isLoading,
      _ref$reverse = _ref.reverse,
      reverse = _ref$reverse === void 0 ? false : _ref$reverse,
      _ref$textAlign = _ref.textAlign,
      textAlign = _ref$textAlign === void 0 ? 'left' : _ref$textAlign,
      title = _ref.title,
      _ref$titleColor = _ref.titleColor,
      titleColor = _ref$titleColor === void 0 ? 'default' : _ref$titleColor,
      _ref$titleSize = _ref.titleSize,
      titleSize = _ref$titleSize === void 0 ? 'l' : _ref$titleSize,
      _ref$titleElement = _ref.titleElement,
      titleElement = _ref$titleElement === void 0 ? 'p' : _ref$titleElement,
      _ref$descriptionEleme = _ref.descriptionElement,
      descriptionElement = _ref$descriptionEleme === void 0 ? 'p' : _ref$descriptionEleme,
      rest = _objectWithoutProperties(_ref, ["children", "className", "description", "isLoading", "reverse", "textAlign", "title", "titleColor", "titleSize", "titleElement", "descriptionElement"]);

  var classes = classNames('euiStat', textAlignToClassNameMap[textAlign], className);
  var titleClasses = classNames('euiStat__title', isColorClass(titleColor) ? colorToClassNameMap[titleColor] : null, {
    'euiStat__title-isLoading': isLoading
  });
  var commonProps = {
    'aria-hidden': true
  };

  var descriptionDisplay = ___EmotionJSX(EuiText, {
    size: "s",
    className: "euiStat__description"
  }, /*#__PURE__*/createElement(descriptionElement, commonProps, description));

  var titlePropsWithColor = {
    'aria-hidden': true,
    style: {
      color: "".concat(titleColor)
    }
  };
  var titleChildren = isLoading ? '--' : title;
  var titleDisplay = isColorClass(titleColor) ? ___EmotionJSX(EuiTitle, {
    size: titleSize,
    className: titleClasses
  }, /*#__PURE__*/createElement(titleElement, commonProps, titleChildren)) : ___EmotionJSX(EuiTitle, {
    size: titleSize,
    className: titleClasses
  }, /*#__PURE__*/createElement(titleElement, titlePropsWithColor, titleChildren));

  var screenReader = ___EmotionJSX(EuiScreenReaderOnly, null, ___EmotionJSX("p", null, isLoading ? ___EmotionJSX(EuiI18n, {
    token: "euiStat.loadingText",
    default: "Statistic is loading"
  }) : ___EmotionJSX(Fragment, null, reverse ? "".concat(title, " ").concat(description) : "".concat(description, " ").concat(title))));

  var statDisplay = ___EmotionJSX(Fragment, null, !reverse && descriptionDisplay, titleDisplay, reverse && descriptionDisplay, typeof title === 'string' && typeof description === 'string' && screenReader);

  return ___EmotionJSX("div", _extends({
    className: classes
  }, rest), statDisplay, children);
};
EuiStat.propTypes = {
  className: PropTypes.string,
  "aria-label": PropTypes.string,
  "data-test-subj": PropTypes.string,

  /**
     * Set the description (label) text
     */
  description: PropTypes.node.isRequired,

  /**
     * Will hide the title with an animation until false
     */
  isLoading: PropTypes.bool,

  /**
     * Flips the order of the description and title
     */
  reverse: PropTypes.bool,
  textAlign: PropTypes.oneOf(["left", "center", "right"]),

  /**
     * The (value) text
     */
  title: PropTypes.node.isRequired,

  /**
     * The color of the title text
     */
  titleColor: PropTypes.oneOfType([PropTypes.oneOf(["default", "subdued", "primary", "success", "danger", "accent"]).isRequired, PropTypes.string.isRequired]),

  /**
     * Size of the title. See EuiTitle for options ('s', 'm', 'l'... etc)
     */
  titleSize: PropTypes.oneOf(["xxxs", "xxs", "xs", "s", "m", "l"]),

  /**
     * HTML Element to be used for title
     */
  titleElement: PropTypes.string,

  /**
     * HTML Element to be used for description
     */
  descriptionElement: PropTypes.string
};