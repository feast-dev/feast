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
import React, { Fragment } from 'react';
import classNames from 'classnames';
import { useEuiI18n } from '../i18n';
import { EuiNotificationBadge } from '../badge/notification_badge';
import { EuiButtonEmpty } from '../button/button_empty';
import { useInnerText } from '../inner_text';
import { jsx as ___EmotionJSX } from "@emotion/react";
export var EuiFilterButton = function EuiFilterButton(_ref) {
  var children = _ref.children,
      className = _ref.className,
      iconType = _ref.iconType,
      _ref$iconSide = _ref.iconSide,
      iconSide = _ref$iconSide === void 0 ? 'right' : _ref$iconSide,
      _ref$color = _ref.color,
      color = _ref$color === void 0 ? 'text' : _ref$color,
      hasActiveFilters = _ref.hasActiveFilters,
      numFilters = _ref.numFilters,
      numActiveFilters = _ref.numActiveFilters,
      isDisabled = _ref.isDisabled,
      isSelected = _ref.isSelected,
      _ref$type = _ref.type,
      type = _ref$type === void 0 ? 'button' : _ref$type,
      _ref$grow = _ref.grow,
      grow = _ref$grow === void 0 ? true : _ref$grow,
      noDivider = _ref.noDivider,
      withNext = _ref.withNext,
      textProps = _ref.textProps,
      rest = _objectWithoutProperties(_ref, ["children", "className", "iconType", "iconSide", "color", "hasActiveFilters", "numFilters", "numActiveFilters", "isDisabled", "isSelected", "type", "grow", "noDivider", "withNext", "textProps"]);

  var numFiltersDefined = numFilters != null; // != instead of !== to allow for null and undefined

  var numActiveFiltersDefined = numActiveFilters != null && numActiveFilters > 0;
  var classes = classNames('euiFilterButton', {
    'euiFilterButton-isSelected': isSelected,
    'euiFilterButton-hasActiveFilters': hasActiveFilters,
    'euiFilterButton-hasNotification': numFiltersDefined,
    'euiFilterButton--hasIcon': iconType,
    'euiFilterButton--noGrow': !grow,
    'euiFilterButton--withNext': noDivider || withNext
  }, className);
  var buttonTextClassNames = classNames({
    'euiFilterButton__text-hasNotification': numFiltersDefined || numActiveFilters
  }, textProps && textProps.className);
  var showBadge = numFiltersDefined || numActiveFiltersDefined;
  var badgeCount = numActiveFilters || numFilters;
  var activeBadgeLabel = useEuiI18n('euiFilterButton.filterBadgeActiveAriaLabel', '{count} active filters', {
    count: badgeCount
  });
  var availableBadgeLabel = useEuiI18n('euiFilterButton.filterBadgeAvailableAriaLabel', '{count} available filters', {
    count: badgeCount
  });

  var badgeContent = showBadge && ___EmotionJSX(EuiNotificationBadge, {
    className: "euiFilterButton__notification",
    size: "m",
    "aria-label": hasActiveFilters ? activeBadgeLabel : availableBadgeLabel,
    color: isDisabled || !hasActiveFilters ? 'subdued' : 'accent'
  }, badgeCount);

  var dataText;

  if (typeof children === 'string') {
    dataText = children;
  }

  var _useInnerText = useInnerText(),
      _useInnerText2 = _slicedToArray(_useInnerText, 2),
      ref = _useInnerText2[0],
      innerText = _useInnerText2[1];

  var buttonContents = ___EmotionJSX(Fragment, null, ___EmotionJSX("span", {
    ref: ref,
    className: "euiFilterButton__textShift",
    "data-text": dataText || innerText,
    title: dataText || innerText
  }, children), badgeContent);

  return ___EmotionJSX(EuiButtonEmpty, _extends({
    className: classes,
    color: color,
    isDisabled: isDisabled,
    iconSide: iconSide,
    iconType: iconType,
    type: type,
    textProps: _objectSpread(_objectSpread({}, textProps), {}, {
      className: buttonTextClassNames
    })
  }, rest), buttonContents);
};