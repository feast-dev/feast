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
import { EuiNotificationBadge } from '../badge';
import { EuiLoadingSpinner } from '../loading';
import { EuiInnerText } from '../inner_text';
import { jsx as ___EmotionJSX } from "@emotion/react";
export var EuiFacetButton = function EuiFacetButton(_ref) {
  var children = _ref.children,
      className = _ref.className,
      icon = _ref.icon,
      _ref$isDisabled = _ref.isDisabled,
      isDisabled = _ref$isDisabled === void 0 ? false : _ref$isDisabled,
      _ref$isLoading = _ref.isLoading,
      isLoading = _ref$isLoading === void 0 ? false : _ref$isLoading,
      _ref$isSelected = _ref.isSelected,
      isSelected = _ref$isSelected === void 0 ? false : _ref$isSelected,
      quantity = _ref.quantity,
      buttonRef = _ref.buttonRef,
      rest = _objectWithoutProperties(_ref, ["children", "className", "icon", "isDisabled", "isLoading", "isSelected", "quantity", "buttonRef"]);

  // If in the loading state, force disabled to true
  isDisabled = isLoading ? true : isDisabled;
  var classes = classNames('euiFacetButton', {
    'euiFacetButton--isSelected': isSelected,
    'euiFacetButton--unSelected': !isSelected
  }, className); // Add quantity number if provided or loading indicator

  var buttonQuantity;

  if (isLoading) {
    buttonQuantity = ___EmotionJSX(EuiLoadingSpinner, {
      className: "euiFacetButton__spinner",
      size: "m"
    });
  } else if (typeof quantity === 'number') {
    buttonQuantity = ___EmotionJSX(EuiNotificationBadge, {
      className: "euiFacetButton__quantity",
      size: "m",
      color: !isSelected || isDisabled ? 'subdued' : 'accent'
    }, quantity);
  } // Add an icon to the button if one exists.


  var buttonIcon;

  if ( /*#__PURE__*/React.isValidElement(icon)) {
    buttonIcon = /*#__PURE__*/React.cloneElement(icon, {
      className: classNames(icon.props.className, 'euiFacetButton__icon')
    });
  }

  return ___EmotionJSX(EuiInnerText, null, function (ref, innerText) {
    return ___EmotionJSX("button", _extends({
      className: classes,
      disabled: isDisabled,
      type: "button",
      ref: buttonRef,
      title: rest['aria-label'] || innerText
    }, rest), ___EmotionJSX("span", {
      className: "euiFacetButton__content"
    }, buttonIcon, ___EmotionJSX("span", {
      className: "euiFacetButton__text",
      "data-text": innerText,
      ref: ref
    }, children), buttonQuantity));
  });
};