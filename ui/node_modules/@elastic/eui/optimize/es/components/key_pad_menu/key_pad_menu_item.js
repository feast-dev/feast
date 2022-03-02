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
import { EuiBetaBadge } from '../badge/beta_badge';
import { getSecureRelForTarget, useGeneratedHtmlId } from '../../services';
import { EuiRadio, EuiCheckbox } from '../form';
import { validateHref } from '../../services/security/href_validator';
import { EuiToolTip } from '../tool_tip';
import { jsx as ___EmotionJSX } from "@emotion/react";
export var EuiKeyPadMenuItem = function EuiKeyPadMenuItem(_ref) {
  var id = _ref.id,
      isSelected = _ref.isSelected,
      _isDisabled = _ref.isDisabled,
      label = _ref.label,
      children = _ref.children,
      className = _ref.className,
      betaBadgeLabel = _ref.betaBadgeLabel,
      betaBadgeTooltipContent = _ref.betaBadgeTooltipContent,
      betaBadgeIconType = _ref.betaBadgeIconType,
      betaBadgeTooltipProps = _ref.betaBadgeTooltipProps,
      href = _ref.href,
      rel = _ref.rel,
      target = _ref.target,
      buttonRef = _ref.buttonRef,
      checkable = _ref.checkable,
      name = _ref.name,
      value = _ref.value,
      disabled = _ref.disabled,
      _onChange = _ref.onChange,
      rest = _objectWithoutProperties(_ref, ["id", "isSelected", "isDisabled", "label", "children", "className", "betaBadgeLabel", "betaBadgeTooltipContent", "betaBadgeIconType", "betaBadgeTooltipProps", "href", "rel", "target", "buttonRef", "checkable", "name", "value", "disabled", "onChange"]);

  var isHrefValid = !href || validateHref(href);
  var isDisabled = disabled || _isDisabled || !isHrefValid;
  var classes = classNames('euiKeyPadMenuItem', {
    'euiKeyPadMenuItem--hasBetaBadge': betaBadgeLabel,
    'euiKeyPadMenuItem--checkable': checkable,
    'euiKeyPadMenuItem-isDisabled': isDisabled,
    'euiKeyPadMenuItem-isSelected': isSelected
  }, className);
  var Element = href && !isDisabled ? 'a' : 'button';
  if (checkable) Element = 'label';
  var itemId = useGeneratedHtmlId({
    conditionalId: id
  });

  var renderCheckableElement = function renderCheckableElement() {
    if (!checkable) return;
    var inputClasses = classNames('euiKeyPadMenuItem__checkableInput');
    var checkableElement;

    if (checkable === 'single') {
      checkableElement = ___EmotionJSX(EuiRadio, {
        id: itemId,
        className: inputClasses,
        checked: isSelected,
        disabled: isDisabled,
        name: name,
        value: value,
        onChange: function onChange() {
          return _onChange(itemId, value);
        }
      });
    } else {
      checkableElement = ___EmotionJSX(EuiCheckbox, {
        id: itemId,
        className: inputClasses,
        checked: isSelected,
        disabled: isDisabled,
        name: name,
        onChange: function onChange() {
          return _onChange(itemId);
        }
      });
    }

    return checkableElement;
  };

  var renderBetaBadge = function renderBetaBadge() {
    if (!betaBadgeLabel) return;
    return ___EmotionJSX(EuiBetaBadge // Since we move the tooltip contents to a wrapping EuiToolTip,
    // this badge is purely visual therefore we can safely hide it from screen readers
    , {
      "aria-hidden": "true",
      size: "s",
      color: "subdued",
      className: "euiKeyPadMenuItem__betaBadge",
      label: betaBadgeLabel.charAt(0),
      iconType: betaBadgeIconType
    });
  };

  var relObj = {};

  if (href && !isDisabled) {
    relObj.href = href;
    relObj.rel = getSecureRelForTarget({
      href: href,
      target: target,
      rel: rel
    });
    relObj.target = target;
    relObj['aria-current'] = isSelected ? isSelected : undefined;
  } else if (checkable) {
    relObj.htmlFor = itemId;
  } else {
    relObj.disabled = isDisabled;
    relObj.type = 'button';
    relObj['aria-pressed'] = isSelected;
  }

  var button = ___EmotionJSX(Element, _extends({
    className: classes
  }, relObj, rest, {
    // Unable to get past `LegacyRef` conflicts
    ref: buttonRef
  }), ___EmotionJSX("span", {
    className: "euiKeyPadMenuItem__inner"
  }, checkable ? renderCheckableElement() : renderBetaBadge(), ___EmotionJSX("span", {
    className: "euiKeyPadMenuItem__icon"
  }, children), ___EmotionJSX("span", {
    className: "euiKeyPadMenuItem__label"
  }, label)));

  return betaBadgeLabel ? ___EmotionJSX(EuiToolTip, _extends({}, betaBadgeTooltipProps, {
    title: betaBadgeLabel,
    content: betaBadgeTooltipContent,
    delay: "long"
  }), button) : button;
};