import _slicedToArray from "@babel/runtime/helpers/slicedToArray";
import _extends from "@babel/runtime/helpers/extends";
import _objectWithoutProperties from "@babel/runtime/helpers/objectWithoutProperties";

/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */
import React, { Fragment } from 'react';
import classNames from 'classnames';
import { EuiButtonIcon } from '../button';
import { EuiIcon } from '../icon';
import { EuiToolTip } from '../tool_tip';
import { useInnerText } from '../inner_text';
import { getSecureRelForTarget } from '../../services';
import { validateHref } from '../../services/security/href_validator';
import { jsx as ___EmotionJSX } from "@emotion/react";
var sizeToClassNameMap = {
  xs: 'euiListGroupItem--xSmall',
  s: 'euiListGroupItem--small',
  m: 'euiListGroupItem--medium',
  l: 'euiListGroupItem--large'
};
export var SIZES = Object.keys(sizeToClassNameMap);
var colorToClassNameMap = {
  inherit: '',
  primary: 'euiListGroupItem--primary',
  text: 'euiListGroupItem--text',
  subdued: 'euiListGroupItem--subdued',
  ghost: 'euiListGroupItem--ghost'
};
export var COLORS = Object.keys(colorToClassNameMap);
export var EuiListGroupItem = function EuiListGroupItem(_ref) {
  var label = _ref.label,
      _ref$isActive = _ref.isActive,
      isActive = _ref$isActive === void 0 ? false : _ref$isActive,
      _ref$isDisabled = _ref.isDisabled,
      _isDisabled = _ref$isDisabled === void 0 ? false : _ref$isDisabled,
      href = _ref.href,
      target = _ref.target,
      rel = _ref.rel,
      className = _ref.className,
      iconType = _ref.iconType,
      icon = _ref.icon,
      iconProps = _ref.iconProps,
      extraAction = _ref.extraAction,
      onClick = _ref.onClick,
      _ref$size = _ref.size,
      size = _ref$size === void 0 ? 'm' : _ref$size,
      _ref$color = _ref.color,
      color = _ref$color === void 0 ? 'inherit' : _ref$color,
      _ref$showToolTip = _ref.showToolTip,
      showToolTip = _ref$showToolTip === void 0 ? false : _ref$showToolTip,
      wrapText = _ref.wrapText,
      buttonRef = _ref.buttonRef,
      rest = _objectWithoutProperties(_ref, ["label", "isActive", "isDisabled", "href", "target", "rel", "className", "iconType", "icon", "iconProps", "extraAction", "onClick", "size", "color", "showToolTip", "wrapText", "buttonRef"]);

  var isHrefValid = !href || validateHref(href);
  var isDisabled = _isDisabled || !isHrefValid;
  var classes = classNames('euiListGroupItem', sizeToClassNameMap[size], colorToClassNameMap[color], {
    'euiListGroupItem-isActive': isActive,
    'euiListGroupItem-isDisabled': isDisabled,
    'euiListGroupItem-isClickable': href || onClick,
    'euiListGroupItem-hasExtraAction': extraAction,
    'euiListGroupItem--wrapText': wrapText
  }, className);
  var iconNode;

  if (iconType) {
    iconNode = ___EmotionJSX(EuiIcon, _extends({
      color: "inherit" // forces the icon to inherit its parent color

    }, iconProps, {
      type: iconType,
      className: classNames('euiListGroupItem__icon', iconProps === null || iconProps === void 0 ? void 0 : iconProps.className)
    }));

    if (icon) {
      console.warn('Both `iconType` and `icon` were passed to EuiListGroupItem but only one can exist. The `iconType` was used.');
    }
  } else if (icon) {
    iconNode = /*#__PURE__*/React.cloneElement(icon, {
      className: classNames('euiListGroupItem__icon', icon.props.className)
    });
  }

  var extraActionNode;

  if (extraAction) {
    var _iconType = extraAction.iconType,
        alwaysShow = extraAction.alwaysShow,
        _className = extraAction.className,
        actionIsDisabled = extraAction.isDisabled,
        _rest = _objectWithoutProperties(extraAction, ["iconType", "alwaysShow", "className", "isDisabled"]);

    var extraActionClasses = classNames('euiListGroupItem__extraAction', {
      'euiListGroupItem__extraAction-alwaysShow': alwaysShow
    }, _className);
    extraActionNode = ___EmotionJSX(EuiButtonIcon, _extends({
      className: extraActionClasses,
      iconType: _iconType
    }, _rest, {
      disabled: isDisabled || actionIsDisabled
    }));
  } // Only add the label as the title attribute if it's possibly truncated
  // Also ensure the value of the title attribute is a string


  var _useInnerText = useInnerText(),
      _useInnerText2 = _slicedToArray(_useInnerText, 2),
      ref = _useInnerText2[0],
      innerText = _useInnerText2[1];

  var shouldRenderTitle = !wrapText && !showToolTip;
  var labelContent = shouldRenderTitle ? ___EmotionJSX("span", {
    ref: ref,
    className: "euiListGroupItem__label",
    title: typeof label === 'string' ? label : innerText
  }, label) : ___EmotionJSX("span", {
    className: "euiListGroupItem__label"
  }, label); // Handle the variety of interaction behavior

  var itemContent;
  var secureRel = getSecureRelForTarget({
    href: href,
    rel: rel,
    target: target
  });

  if (href && !isDisabled) {
    itemContent = ___EmotionJSX("a", _extends({
      className: "euiListGroupItem__button",
      href: href,
      target: target,
      rel: secureRel,
      onClick: onClick
    }, rest), iconNode, labelContent);
  } else if (href && isDisabled || onClick) {
    itemContent = ___EmotionJSX("button", _extends({
      type: "button",
      className: "euiListGroupItem__button",
      disabled: isDisabled,
      onClick: onClick,
      ref: buttonRef
    }, rest), iconNode, labelContent);
  } else {
    itemContent = ___EmotionJSX("span", _extends({
      className: "euiListGroupItem__text"
    }, rest), iconNode, labelContent);
  }

  if (showToolTip) {
    itemContent = ___EmotionJSX("li", {
      className: classes
    }, ___EmotionJSX(EuiToolTip, {
      anchorClassName: "euiListGroupItem__tooltip",
      content: label,
      position: "right",
      delay: "long"
    }, itemContent));
  } else {
    itemContent = ___EmotionJSX("li", {
      className: classes
    }, itemContent, extraActionNode);
  }

  return ___EmotionJSX(Fragment, null, itemContent);
};