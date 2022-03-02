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
import React, { cloneElement, useState, useEffect } from 'react';
import classNames from 'classnames';
import { EuiIcon } from '../icon';
import { getSecureRelForTarget } from '../../services';
import { validateHref } from '../../services/security/href_validator';
import { EuiInnerText } from '../inner_text';
/**
 * The props that are exposed to, or altered for, the consumer
 * for use in the object of items in `EuiSideNav`
 * can be found in the `side_nave_types.ts` file.
 */

import { jsx as ___EmotionJSX } from "@emotion/react";

var DefaultRenderItem = function DefaultRenderItem(_ref) {
  var href = _ref.href,
      target = _ref.target,
      rel = _ref.rel,
      onClick = _ref.onClick,
      className = _ref.className,
      children = _ref.children,
      disabled = _ref.disabled,
      rest = _objectWithoutProperties(_ref, ["href", "target", "rel", "onClick", "className", "children", "disabled"]);

  if (href && !disabled) {
    var secureRel = getSecureRelForTarget({
      href: href,
      rel: rel,
      target: target
    });
    return ___EmotionJSX("a", _extends({
      className: className,
      href: href,
      target: target,
      rel: secureRel,
      onClick: onClick
    }, rest), children);
  }

  if (onClick || disabled) {
    return ___EmotionJSX("button", _extends({
      type: "button",
      className: className,
      onClick: onClick,
      disabled: disabled
    }, rest), children);
  }

  return ___EmotionJSX("div", _extends({
    className: className
  }, rest), children);
};

export function EuiSideNavItem(_ref2) {
  var isOpen = _ref2.isOpen,
      isSelected = _ref2.isSelected,
      isParent = _ref2.isParent,
      icon = _ref2.icon,
      onClick = _ref2.onClick,
      _href = _ref2.href,
      rel = _ref2.rel,
      target = _ref2.target,
      items = _ref2.items,
      children = _ref2.children,
      _ref2$renderItem = _ref2.renderItem,
      RenderItem = _ref2$renderItem === void 0 ? DefaultRenderItem : _ref2$renderItem,
      _ref2$depth = _ref2.depth,
      depth = _ref2$depth === void 0 ? 0 : _ref2$depth,
      className = _ref2.className,
      _ref2$truncate = _ref2.truncate,
      truncate = _ref2$truncate === void 0 ? true : _ref2$truncate,
      emphasize = _ref2.emphasize,
      buttonClassName = _ref2.buttonClassName,
      childrenOnly = _ref2.childrenOnly,
      rest = _objectWithoutProperties(_ref2, ["isOpen", "isSelected", "isParent", "icon", "onClick", "href", "rel", "target", "items", "children", "renderItem", "depth", "className", "truncate", "emphasize", "buttonClassName", "childrenOnly"]);

  var isHrefValid = !_href || validateHref(_href);
  var href = isHrefValid ? _href : '';
  var isClickable = onClick || href; // Forcing accordion style item if not linked, but has children

  var _useState = useState(isOpen),
      _useState2 = _slicedToArray(_useState, 2),
      itemIsOpen = _useState2[0],
      setItemIsOpen = _useState2[1];

  useEffect(function () {
    setItemIsOpen(isOpen);
  }, [isOpen]);

  var toggleItemOpen = function toggleItemOpen() {
    setItemIsOpen(function (isOpen) {
      return !isOpen;
    });
  };

  var childItems;

  if (items && itemIsOpen) {
    childItems = ___EmotionJSX("div", {
      className: "euiSideNavItem__items"
    }, items);
  }

  var buttonIcon;

  if (icon) {
    buttonIcon = /*#__PURE__*/cloneElement(icon, {
      className: classNames('euiSideNavItemButton__icon', icon.props.className)
    });
  }

  var classes = classNames('euiSideNavItem', {
    'euiSideNavItem--root': depth === 0,
    'euiSideNavItem--rootIcon': depth === 0 && icon,
    'euiSideNavItem--trunk': depth === 1,
    'euiSideNavItem--branch': depth > 1,
    'euiSideNavItem--hasChildItems': !!childItems,
    'euiSideNavItem--emphasized': emphasize
  }, className);
  var buttonClasses = classNames('euiSideNavItemButton', {
    'euiSideNavItemButton--isClickable': isClickable,
    'euiSideNavItemButton-isOpen': depth > 0 && itemIsOpen && !isSelected,
    'euiSideNavItemButton-isSelected': isSelected
  }, buttonClassName);
  var caret;

  if (depth > 0 && childrenOnly) {
    caret = ___EmotionJSX(EuiIcon, {
      type: itemIsOpen ? 'arrowDown' : 'arrowRight',
      size: "s"
    });
  }

  var buttonContent = ___EmotionJSX("span", {
    className: "euiSideNavItemButton__content"
  }, buttonIcon, ___EmotionJSX(EuiInnerText, null, function (ref, innerText) {
    return ___EmotionJSX("span", {
      ref: ref,
      title: truncate ? innerText : undefined,
      className: classNames('euiSideNavItemButton__label', {
        'euiSideNavItemButton__label--truncated': truncate
      })
    }, children);
  }), caret);

  var renderItemProps = {
    href: href,
    rel: rel,
    target: target,
    onClick: childrenOnly ? toggleItemOpen : onClick,
    className: buttonClasses,
    children: buttonContent
  };
  return ___EmotionJSX("div", {
    className: classes
  }, ___EmotionJSX(RenderItem, _extends({}, renderItemProps, rest)), childItems);
}