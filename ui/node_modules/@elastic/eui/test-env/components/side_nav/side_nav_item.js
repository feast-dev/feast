"use strict";

var _interopRequireWildcard = require("@babel/runtime/helpers/interopRequireWildcard");

var _interopRequireDefault = require("@babel/runtime/helpers/interopRequireDefault");

Object.defineProperty(exports, "__esModule", {
  value: true
});
exports.EuiSideNavItem = EuiSideNavItem;

var _slicedToArray2 = _interopRequireDefault(require("@babel/runtime/helpers/slicedToArray"));

var _extends2 = _interopRequireDefault(require("@babel/runtime/helpers/extends"));

var _objectWithoutProperties2 = _interopRequireDefault(require("@babel/runtime/helpers/objectWithoutProperties"));

var _react = _interopRequireWildcard(require("react"));

var _classnames = _interopRequireDefault(require("classnames"));

var _icon = require("../icon");

var _services = require("../../services");

var _href_validator = require("../../services/security/href_validator");

var _inner_text = require("../inner_text");

var _react2 = require("@emotion/react");

/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */
var DefaultRenderItem = function DefaultRenderItem(_ref) {
  var href = _ref.href,
      target = _ref.target,
      rel = _ref.rel,
      onClick = _ref.onClick,
      className = _ref.className,
      children = _ref.children,
      disabled = _ref.disabled,
      rest = (0, _objectWithoutProperties2.default)(_ref, ["href", "target", "rel", "onClick", "className", "children", "disabled"]);

  if (href && !disabled) {
    var secureRel = (0, _services.getSecureRelForTarget)({
      href: href,
      rel: rel,
      target: target
    });
    return (0, _react2.jsx)("a", (0, _extends2.default)({
      className: className,
      href: href,
      target: target,
      rel: secureRel,
      onClick: onClick
    }, rest), children);
  }

  if (onClick || disabled) {
    return (0, _react2.jsx)("button", (0, _extends2.default)({
      type: "button",
      className: className,
      onClick: onClick,
      disabled: disabled
    }, rest), children);
  }

  return (0, _react2.jsx)("div", (0, _extends2.default)({
    className: className
  }, rest), children);
};

function EuiSideNavItem(_ref2) {
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
      rest = (0, _objectWithoutProperties2.default)(_ref2, ["isOpen", "isSelected", "isParent", "icon", "onClick", "href", "rel", "target", "items", "children", "renderItem", "depth", "className", "truncate", "emphasize", "buttonClassName", "childrenOnly"]);
  var isHrefValid = !_href || (0, _href_validator.validateHref)(_href);
  var href = isHrefValid ? _href : '';
  var isClickable = onClick || href; // Forcing accordion style item if not linked, but has children

  var _useState = (0, _react.useState)(isOpen),
      _useState2 = (0, _slicedToArray2.default)(_useState, 2),
      itemIsOpen = _useState2[0],
      setItemIsOpen = _useState2[1];

  (0, _react.useEffect)(function () {
    setItemIsOpen(isOpen);
  }, [isOpen]);

  var toggleItemOpen = function toggleItemOpen() {
    setItemIsOpen(function (isOpen) {
      return !isOpen;
    });
  };

  var childItems;

  if (items && itemIsOpen) {
    childItems = (0, _react2.jsx)("div", {
      className: "euiSideNavItem__items"
    }, items);
  }

  var buttonIcon;

  if (icon) {
    buttonIcon = /*#__PURE__*/(0, _react.cloneElement)(icon, {
      className: (0, _classnames.default)('euiSideNavItemButton__icon', icon.props.className)
    });
  }

  var classes = (0, _classnames.default)('euiSideNavItem', {
    'euiSideNavItem--root': depth === 0,
    'euiSideNavItem--rootIcon': depth === 0 && icon,
    'euiSideNavItem--trunk': depth === 1,
    'euiSideNavItem--branch': depth > 1,
    'euiSideNavItem--hasChildItems': !!childItems,
    'euiSideNavItem--emphasized': emphasize
  }, className);
  var buttonClasses = (0, _classnames.default)('euiSideNavItemButton', {
    'euiSideNavItemButton--isClickable': isClickable,
    'euiSideNavItemButton-isOpen': depth > 0 && itemIsOpen && !isSelected,
    'euiSideNavItemButton-isSelected': isSelected
  }, buttonClassName);
  var caret;

  if (depth > 0 && childrenOnly) {
    caret = (0, _react2.jsx)(_icon.EuiIcon, {
      type: itemIsOpen ? 'arrowDown' : 'arrowRight',
      size: "s"
    });
  }

  var buttonContent = (0, _react2.jsx)("span", {
    className: "euiSideNavItemButton__content"
  }, buttonIcon, (0, _react2.jsx)(_inner_text.EuiInnerText, null, function (ref, innerText) {
    return (0, _react2.jsx)("span", {
      ref: ref,
      title: truncate ? innerText : undefined,
      className: (0, _classnames.default)('euiSideNavItemButton__label', {
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
  return (0, _react2.jsx)("div", {
    className: classes
  }, (0, _react2.jsx)(RenderItem, (0, _extends2.default)({}, renderItemProps, rest)), childItems);
}