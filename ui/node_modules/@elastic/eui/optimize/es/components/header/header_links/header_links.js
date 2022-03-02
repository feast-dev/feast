import _extends from "@babel/runtime/helpers/extends";
import _slicedToArray from "@babel/runtime/helpers/slicedToArray";
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
import React, { useState, useEffect } from 'react';
import classNames from 'classnames';
import { keysOf } from '../../common';
import { EuiIcon } from '../../icon';
import { EuiPopover } from '../../popover';
import { EuiI18n } from '../../i18n';
import { EuiHeaderSectionItemButton } from '../header_section';
import { EuiHideFor, EuiShowFor } from '../../responsive';
import { jsx as ___EmotionJSX } from "@emotion/react";
var gutterSizeToClassNameMap = {
  xs: '--gutterXS',
  s: '--gutterS',
  m: '--gutterM',
  l: '--gutterL'
};
export var GUTTER_SIZES = keysOf(gutterSizeToClassNameMap);
export var EuiHeaderLinks = function EuiHeaderLinks(_ref) {
  var children = _ref.children,
      className = _ref.className,
      _ref$gutterSize = _ref.gutterSize,
      gutterSize = _ref$gutterSize === void 0 ? 's' : _ref$gutterSize,
      _ref$popoverBreakpoin = _ref.popoverBreakpoints,
      popoverBreakpoints = _ref$popoverBreakpoin === void 0 ? ['xs', 's'] : _ref$popoverBreakpoin,
      popoverButtonProps = _ref.popoverButtonProps,
      popoverProps = _ref.popoverProps,
      rest = _objectWithoutProperties(_ref, ["children", "className", "gutterSize", "popoverBreakpoints", "popoverButtonProps", "popoverProps"]);

  var _popoverButtonProps = _objectSpread({}, popoverButtonProps),
      _onClick = _popoverButtonProps.onClick,
      _popoverButtonProps$i = _popoverButtonProps.iconType,
      iconType = _popoverButtonProps$i === void 0 ? 'apps' : _popoverButtonProps$i,
      popoverButtonRest = _objectWithoutProperties(_popoverButtonProps, ["onClick", "iconType"]);

  var _useState = useState(false),
      _useState2 = _slicedToArray(_useState, 2),
      mobileMenuIsOpen = _useState2[0],
      setMobileMenuIsOpen = _useState2[1];

  var onMenuButtonClick = function onMenuButtonClick(e) {
    _onClick && _onClick(e);
    setMobileMenuIsOpen(!mobileMenuIsOpen);
  };

  var closeMenu = function closeMenu() {
    setMobileMenuIsOpen(false);
  };

  useEffect(function () {
    window.addEventListener('resize', closeMenu);
    return function () {
      window.removeEventListener('resize', closeMenu);
    };
  });
  var classes = classNames('euiHeaderLinks', className);

  var button = ___EmotionJSX(EuiI18n, {
    token: "euiHeaderLinks.openNavigationMenu",
    default: "Open menu"
  }, function (openNavigationMenu) {
    return ___EmotionJSX(EuiHeaderSectionItemButton, _extends({
      "aria-label": openNavigationMenu,
      onClick: onMenuButtonClick
    }, popoverButtonRest), ___EmotionJSX(EuiIcon, {
      type: iconType,
      size: "m"
    }));
  });

  return ___EmotionJSX(EuiI18n, {
    token: "euiHeaderLinks.appNavigation",
    default: "App menu"
  }, function (appNavigation) {
    return ___EmotionJSX("nav", _extends({
      className: classes,
      "aria-label": appNavigation
    }, rest), ___EmotionJSX(EuiHideFor, {
      sizes: popoverBreakpoints
    }, ___EmotionJSX("div", {
      className: classNames('euiHeaderLinks__list', ["euiHeaderLinks__list".concat(gutterSizeToClassNameMap[gutterSize])])
    }, children)), ___EmotionJSX(EuiShowFor, {
      sizes: popoverBreakpoints
    }, ___EmotionJSX(EuiPopover, _extends({
      button: button,
      isOpen: mobileMenuIsOpen,
      anchorPosition: "downRight",
      closePopover: closeMenu,
      panelPaddingSize: "none"
    }, popoverProps), ___EmotionJSX("div", {
      className: classNames('euiHeaderLinks__mobileList', ["euiHeaderLinks__mobileList".concat(gutterSizeToClassNameMap[gutterSize])])
    }, children))));
  });
};