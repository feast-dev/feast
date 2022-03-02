import _extends from "@babel/runtime/helpers/extends";
import _defineProperty from "@babel/runtime/helpers/defineProperty";
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
import { keysOf } from '../../common';
import { EuiPageHeaderContent } from './page_header_content';
import { setPropsForRestrictedPageWidth } from '../_restrict_width';
import { jsx as ___EmotionJSX } from "@emotion/react";
var paddingSizeToClassNameMap = {
  none: null,
  s: 'euiPageHeader--paddingSmall',
  m: 'euiPageHeader--paddingMedium',
  l: 'euiPageHeader--paddingLarge'
};
export var PADDING_SIZES = keysOf(paddingSizeToClassNameMap);
export var EuiPageHeader = function EuiPageHeader(_ref) {
  var _classNames;

  var className = _ref.className,
      _ref$restrictWidth = _ref.restrictWidth,
      restrictWidth = _ref$restrictWidth === void 0 ? false : _ref$restrictWidth,
      _ref$paddingSize = _ref.paddingSize,
      paddingSize = _ref$paddingSize === void 0 ? 'none' : _ref$paddingSize,
      bottomBorder = _ref.bottomBorder,
      style = _ref.style,
      alignItems = _ref.alignItems,
      _ref$responsive = _ref.responsive,
      responsive = _ref$responsive === void 0 ? true : _ref$responsive,
      children = _ref.children,
      pageTitle = _ref.pageTitle,
      pageTitleProps = _ref.pageTitleProps,
      iconType = _ref.iconType,
      iconProps = _ref.iconProps,
      tabs = _ref.tabs,
      tabsProps = _ref.tabsProps,
      description = _ref.description,
      rightSideItems = _ref.rightSideItems,
      rightSideGroupProps = _ref.rightSideGroupProps,
      rest = _objectWithoutProperties(_ref, ["className", "restrictWidth", "paddingSize", "bottomBorder", "style", "alignItems", "responsive", "children", "pageTitle", "pageTitleProps", "iconType", "iconProps", "tabs", "tabsProps", "description", "rightSideItems", "rightSideGroupProps"]);

  var _setPropsForRestricte = setPropsForRestrictedPageWidth(restrictWidth, style),
      widthClassName = _setPropsForRestricte.widthClassName,
      newStyle = _setPropsForRestricte.newStyle;

  var classes = classNames('euiPageHeader', paddingSizeToClassNameMap[paddingSize], (_classNames = {}, _defineProperty(_classNames, "euiPageHeader--".concat(widthClassName), widthClassName), _defineProperty(_classNames, 'euiPageHeader--bottomBorder', bottomBorder), _defineProperty(_classNames, 'euiPageHeader--responsive', responsive === true), _defineProperty(_classNames, 'euiPageHeader--responsiveReverse', responsive === 'reverse'), _defineProperty(_classNames, 'euiPageHeader--tabsAtBottom', pageTitle && tabs), _defineProperty(_classNames, 'euiPageHeader--onlyTabs', tabs && !pageTitle && !rightSideItems && !description && !children), _classNames), "euiPageHeader--".concat(alignItems !== null && alignItems !== void 0 ? alignItems : 'center'), className);

  if (!pageTitle && !tabs && !description && !rightSideItems) {
    return ___EmotionJSX("header", _extends({
      className: classes,
      style: newStyle || style
    }, rest), children);
  }

  return ___EmotionJSX("header", _extends({
    className: classes,
    style: newStyle || style
  }, rest), ___EmotionJSX(EuiPageHeaderContent, {
    alignItems: alignItems,
    responsive: responsive,
    pageTitle: pageTitle,
    pageTitleProps: pageTitleProps,
    iconType: iconType,
    iconProps: iconProps,
    tabs: tabs,
    tabsProps: tabsProps,
    description: description,
    rightSideItems: rightSideItems,
    rightSideGroupProps: rightSideGroupProps
  }, children));
};