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
import { EuiIcon } from '../../icon';
import { EuiTab, EuiTabs } from '../../tabs';
import { EuiFlexGroup, EuiFlexItem } from '../../flex';
import { EuiSpacer } from '../../spacer';
import { EuiTitle } from '../../title';
import { EuiText } from '../../text';
import { useIsWithinBreakpoints } from '../../../services/hooks';
import { EuiScreenReaderOnly } from '../../accessibility';
import { jsx as ___EmotionJSX } from "@emotion/react";
export var ALIGN_ITEMS = ['top', 'bottom', 'center', 'stretch']; // Gets all the tab props including the button or link props

export var EuiPageHeaderContent = function EuiPageHeaderContent(_ref) {
  var className = _ref.className,
      pageTitle = _ref.pageTitle,
      pageTitleProps = _ref.pageTitleProps,
      iconType = _ref.iconType,
      iconProps = _ref.iconProps,
      tabs = _ref.tabs,
      tabsProps = _ref.tabsProps,
      description = _ref.description,
      _ref$alignItems = _ref.alignItems,
      alignItems = _ref$alignItems === void 0 ? 'top' : _ref$alignItems,
      _ref$responsive = _ref.responsive,
      responsive = _ref$responsive === void 0 ? true : _ref$responsive,
      rightSideItems = _ref.rightSideItems,
      rightSideGroupProps = _ref.rightSideGroupProps,
      children = _ref.children,
      rest = _objectWithoutProperties(_ref, ["className", "pageTitle", "pageTitleProps", "iconType", "iconProps", "tabs", "tabsProps", "description", "alignItems", "responsive", "rightSideItems", "rightSideGroupProps", "children"]);

  var isResponsiveBreakpoint = useIsWithinBreakpoints(['xs', 's'], !!responsive);
  var classes = classNames('euiPageHeaderContent');
  var descriptionNode;

  if (description) {
    descriptionNode = ___EmotionJSX(React.Fragment, null, (pageTitle || tabs) && ___EmotionJSX(EuiSpacer, null), ___EmotionJSX(EuiText, {
      grow: false
    }, ___EmotionJSX("p", null, description)));
  }

  var pageTitleNode;

  if (pageTitle) {
    var icon = iconType ? ___EmotionJSX(EuiIcon, _extends({
      size: "xl"
    }, iconProps, {
      type: iconType,
      className: classNames('euiPageHeaderContent__titleIcon', iconProps === null || iconProps === void 0 ? void 0 : iconProps.className)
    })) : undefined;
    pageTitleNode = ___EmotionJSX(EuiTitle, _extends({}, pageTitleProps, {
      size: "l"
    }), ___EmotionJSX("h1", null, icon, pageTitle));
  }

  var tabsNode;

  if (tabs) {
    var _tabs$find;

    var tabsSize = pageTitle ? 'l' : 'xl';

    var renderTabs = function renderTabs() {
      return tabs.map(function (tab, index) {
        var label = tab.label,
            tabRest = _objectWithoutProperties(tab, ["label"]);

        return ___EmotionJSX(EuiTab, _extends({
          key: index
        }, tabRest), label);
      });
    }; // When tabs exist without a pageTitle, we need to recreate an h1 based on the currently selected tab and visually hide it


    var screenReaderPageTitle = !pageTitle && ___EmotionJSX(EuiScreenReaderOnly, null, ___EmotionJSX("h1", null, (_tabs$find = tabs.find(function (obj) {
      return obj.isSelected === true;
    })) === null || _tabs$find === void 0 ? void 0 : _tabs$find.label));

    tabsNode = ___EmotionJSX(React.Fragment, null, pageTitleNode && ___EmotionJSX(EuiSpacer, null), screenReaderPageTitle, ___EmotionJSX(EuiTabs, _extends({}, tabsProps, {
      display: "condensed",
      bottomBorder: false,
      size: tabsSize
    }), renderTabs()));
  }

  var childrenNode = children && ___EmotionJSX(React.Fragment, null, ___EmotionJSX(EuiSpacer, null), children);

  var bottomContentNode;

  if (childrenNode || tabsNode && pageTitleNode) {
    bottomContentNode = ___EmotionJSX("div", {
      className: "euiPageHeaderContent__bottom"
    }, childrenNode, pageTitleNode && tabsNode);
  }
  /**
   * The left side order depends on if a `pageTitle` was supplied.
   * If not, but there are `tabs`, then the tabs become the page title
   */


  var leftSideOrder;

  if (tabsNode && !pageTitleNode) {
    leftSideOrder = ___EmotionJSX(React.Fragment, null, tabsNode, descriptionNode);
  } else {
    leftSideOrder = ___EmotionJSX(React.Fragment, null, pageTitleNode, descriptionNode);
  }

  var rightSideFlexItem;

  if (rightSideItems && rightSideItems.length) {
    var wrapWithFlex = function wrapWithFlex() {
      return rightSideItems.map(function (item, index) {
        return ___EmotionJSX(EuiFlexItem, {
          grow: false,
          key: index
        }, item);
      });
    };

    rightSideFlexItem = ___EmotionJSX(EuiFlexItem, {
      grow: false
    }, ___EmotionJSX(EuiFlexGroup, _extends({
      wrap: true,
      responsive: false
    }, rightSideGroupProps, {
      className: classNames('euiPageHeaderContent__rightSideItems', rightSideGroupProps === null || rightSideGroupProps === void 0 ? void 0 : rightSideGroupProps.className)
    }), wrapWithFlex()));
  }

  return alignItems === 'top' || isResponsiveBreakpoint ? ___EmotionJSX("div", _extends({
    className: classes
  }, rest), ___EmotionJSX(EuiFlexGroup, {
    responsive: !!responsive,
    className: "euiPageHeaderContent__top",
    alignItems: pageTitle ? 'flexStart' : 'baseline',
    gutterSize: "l"
  }, isResponsiveBreakpoint && responsive === 'reverse' ? ___EmotionJSX(React.Fragment, null, rightSideFlexItem, ___EmotionJSX(EuiFlexItem, null, leftSideOrder)) : ___EmotionJSX(React.Fragment, null, ___EmotionJSX(EuiFlexItem, null, leftSideOrder), rightSideFlexItem)), bottomContentNode) : ___EmotionJSX("div", _extends({
    className: classes
  }, rest), ___EmotionJSX(EuiFlexGroup, {
    responsive: !!responsive,
    className: "euiPageHeaderContent__top",
    alignItems: alignItems === 'bottom' ? 'flexEnd' : alignItems,
    gutterSize: "l"
  }, ___EmotionJSX(EuiFlexItem, null, leftSideOrder, bottomContentNode), rightSideFlexItem));
};