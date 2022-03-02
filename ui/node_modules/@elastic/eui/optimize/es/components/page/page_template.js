import _extends from "@babel/runtime/helpers/extends";
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
import React from 'react';
import classNames from 'classnames';
import { EuiPage } from './page';
import { EuiPageSideBar } from './page_side_bar';
import { EuiPageBody } from './page_body';
import { EuiPageHeader } from './page_header';
import { EuiPageContent, EuiPageContentBody } from './page_content';
import { EuiBottomBar } from '../bottom_bar';
import { useIsWithinBreakpoints } from '../../services';
import { EuiFlexGroup, EuiFlexItem } from '../flex';
import { jsx as ___EmotionJSX } from "@emotion/react";
export var TEMPLATES = ['default', 'centeredBody', 'centeredContent', 'empty'];
export var EuiPageTemplate = function EuiPageTemplate(_ref) {
  var _pageBodyProps2;

  var _ref$template = _ref.template,
      template = _ref$template === void 0 ? 'default' : _ref$template,
      _ref$restrictWidth = _ref.restrictWidth,
      restrictWidth = _ref$restrictWidth === void 0 ? true : _ref$restrictWidth,
      _ref$grow = _ref.grow,
      grow = _ref$grow === void 0 ? true : _ref$grow,
      _ref$paddingSize = _ref.paddingSize,
      paddingSize = _ref$paddingSize === void 0 ? 'l' : _ref$paddingSize,
      fullHeight = _ref.fullHeight,
      children = _ref.children,
      className = _ref.className,
      pageSideBar = _ref.pageSideBar,
      pageSideBarProps = _ref.pageSideBarProps,
      pageHeader = _ref.pageHeader,
      pageBodyProps = _ref.pageBodyProps,
      pageContentProps = _ref.pageContentProps,
      pageContentBodyProps = _ref.pageContentBodyProps,
      bottomBar = _ref.bottomBar,
      bottomBarProps = _ref.bottomBarProps,
      _ref$minHeight = _ref.minHeight,
      minHeight = _ref$minHeight === void 0 ? 460 : _ref$minHeight,
      rest = _objectWithoutProperties(_ref, ["template", "restrictWidth", "grow", "paddingSize", "fullHeight", "children", "className", "pageSideBar", "pageSideBarProps", "pageHeader", "pageBodyProps", "pageContentProps", "pageContentBodyProps", "bottomBar", "bottomBarProps", "minHeight"]);

  /**
   * Full height ~madness~ logic
   */
  var canFullHeight = useIsWithinBreakpoints(['m', 'l', 'xl']) && (template === 'default' || template === 'empty');
  var fullHeightClass = {
    'eui-fullHeight': fullHeight && canFullHeight
  };
  var yScrollClass = {
    'eui-yScroll': fullHeight && canFullHeight
  };

  if (canFullHeight && fullHeight) {
    var _pageBodyProps, _pageContentProps, _pageContentBodyProps;

    // By using flex group it will also fix the negative margin issues for nested flex groups
    children = ___EmotionJSX(EuiFlexGroup, {
      className: "eui-fullHeight",
      gutterSize: "none",
      direction: "column",
      responsive: false
    }, ___EmotionJSX(EuiFlexItem, {
      className: classNames({
        'eui-yScroll': fullHeight === true,
        'eui-fullHeight': fullHeight === 'noscroll'
      }),
      grow: true
    }, children));
    pageBodyProps = _objectSpread(_objectSpread({}, pageBodyProps), {}, {
      className: classNames(fullHeightClass, (_pageBodyProps = pageBodyProps) === null || _pageBodyProps === void 0 ? void 0 : _pageBodyProps.className)
    });
    pageContentProps = _objectSpread(_objectSpread({}, pageContentProps), {}, {
      className: classNames(yScrollClass, (_pageContentProps = pageContentProps) === null || _pageContentProps === void 0 ? void 0 : _pageContentProps.className)
    });
    pageContentBodyProps = _objectSpread(_objectSpread({}, pageContentBodyProps), {}, {
      className: classNames(fullHeightClass, (_pageContentBodyProps = pageContentBodyProps) === null || _pageContentBodyProps === void 0 ? void 0 : _pageContentBodyProps.className)
    });
  }

  var classes = classNames('euiPageTemplate', fullHeightClass, className);

  var pageStyle = _objectSpread({
    minHeight: minHeight
  }, rest.style);
  /**
   * This seems very repetitious but it's the most readable, scalable, and maintainable
   */


  switch (template) {
    /**
     * CENTERED BODY
     * The panelled content is centered
     */
    case 'centeredBody':
      return pageSideBar ? ___EmotionJSX(EuiPage, _extends({
        className: classes,
        paddingSize: "none",
        grow: grow
      }, rest, {
        style: pageStyle
      }), ___EmotionJSX(EuiPageSideBar, _extends({
        sticky: true,
        paddingSize: paddingSize
      }, pageSideBarProps), pageSideBar), ___EmotionJSX(EuiPageBody, _extends({
        paddingSize: paddingSize
      }, pageBodyProps), pageHeader && ___EmotionJSX(EuiPageHeader, _extends({
        restrictWidth: restrictWidth
      }, pageHeader)), ___EmotionJSX(EuiPageContent, _extends({
        verticalPosition: "center",
        horizontalPosition: "center",
        paddingSize: paddingSize
      }, pageContentProps), ___EmotionJSX(EuiPageContentBody, _extends({
        restrictWidth: restrictWidth
      }, pageContentBodyProps), children)))) : ___EmotionJSX(EuiPage, _extends({
        className: classes,
        paddingSize: paddingSize,
        grow: grow
      }, rest, {
        style: pageStyle
      }), ___EmotionJSX(EuiPageBody, _extends({
        restrictWidth: restrictWidth
      }, pageBodyProps), pageHeader && ___EmotionJSX(EuiPageHeader, _extends({
        paddingSize: "none",
        restrictWidth: false,
        bottomBorder: true
      }, pageHeader)), ___EmotionJSX(EuiPageBody, null, ___EmotionJSX(EuiPageContent, _extends({
        verticalPosition: "center",
        horizontalPosition: "center",
        paddingSize: paddingSize
      }, pageContentProps), ___EmotionJSX(EuiPageContentBody, _extends({
        paddingSize: "none",
        restrictWidth: restrictWidth
      }, pageContentBodyProps), children)))));

    /**
     * CENTERED CONTENT
     * The content inside the panel is centered
     */

    case 'centeredContent':
      return pageSideBar ? ___EmotionJSX(EuiPage, _extends({
        className: classes,
        paddingSize: "none",
        grow: grow
      }, rest, {
        style: pageStyle
      }), ___EmotionJSX(EuiPageSideBar, _extends({
        sticky: true,
        paddingSize: paddingSize
      }, pageSideBarProps), pageSideBar), ___EmotionJSX(EuiPageBody, _extends({
        panelled: true,
        paddingSize: paddingSize
      }, pageBodyProps), pageHeader && ___EmotionJSX(EuiPageHeader, _extends({
        restrictWidth: restrictWidth
      }, pageHeader)), ___EmotionJSX(EuiPageContent, _extends({
        verticalPosition: "center",
        horizontalPosition: "center",
        hasShadow: false,
        color: "subdued",
        paddingSize: paddingSize
      }, pageContentProps), ___EmotionJSX(EuiPageContentBody, _extends({
        restrictWidth: restrictWidth
      }, pageContentBodyProps), children)))) : ___EmotionJSX(EuiPage, _extends({
        className: classes,
        paddingSize: "none",
        grow: grow
      }, rest, {
        style: pageStyle
      }), ___EmotionJSX(EuiPageBody, pageBodyProps, pageHeader && ___EmotionJSX(EuiPageHeader, _extends({
        paddingSize: paddingSize,
        restrictWidth: restrictWidth
      }, pageHeader)), ___EmotionJSX(EuiPageContent, {
        role: null,
        borderRadius: "none",
        hasShadow: false,
        paddingSize: paddingSize,
        style: {
          display: 'flex'
        }
      }, ___EmotionJSX(EuiPageContent, _extends({
        verticalPosition: "center",
        horizontalPosition: "center",
        hasShadow: false,
        color: "subdued",
        paddingSize: paddingSize
      }, pageContentProps), ___EmotionJSX(EuiPageContentBody, _extends({
        restrictWidth: restrictWidth
      }, pageContentBodyProps), children)))));

    /**
     * EMPTY
     * No panelling at all
     */

    case 'empty':
      return pageSideBar ? ___EmotionJSX(EuiPage, _extends({
        className: classes,
        paddingSize: "none",
        grow: grow
      }, rest, {
        style: pageStyle
      }), ___EmotionJSX(EuiPageSideBar, _extends({
        sticky: true,
        paddingSize: paddingSize
      }, pageSideBarProps), pageSideBar), ___EmotionJSX(EuiPageBody, _extends({
        paddingSize: paddingSize
      }, pageBodyProps), pageHeader && ___EmotionJSX(EuiPageHeader, _extends({
        restrictWidth: restrictWidth
      }, pageHeader)), ___EmotionJSX(EuiPageContent, _extends({
        hasBorder: false,
        hasShadow: false,
        paddingSize: 'none',
        color: 'transparent',
        borderRadius: 'none'
      }, pageContentProps), ___EmotionJSX(EuiPageContentBody, _extends({
        restrictWidth: restrictWidth
      }, pageContentBodyProps), children)))) : ___EmotionJSX(EuiPage, _extends({
        className: classes,
        paddingSize: paddingSize,
        grow: grow
      }, rest, {
        style: pageStyle
      }), ___EmotionJSX(EuiPageBody, _extends({
        restrictWidth: restrictWidth
      }, pageBodyProps), pageHeader && ___EmotionJSX(EuiPageHeader, _extends({
        paddingSize: "none",
        restrictWidth: false,
        bottomBorder: true
      }, pageHeader)), ___EmotionJSX(EuiPageContent, _extends({
        hasBorder: false,
        hasShadow: false,
        paddingSize: 'none',
        color: 'transparent',
        borderRadius: 'none'
      }, pageContentProps), ___EmotionJSX(EuiPageContentBody, _extends({
        paddingSize: "none"
      }, pageContentBodyProps), children))));

    /**
     * DEFAULT
     * Typical layout with nothing "centered"
     */

    default:
      // Only the default template can display a bottom bar
      var bottomBarNode = bottomBar ? ___EmotionJSX(EuiBottomBar, _extends({
        paddingSize: paddingSize,
        position: canFullHeight && fullHeight ? 'static' : 'sticky' // Using uknown here because of the possible conflict with overriding props and position `sticky`

      }, bottomBarProps), ___EmotionJSX(EuiPageContentBody, {
        paddingSize: 'none',
        restrictWidth: restrictWidth
      }, bottomBar)) : undefined;
      return pageSideBar ? ___EmotionJSX(EuiPage, _extends({
        className: classes,
        paddingSize: "none",
        grow: grow
      }, rest, {
        style: pageStyle
      }), ___EmotionJSX(EuiPageSideBar, _extends({
        sticky: true,
        paddingSize: paddingSize
      }, pageSideBarProps), pageSideBar), ___EmotionJSX(EuiPageBody, _extends({
        panelled: true,
        paddingSize: "none"
      }, pageBodyProps), ___EmotionJSX(EuiPageBody, {
        component: "div",
        paddingSize: paddingSize,
        className: (_pageBodyProps2 = pageBodyProps) === null || _pageBodyProps2 === void 0 ? void 0 : _pageBodyProps2.className
      }, pageHeader && ___EmotionJSX(EuiPageHeader, _extends({
        bottomBorder: true,
        restrictWidth: restrictWidth
      }, pageHeader)), ___EmotionJSX(EuiPageContent, _extends({
        hasShadow: false,
        hasBorder: false,
        color: 'transparent',
        borderRadius: 'none',
        paddingSize: "none"
      }, pageContentProps), ___EmotionJSX(EuiPageContentBody, _extends({
        restrictWidth: restrictWidth
      }, pageContentBodyProps), children))), bottomBarNode)) : ___EmotionJSX(EuiPage, _extends({
        className: classes,
        paddingSize: "none",
        grow: grow
      }, rest, {
        style: pageStyle
      }), ___EmotionJSX(EuiPageBody, pageBodyProps, pageHeader && ___EmotionJSX(EuiPageHeader, _extends({
        restrictWidth: restrictWidth,
        paddingSize: paddingSize
      }, pageHeader)), ___EmotionJSX(EuiPageContent, _extends({
        hasBorder: pageHeader === undefined ? false : undefined,
        hasShadow: false,
        paddingSize: 'none',
        color: 'plain',
        borderRadius: 'none'
      }, pageContentProps), ___EmotionJSX(EuiPageContentBody, _extends({
        restrictWidth: restrictWidth,
        paddingSize: paddingSize
      }, pageContentBodyProps), children)), bottomBarNode));
  }
};