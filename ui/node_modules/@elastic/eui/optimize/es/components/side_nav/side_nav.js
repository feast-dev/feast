import _extends from "@babel/runtime/helpers/extends";
import _objectWithoutProperties from "@babel/runtime/helpers/objectWithoutProperties";
import _classCallCheck from "@babel/runtime/helpers/classCallCheck";
import _createClass from "@babel/runtime/helpers/createClass";
import _assertThisInitialized from "@babel/runtime/helpers/assertThisInitialized";
import _inherits from "@babel/runtime/helpers/inherits";
import _possibleConstructorReturn from "@babel/runtime/helpers/possibleConstructorReturn";
import _getPrototypeOf from "@babel/runtime/helpers/getPrototypeOf";
import _defineProperty from "@babel/runtime/helpers/defineProperty";

function _createSuper(Derived) { var hasNativeReflectConstruct = _isNativeReflectConstruct(); return function _createSuperInternal() { var Super = _getPrototypeOf(Derived), result; if (hasNativeReflectConstruct) { var NewTarget = _getPrototypeOf(this).constructor; result = Reflect.construct(Super, arguments, NewTarget); } else { result = Super.apply(this, arguments); } return _possibleConstructorReturn(this, result); }; }

function _isNativeReflectConstruct() { if (typeof Reflect === "undefined" || !Reflect.construct) return false; if (Reflect.construct.sham) return false; if (typeof Proxy === "function") return true; try { Date.prototype.toString.call(Reflect.construct(Date, [], function () {})); return true; } catch (e) { return false; } }

/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */
import React, { Component } from 'react';
import classNames from 'classnames';
import { EuiSideNavItem } from './side_nav_item';
import { EuiButtonEmpty } from '../button';
import { EuiTitle } from '../title';
import { EuiScreenReaderOnly } from '../accessibility';
import { htmlIdGenerator } from '../../services';
import { EuiHideFor, EuiShowFor } from '../responsive';
import { jsx as ___EmotionJSX } from "@emotion/react";
export var EuiSideNav = /*#__PURE__*/function (_Component) {
  _inherits(EuiSideNav, _Component);

  var _super = _createSuper(EuiSideNav);

  function EuiSideNav() {
    var _this;

    _classCallCheck(this, EuiSideNav);

    for (var _len = arguments.length, args = new Array(_len), _key = 0; _key < _len; _key++) {
      args[_key] = arguments[_key];
    }

    _this = _super.call.apply(_super, [this].concat(args));

    _defineProperty(_assertThisInitialized(_this), "generateId", htmlIdGenerator('euiSideNav'));

    _defineProperty(_assertThisInitialized(_this), "isItemOpen", function (item) {
      // The developer can force the item to be open.
      if (item.forceOpen) {
        return true;
      } // Of course a selected item is open.


      if (item.isSelected) {
        return true;
      } // The item has to be open if it has a child that's open.


      if (item.items) {
        return item.items.some(_this.isItemOpen);
      }

      return false;
    });

    _defineProperty(_assertThisInitialized(_this), "renderTree", function (items) {
      var depth = arguments.length > 1 && arguments[1] !== undefined ? arguments[1] : 0;
      var _this$props = _this.props,
          renderItem = _this$props.renderItem,
          truncate = _this$props.truncate;
      return items.map(function (item) {
        var id = item.id,
            name = item.name,
            isSelected = item.isSelected,
            childItems = item.items,
            icon = item.icon,
            onClick = item.onClick,
            href = item.href,
            forceOpen = item.forceOpen,
            rest = _objectWithoutProperties(item, ["id", "name", "isSelected", "items", "icon", "onClick", "href", "forceOpen"]); // Root items are always open.


        var isOpen = depth === 0 ? true : _this.isItemOpen(item);
        var renderedItems;

        if (childItems) {
          renderedItems = _this.renderTree(childItems, depth + 1);
        } // Act as an accordion only if item is not linked but has children (and not the root)


        var childrenOnly = depth > 0 && !onClick && !href && !!childItems;
        return ___EmotionJSX(EuiSideNavItem, _extends({
          isOpen: isOpen,
          isSelected: !childrenOnly && isSelected,
          isParent: !!childItems,
          icon: icon,
          onClick: onClick,
          href: href,
          items: renderedItems,
          key: id,
          depth: depth,
          renderItem: renderItem,
          truncate: truncate,
          childrenOnly: childrenOnly
        }, rest), name);
      });
    });

    return _this;
  }

  _createClass(EuiSideNav, [{
    key: "render",
    value: function render() {
      var _this$props2 = this.props,
          className = _this$props2.className,
          items = _this$props2.items,
          toggleOpenOnMobile = _this$props2.toggleOpenOnMobile,
          isOpenOnMobile = _this$props2.isOpenOnMobile,
          mobileTitle = _this$props2.mobileTitle,
          mobileBreakpoints = _this$props2.mobileBreakpoints,
          renderItem = _this$props2.renderItem,
          truncate = _this$props2.truncate,
          heading = _this$props2.heading,
          _this$props2$headingP = _this$props2.headingProps,
          headingProps = _this$props2$headingP === void 0 ? {} : _this$props2$headingP,
          rest = _objectWithoutProperties(_this$props2, ["className", "items", "toggleOpenOnMobile", "isOpenOnMobile", "mobileTitle", "mobileBreakpoints", "renderItem", "truncate", "heading", "headingProps"]);

      var classes = classNames('euiSideNav', className, {
        'euiSideNav-isOpenMobile': isOpenOnMobile
      }); // To support the extra CSS needed to show/hide/animate the content,
      // We add a className for every breakpoint supported

      var contentClasses = classNames('euiSideNav__content', mobileBreakpoints === null || mobileBreakpoints === void 0 ? void 0 : mobileBreakpoints.map(function (breakpointName) {
        return "euiSideNav__contentMobile-".concat(breakpointName);
      }));
      var sideNavContentId = this.generateId('content');

      var navContent = ___EmotionJSX("div", {
        id: sideNavContentId,
        className: contentClasses
      }, this.renderTree(items));

      var _ref = headingProps,
          _ref$screenReaderOnly = _ref.screenReaderOnly,
          headingScreenReaderOnly = _ref$screenReaderOnly === void 0 ? false : _ref$screenReaderOnly,
          _ref$element = _ref.element,
          HeadingElement = _ref$element === void 0 ? 'h2' : _ref$element,
          titleProps = _objectWithoutProperties(_ref, ["screenReaderOnly", "element"]);

      var hasMobileVersion = mobileBreakpoints && mobileBreakpoints.length > 0;
      var hasHeader = !!heading;
      var headingNode;
      var sharedHeadingProps = {
        id: (headingProps === null || headingProps === void 0 ? void 0 : headingProps.id) || this.generateId('heading'),
        className: headingProps === null || headingProps === void 0 ? void 0 : headingProps.className,
        'data-test-subj': headingProps === null || headingProps === void 0 ? void 0 : headingProps['data-test-subj'],
        'aria-label': headingProps === null || headingProps === void 0 ? void 0 : headingProps['aria-label']
      };

      if (hasHeader) {
        headingNode = ___EmotionJSX(HeadingElement, sharedHeadingProps, heading);

        if (headingScreenReaderOnly) {
          headingNode = ___EmotionJSX(EuiScreenReaderOnly, null, headingNode);
        } else {
          headingNode = ___EmotionJSX(EuiTitle, _extends({
            size: "xs"
          }, titleProps, {
            className: classNames('euiSideNav__heading', headingProps === null || headingProps === void 0 ? void 0 : headingProps.className)
          }), ___EmotionJSX(HeadingElement, sharedHeadingProps, heading));
        }
      }

      var mobileNode;
      var breakpoints = mobileBreakpoints;

      if (hasMobileVersion) {
        mobileNode = ___EmotionJSX(EuiShowFor, {
          sizes: breakpoints || 'none'
        }, ___EmotionJSX("nav", _extends({
          "aria-labelledby": sharedHeadingProps.id,
          className: classes
        }, rest), ___EmotionJSX(HeadingElement, sharedHeadingProps, ___EmotionJSX(EuiButtonEmpty, {
          className: "euiSideNav__mobileToggle",
          textProps: {
            className: 'euiSideNav__mobileToggleText'
          },
          contentProps: {
            className: 'euiSideNav__mobileToggleContent'
          },
          onClick: toggleOpenOnMobile,
          iconType: "apps",
          iconSide: "right",
          "aria-controls": sideNavContentId,
          "aria-expanded": isOpenOnMobile
        }, mobileTitle || heading)), navContent));
      }

      return ___EmotionJSX(React.Fragment, null, mobileNode, ___EmotionJSX(EuiHideFor, {
        sizes: breakpoints || 'none'
      }, ___EmotionJSX("nav", _extends({
        "aria-labelledby": headingNode ? sharedHeadingProps.id : undefined,
        className: classes
      }, rest), headingNode, navContent)));
    }
  }]);

  return EuiSideNav;
}(Component);

_defineProperty(EuiSideNav, "defaultProps", {
  items: [],
  mobileBreakpoints: ['xs', 's']
});