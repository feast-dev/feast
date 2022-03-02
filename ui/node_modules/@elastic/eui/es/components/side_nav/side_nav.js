function _typeof(obj) { "@babel/helpers - typeof"; if (typeof Symbol === "function" && typeof Symbol.iterator === "symbol") { _typeof = function _typeof(obj) { return typeof obj; }; } else { _typeof = function _typeof(obj) { return obj && typeof Symbol === "function" && obj.constructor === Symbol && obj !== Symbol.prototype ? "symbol" : typeof obj; }; } return _typeof(obj); }

function _extends() { _extends = Object.assign || function (target) { for (var i = 1; i < arguments.length; i++) { var source = arguments[i]; for (var key in source) { if (Object.prototype.hasOwnProperty.call(source, key)) { target[key] = source[key]; } } } return target; }; return _extends.apply(this, arguments); }

function _objectWithoutProperties(source, excluded) { if (source == null) return {}; var target = _objectWithoutPropertiesLoose(source, excluded); var key, i; if (Object.getOwnPropertySymbols) { var sourceSymbolKeys = Object.getOwnPropertySymbols(source); for (i = 0; i < sourceSymbolKeys.length; i++) { key = sourceSymbolKeys[i]; if (excluded.indexOf(key) >= 0) continue; if (!Object.prototype.propertyIsEnumerable.call(source, key)) continue; target[key] = source[key]; } } return target; }

function _objectWithoutPropertiesLoose(source, excluded) { if (source == null) return {}; var target = {}; var sourceKeys = Object.keys(source); var key, i; for (i = 0; i < sourceKeys.length; i++) { key = sourceKeys[i]; if (excluded.indexOf(key) >= 0) continue; target[key] = source[key]; } return target; }

function _classCallCheck(instance, Constructor) { if (!(instance instanceof Constructor)) { throw new TypeError("Cannot call a class as a function"); } }

function _defineProperties(target, props) { for (var i = 0; i < props.length; i++) { var descriptor = props[i]; descriptor.enumerable = descriptor.enumerable || false; descriptor.configurable = true; if ("value" in descriptor) descriptor.writable = true; Object.defineProperty(target, descriptor.key, descriptor); } }

function _createClass(Constructor, protoProps, staticProps) { if (protoProps) _defineProperties(Constructor.prototype, protoProps); if (staticProps) _defineProperties(Constructor, staticProps); return Constructor; }

function _inherits(subClass, superClass) { if (typeof superClass !== "function" && superClass !== null) { throw new TypeError("Super expression must either be null or a function"); } subClass.prototype = Object.create(superClass && superClass.prototype, { constructor: { value: subClass, writable: true, configurable: true } }); if (superClass) _setPrototypeOf(subClass, superClass); }

function _setPrototypeOf(o, p) { _setPrototypeOf = Object.setPrototypeOf || function _setPrototypeOf(o, p) { o.__proto__ = p; return o; }; return _setPrototypeOf(o, p); }

function _createSuper(Derived) { var hasNativeReflectConstruct = _isNativeReflectConstruct(); return function _createSuperInternal() { var Super = _getPrototypeOf(Derived), result; if (hasNativeReflectConstruct) { var NewTarget = _getPrototypeOf(this).constructor; result = Reflect.construct(Super, arguments, NewTarget); } else { result = Super.apply(this, arguments); } return _possibleConstructorReturn(this, result); }; }

function _possibleConstructorReturn(self, call) { if (call && (_typeof(call) === "object" || typeof call === "function")) { return call; } return _assertThisInitialized(self); }

function _assertThisInitialized(self) { if (self === void 0) { throw new ReferenceError("this hasn't been initialised - super() hasn't been called"); } return self; }

function _isNativeReflectConstruct() { if (typeof Reflect === "undefined" || !Reflect.construct) return false; if (Reflect.construct.sham) return false; if (typeof Proxy === "function") return true; try { Date.prototype.toString.call(Reflect.construct(Date, [], function () {})); return true; } catch (e) { return false; } }

function _getPrototypeOf(o) { _getPrototypeOf = Object.setPrototypeOf ? Object.getPrototypeOf : function _getPrototypeOf(o) { return o.__proto__ || Object.getPrototypeOf(o); }; return _getPrototypeOf(o); }

function _defineProperty(obj, key, value) { if (key in obj) { Object.defineProperty(obj, key, { value: value, enumerable: true, configurable: true, writable: true }); } else { obj[key] = value; } return obj; }

/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */
import React, { Component } from 'react';
import PropTypes from "prop-types";
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

EuiSideNav.propTypes = {
  /**
       * Class names to be merged into the final `className` property.
       */
  className: PropTypes.string,
  "aria-label": PropTypes.string,
  "data-test-subj": PropTypes.string,

  /**
       * Creates an associated heading element and uses the same node as default for `mobileTitle`
       */
  heading: PropTypes.node,

  /**
       * Adds a couple extra #EuiSideNavHeading props and extends the props of EuiTitle that wraps the `heading`
       */
  headingProps: PropTypes.shape({
    /**
       * The actual HTML heading element to wrap the `heading`.
       * Default is `h2`
       */
    element: PropTypes.oneOf(["h1", "h2", "h3", "h4", "h5", "h6", "span"]),

    /**
       * For best accessibility, `<nav>` elements should have a nested heading. But you can hide this element if it's redundent from something else (except on mobile).
       */
    screenReaderOnly: PropTypes.bool
  }),

  /**
       * When called, toggles visibility of the navigation menu at mobile responsive widths. The callback should set the `isOpenOnMobile` prop to actually toggle navigation visibility.
       */
  toggleOpenOnMobile: PropTypes.func,

  /**
       * If `true`, the navigation menu will be open at mobile device widths. Use in conjunction with the `toggleOpenOnMobile` prop.
       */
  isOpenOnMobile: PropTypes.bool,

  /**
       * A React node to render at mobile responsive widths, representing the title of this navigation menu.
       */
  mobileTitle: PropTypes.node,

  /**
       * Array of breakpoint names for when to show the mobile version.
       * Set to `undefined` to remove responsive behavior
       */
  mobileBreakpoints: PropTypes.arrayOf(PropTypes.oneOf(["xs", "s", "m", "l", "xl"]).isRequired),

  /**
       *  An array of #EuiSideNavItem objects. Lists navigation menu items.
       */
  items: PropTypes.arrayOf(PropTypes.shape({
    /**
       * A value that is passed to React as the `key` for this item
       */
    id: PropTypes.oneOfType([PropTypes.string.isRequired, PropTypes.number.isRequired]).isRequired,

    /**
       * If set to true it will force the item to display in an "open" state at all times.
       */
    forceOpen: PropTypes.bool,

    /**
       * Array containing additional item objects, representing nested children of this navigation item.
       */
    items: PropTypes.arrayOf(PropTypes.shape({
      /**
         * A value that is passed to React as the `key` for this item
         */
      id: PropTypes.oneOfType([PropTypes.string.isRequired, PropTypes.number.isRequired]).isRequired,

      /**
         * If set to true it will force the item to display in an "open" state at all times.
         */
      forceOpen: PropTypes.bool,

      /**
         * Array containing additional item objects, representing nested children of this navigation item.
         */
      items: PropTypes.arrayOf(PropTypes.any.isRequired),

      /**
         * React node representing the text to render for this item (usually a string will suffice).
         */
      name: PropTypes.node.isRequired,

      /**
         * Function overriding default rendering for this navigation item — when called, it should return a React node representing a replacement navigation item.
         */
      renderItem: PropTypes.func
    }).isRequired),

    /**
       * React node representing the text to render for this item (usually a string will suffice).
       */
    name: PropTypes.node.isRequired,

    /**
       * Function overriding default rendering for this navigation item — when called, it should return a React node representing a replacement navigation item.
       */
    renderItem: PropTypes.func
  }).isRequired).isRequired,

  /**
       * Overrides default navigation menu item rendering. When called, it should return a React node representing a replacement navigation item.
       */
  renderItem: PropTypes.func,

  /**
       * Truncates the text of all items to stick to a single line
       */
  truncate: PropTypes.bool
};