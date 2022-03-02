function _typeof(obj) { "@babel/helpers - typeof"; if (typeof Symbol === "function" && typeof Symbol.iterator === "symbol") { _typeof = function _typeof(obj) { return typeof obj; }; } else { _typeof = function _typeof(obj) { return obj && typeof Symbol === "function" && obj.constructor === Symbol && obj !== Symbol.prototype ? "symbol" : typeof obj; }; } return _typeof(obj); }

function _extends() { _extends = Object.assign || function (target) { for (var i = 1; i < arguments.length; i++) { var source = arguments[i]; for (var key in source) { if (Object.prototype.hasOwnProperty.call(source, key)) { target[key] = source[key]; } } } return target; }; return _extends.apply(this, arguments); }

function ownKeys(object, enumerableOnly) { var keys = Object.keys(object); if (Object.getOwnPropertySymbols) { var symbols = Object.getOwnPropertySymbols(object); if (enumerableOnly) symbols = symbols.filter(function (sym) { return Object.getOwnPropertyDescriptor(object, sym).enumerable; }); keys.push.apply(keys, symbols); } return keys; }

function _objectSpread(target) { for (var i = 1; i < arguments.length; i++) { var source = arguments[i] != null ? arguments[i] : {}; if (i % 2) { ownKeys(Object(source), true).forEach(function (key) { _defineProperty(target, key, source[key]); }); } else if (Object.getOwnPropertyDescriptors) { Object.defineProperties(target, Object.getOwnPropertyDescriptors(source)); } else { ownKeys(Object(source)).forEach(function (key) { Object.defineProperty(target, key, Object.getOwnPropertyDescriptor(source, key)); }); } } return target; }

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
import React, { Component, createRef } from 'react';
import PropTypes from "prop-types";
import { htmlIdGenerator } from '../../../services';
import { EuiTabs } from '../tabs';
import { EuiTab } from '../tab';
import { jsx as ___EmotionJSX } from "@emotion/react";

/**
 * Marked as const so type is `['initial', 'selected']` instead of `string[]`
 */
export var AUTOFOCUS = ['initial', 'selected'];
export var EuiTabbedContent = /*#__PURE__*/function (_Component) {
  _inherits(EuiTabbedContent, _Component);

  var _super = _createSuper(EuiTabbedContent);

  function EuiTabbedContent(props) {
    var _this;

    _classCallCheck(this, EuiTabbedContent);

    _this = _super.call(this, props);

    _defineProperty(_assertThisInitialized(_this), "rootId", htmlIdGenerator()());

    _defineProperty(_assertThisInitialized(_this), "tabsRef", /*#__PURE__*/createRef());

    _defineProperty(_assertThisInitialized(_this), "focusTab", function () {
      var targetTab = _this.tabsRef.current.querySelector("#".concat(_this.state.selectedTabId));

      targetTab.focus();
    });

    _defineProperty(_assertThisInitialized(_this), "initializeFocus", function () {
      if (!_this.state.inFocus && _this.props.autoFocus === 'selected') {
        // Must wait for setState to finish before calling `.focus()`
        // as the focus call triggers a blur on the first tab
        _this.setState({
          inFocus: true
        }, function () {
          _this.focusTab();
        });
      }
    });

    _defineProperty(_assertThisInitialized(_this), "removeFocus", function (blurEvent) {
      // only set inFocus to false if the wrapping div doesn't contain the now-focusing element
      var currentTarget = blurEvent.currentTarget;
      var relatedTarget = blurEvent.relatedTarget;

      if (currentTarget.contains(relatedTarget) === false) {
        _this.setState({
          inFocus: false
        });
      }
    });

    _defineProperty(_assertThisInitialized(_this), "onTabClick", function (selectedTab) {
      var _this$props = _this.props,
          onTabClick = _this$props.onTabClick,
          externalSelectedTab = _this$props.selectedTab;

      if (onTabClick) {
        onTabClick(selectedTab);
      } // Only track selection state if it's not controlled externally.


      if (!externalSelectedTab) {
        _this.setState({
          selectedTabId: selectedTab.id
        }, function () {
          _this.focusTab();
        });
      }
    });

    var initialSelectedTab = props.initialSelectedTab,
        _selectedTab = props.selectedTab,
        tabs = props.tabs; // Only track selection state if it's not controlled externally.

    var selectedTabId;

    if (!_selectedTab) {
      selectedTabId = initialSelectedTab && initialSelectedTab.id || tabs[0].id;
    }

    _this.state = {
      selectedTabId: selectedTabId,
      inFocus: false
    };
    return _this;
  }

  _createClass(EuiTabbedContent, [{
    key: "componentDidMount",
    value: function componentDidMount() {
      // IE11 doesn't support the `relatedTarget` event property for blur events
      // but does add it for focusout. React doesn't support `onFocusOut` so here we are.
      if (this.tabsRef.current) {
        // Current short-term solution for event listener (see https://github.com/elastic/eui/pull/2717)
        this.tabsRef.current.addEventListener('focusout', this.removeFocus);
      }
    }
  }, {
    key: "componentWillUnmount",
    value: function componentWillUnmount() {
      if (this.tabsRef.current) {
        // Current short-term solution for event listener (see https://github.com/elastic/eui/pull/2717)
        this.tabsRef.current.removeEventListener('focusout', this.removeFocus);
      }
    }
  }, {
    key: "render",
    value: function render() {
      var _this2 = this;

      var _this$props2 = this.props,
          className = _this$props2.className,
          display = _this$props2.display,
          expand = _this$props2.expand,
          initialSelectedTab = _this$props2.initialSelectedTab,
          onTabClick = _this$props2.onTabClick,
          externalSelectedTab = _this$props2.selectedTab,
          size = _this$props2.size,
          tabs = _this$props2.tabs,
          autoFocus = _this$props2.autoFocus,
          rest = _objectWithoutProperties(_this$props2, ["className", "display", "expand", "initialSelectedTab", "onTabClick", "selectedTab", "size", "tabs", "autoFocus"]); // Allow the consumer to control tab selection.


      var selectedTab = externalSelectedTab || tabs.find(function (tab) {
        return tab.id === _this2.state.selectedTabId;
      });
      var _ref = selectedTab,
          selectedTabContent = _ref.content,
          selectedTabId = _ref.id;
      return ___EmotionJSX("div", _extends({
        className: className
      }, rest), ___EmotionJSX(EuiTabs, {
        ref: this.tabsRef,
        expand: expand,
        display: display,
        size: size,
        onFocus: this.initializeFocus
      }, tabs.map(function (tab) {
        var id = tab.id,
            name = tab.name,
            content = tab.content,
            tabProps = _objectWithoutProperties(tab, ["id", "name", "content"]);

        var props = _objectSpread(_objectSpread({
          key: id,
          id: id
        }, tabProps), {}, {
          onClick: function onClick() {
            return _this2.onTabClick(tab);
          },
          isSelected: tab === selectedTab,
          'aria-controls': "".concat(_this2.rootId)
        });

        return ___EmotionJSX(EuiTab, props, name);
      })), ___EmotionJSX("div", {
        role: "tabpanel",
        id: "".concat(this.rootId),
        "aria-labelledby": selectedTabId
      }, selectedTabContent));
    }
  }]);

  return EuiTabbedContent;
}(Component);

_defineProperty(EuiTabbedContent, "defaultProps", {
  autoFocus: 'initial'
});

EuiTabbedContent.propTypes = {
  className: PropTypes.string,
  "aria-label": PropTypes.string,
  "data-test-subj": PropTypes.string,

  /**
       * When tabbing into the tabs, set the focus on `initial` for the first tab,
       * or `selected` for the currently selected tab. Best use case is for inside of
       * overlay content like popovers or flyouts.
       */
  autoFocus: PropTypes.oneOf(["initial", "selected"]),

  /**
       * Choose `default` or alternative `condensed` display styles
       */
  display: PropTypes.oneOf(["condensed", "default"]),

  /**
       * Evenly stretches each tab to fill the horizontal space
       */
  expand: PropTypes.bool,

  /**
       * Use this prop to set the initially selected tab while letting the tabbed content component
       * control selection state internally
       */
  initialSelectedTab: PropTypes.shape({
    id: PropTypes.string.isRequired,
    name: PropTypes.node.isRequired,
    content: PropTypes.node.isRequired,
    isSelected: PropTypes.bool,
    disabled: PropTypes.bool,

    /**
       * Places content before the tab content/children.
       * Will be excluded from interactive effects.
       */
    prepend: PropTypes.node,

    /**
       * Places content after the tab content/children.
       * Will be excluded from interactive effects.
       */
    append: PropTypes.node,
    className: PropTypes.string,
    "aria-label": PropTypes.string,
    "data-test-subj": PropTypes.string
  }),
  onTabClick: PropTypes.func,

  /**
       * Use this prop if you want to control selection state within the owner component
       */
  selectedTab: PropTypes.shape({
    id: PropTypes.string.isRequired,
    name: PropTypes.node.isRequired,
    content: PropTypes.node.isRequired,
    isSelected: PropTypes.bool,
    disabled: PropTypes.bool,
    prepend: PropTypes.node,
    append: PropTypes.node,
    className: PropTypes.string,
    "aria-label": PropTypes.string,
    "data-test-subj": PropTypes.string
  }),
  size: PropTypes.oneOf(["s", "m", "l", "xl"]),

  /**
       * Each tab needs id and content properties, so we can associate it with its panel for accessibility.
       * The name property (a node) is also required to display to the user.
       */
  tabs: PropTypes.arrayOf(PropTypes.shape({
    id: PropTypes.string.isRequired,
    name: PropTypes.node.isRequired,
    content: PropTypes.node.isRequired,
    isSelected: PropTypes.bool,
    disabled: PropTypes.bool,
    prepend: PropTypes.node,
    append: PropTypes.node,
    className: PropTypes.string,
    "aria-label": PropTypes.string,
    "data-test-subj": PropTypes.string
  }).isRequired).isRequired
};