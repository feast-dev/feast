import _extends from "@babel/runtime/helpers/extends";
import _objectWithoutProperties from "@babel/runtime/helpers/objectWithoutProperties";
import _classCallCheck from "@babel/runtime/helpers/classCallCheck";
import _createClass from "@babel/runtime/helpers/createClass";
import _assertThisInitialized from "@babel/runtime/helpers/assertThisInitialized";
import _inherits from "@babel/runtime/helpers/inherits";
import _possibleConstructorReturn from "@babel/runtime/helpers/possibleConstructorReturn";
import _getPrototypeOf from "@babel/runtime/helpers/getPrototypeOf";
import _defineProperty from "@babel/runtime/helpers/defineProperty";

function ownKeys(object, enumerableOnly) { var keys = Object.keys(object); if (Object.getOwnPropertySymbols) { var symbols = Object.getOwnPropertySymbols(object); if (enumerableOnly) symbols = symbols.filter(function (sym) { return Object.getOwnPropertyDescriptor(object, sym).enumerable; }); keys.push.apply(keys, symbols); } return keys; }

function _objectSpread(target) { for (var i = 1; i < arguments.length; i++) { var source = arguments[i] != null ? arguments[i] : {}; if (i % 2) { ownKeys(Object(source), true).forEach(function (key) { _defineProperty(target, key, source[key]); }); } else if (Object.getOwnPropertyDescriptors) { Object.defineProperties(target, Object.getOwnPropertyDescriptors(source)); } else { ownKeys(Object(source)).forEach(function (key) { Object.defineProperty(target, key, Object.getOwnPropertyDescriptor(source, key)); }); } } return target; }

function _createSuper(Derived) { var hasNativeReflectConstruct = _isNativeReflectConstruct(); return function _createSuperInternal() { var Super = _getPrototypeOf(Derived), result; if (hasNativeReflectConstruct) { var NewTarget = _getPrototypeOf(this).constructor; result = Reflect.construct(Super, arguments, NewTarget); } else { result = Super.apply(this, arguments); } return _possibleConstructorReturn(this, result); }; }

function _isNativeReflectConstruct() { if (typeof Reflect === "undefined" || !Reflect.construct) return false; if (Reflect.construct.sham) return false; if (typeof Proxy === "function") return true; try { Date.prototype.toString.call(Reflect.construct(Date, [], function () {})); return true; } catch (e) { return false; } }

/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */
import React, { Component, createRef } from 'react';
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