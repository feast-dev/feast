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
import React, { Component } from 'react';
import { FocusOn } from 'react-focus-on';
import { jsx as ___EmotionJSX } from "@emotion/react";
export var EuiFocusTrap = /*#__PURE__*/function (_Component) {
  _inherits(EuiFocusTrap, _Component);

  var _super = _createSuper(EuiFocusTrap);

  function EuiFocusTrap() {
    var _this;

    _classCallCheck(this, EuiFocusTrap);

    for (var _len = arguments.length, _args = new Array(_len), _key = 0; _key < _len; _key++) {
      _args[_key] = arguments[_key];
    }

    _this = _super.call.apply(_super, [this].concat(_args));

    _defineProperty(_assertThisInitialized(_this), "state", {
      hasBeenDisabledByClick: false
    });

    _defineProperty(_assertThisInitialized(_this), "lastInterceptedEvent", null);

    _defineProperty(_assertThisInitialized(_this), "preventFocusExit", false);

    _defineProperty(_assertThisInitialized(_this), "setInitialFocus", function (initialFocus) {
      var node = initialFocus instanceof HTMLElement ? initialFocus : null;

      if (typeof initialFocus === 'string') {
        node = document.querySelector(initialFocus);
      } else if (typeof initialFocus === 'function') {
        node = initialFocus();
      }

      if (!node) return; // `data-autofocus` is part of the 'react-focus-on' API

      node.setAttribute('data-autofocus', 'true');
    });

    _defineProperty(_assertThisInitialized(_this), "handleOutsideClick", function () {
      var _this$props = _this.props,
          onClickOutside = _this$props.onClickOutside,
          clickOutsideDisables = _this$props.clickOutsideDisables;

      if (clickOutsideDisables) {
        _this.setState({
          hasBeenDisabledByClick: true
        });
      }

      if (onClickOutside) {
        onClickOutside.apply(void 0, arguments);
      }
    });

    return _this;
  }

  _createClass(EuiFocusTrap, [{
    key: "componentDidMount",
    value: function componentDidMount() {
      this.setInitialFocus(this.props.initialFocus);
    }
  }, {
    key: "componentDidUpdate",
    value: function componentDidUpdate(prevProps) {
      if (prevProps.disabled === true && this.props.disabled === false) {
        // eslint-disable-next-line react/no-did-update-set-state
        this.setState({
          hasBeenDisabledByClick: false
        });
      }
    } // Programmatically sets focus on a nested DOM node; optional

  }, {
    key: "render",
    value: function render() {
      var _this$props2 = this.props,
          children = _this$props2.children,
          clickOutsideDisables = _this$props2.clickOutsideDisables,
          disabled = _this$props2.disabled,
          returnFocus = _this$props2.returnFocus,
          noIsolation = _this$props2.noIsolation,
          scrollLock = _this$props2.scrollLock,
          rest = _objectWithoutProperties(_this$props2, ["children", "clickOutsideDisables", "disabled", "returnFocus", "noIsolation", "scrollLock"]);

      var isDisabled = disabled || this.state.hasBeenDisabledByClick;

      var focusOnProps = _objectSpread(_objectSpread({
        returnFocus: returnFocus,
        noIsolation: noIsolation,
        scrollLock: scrollLock,
        enabled: !isDisabled
      }, rest), {}, {
        onClickOutside: this.handleOutsideClick
      });

      return ___EmotionJSX(FocusOn, focusOnProps, children);
    }
  }]);

  return EuiFocusTrap;
}(Component);

_defineProperty(EuiFocusTrap, "defaultProps", {
  clickOutsideDisables: false,
  disabled: false,
  returnFocus: true,
  noIsolation: true,
  scrollLock: false
});