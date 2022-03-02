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
import React, { Component, cloneElement } from 'react';
import { jsx as ___EmotionJSX } from "@emotion/react";
export var CustomItemAction = /*#__PURE__*/function (_Component) {
  _inherits(CustomItemAction, _Component);

  var _super = _createSuper(CustomItemAction);

  function CustomItemAction(props) {
    var _this;

    _classCallCheck(this, CustomItemAction);

    _this = _super.call(this, props);

    _defineProperty(_assertThisInitialized(_this), "mounted", void 0);

    _defineProperty(_assertThisInitialized(_this), "onFocus", function () {
      if (_this.mounted) {
        _this.setState({
          hasFocus: true
        });
      }
    });

    _defineProperty(_assertThisInitialized(_this), "onBlur", function () {
      if (_this.mounted) {
        _this.setState({
          hasFocus: false
        });
      }
    });

    _defineProperty(_assertThisInitialized(_this), "hasFocus", function () {
      return _this.state.hasFocus;
    });

    _this.state = {
      hasFocus: false
    }; // while generally considered an anti-pattern, here we require
    // to do that as the onFocus/onBlur events of the action controls
    // may trigger while this component is unmounted. An alternative
    // (at least the workarounds suggested by react is to unregister
    // the onFocus/onBlur listeners from the action controls... this
    // unfortunately will lead to unnecessarily complex code... so we'll
    // stick to this approach for now)

    _this.mounted = false;
    return _this;
  }

  _createClass(CustomItemAction, [{
    key: "componentDidMount",
    value: function componentDidMount() {
      this.mounted = true;
    }
  }, {
    key: "componentWillUnmount",
    value: function componentWillUnmount() {
      this.mounted = false;
    }
  }, {
    key: "render",
    value: function render() {
      var _this$props = this.props,
          action = _this$props.action,
          enabled = _this$props.enabled,
          item = _this$props.item,
          className = _this$props.className;
      var tool = action.render(item, enabled);
      var clonedTool = /*#__PURE__*/cloneElement(tool, {
        onFocus: this.onFocus,
        onBlur: this.onBlur
      });
      var style = this.hasFocus() ? {
        opacity: 1
      } : undefined;
      return ___EmotionJSX("div", {
        style: style,
        className: className
      }, clonedTool);
    }
  }]);

  return CustomItemAction;
}(Component);