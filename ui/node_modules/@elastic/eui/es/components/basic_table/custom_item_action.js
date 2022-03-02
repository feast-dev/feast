function _typeof(obj) { "@babel/helpers - typeof"; if (typeof Symbol === "function" && typeof Symbol.iterator === "symbol") { _typeof = function _typeof(obj) { return typeof obj; }; } else { _typeof = function _typeof(obj) { return obj && typeof Symbol === "function" && obj.constructor === Symbol && obj !== Symbol.prototype ? "symbol" : typeof obj; }; } return _typeof(obj); }

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
import React, { Component, cloneElement } from 'react';
import PropTypes from "prop-types";
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
CustomItemAction.propTypes = {
  action: PropTypes.any.isRequired,
  enabled: PropTypes.bool.isRequired,
  item: PropTypes.any.isRequired,
  className: PropTypes.string.isRequired,
  index: PropTypes.number
};