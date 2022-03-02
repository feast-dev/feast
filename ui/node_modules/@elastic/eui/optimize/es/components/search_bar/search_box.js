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
import { EuiFieldSearch } from '../form';
import { jsx as ___EmotionJSX } from "@emotion/react";
export var EuiSearchBox = /*#__PURE__*/function (_Component) {
  _inherits(EuiSearchBox, _Component);

  var _super = _createSuper(EuiSearchBox);

  function EuiSearchBox() {
    var _this;

    _classCallCheck(this, EuiSearchBox);

    for (var _len = arguments.length, args = new Array(_len), _key = 0; _key < _len; _key++) {
      args[_key] = arguments[_key];
    }

    _this = _super.call.apply(_super, [this].concat(args));

    _defineProperty(_assertThisInitialized(_this), "inputElement", null);

    return _this;
  }

  _createClass(EuiSearchBox, [{
    key: "componentDidUpdate",
    value: function componentDidUpdate(oldProps) {
      if (oldProps.query !== this.props.query && this.inputElement != null) {
        this.inputElement.value = this.props.query;
        this.inputElement.dispatchEvent(new Event('change'));
      }
    }
  }, {
    key: "render",
    value: function render() {
      var _this2 = this;

      var _this$props = this.props,
          query = _this$props.query,
          incremental = _this$props.incremental,
          rest = _objectWithoutProperties(_this$props, ["query", "incremental"]);

      var ariaLabel;

      if (incremental) {
        ariaLabel = 'This is a search bar. As you type, the results lower in the page will automatically filter.';
      } else {
        ariaLabel = 'This is a search bar. After typing your query, hit enter to filter the results lower in the page.';
      }

      return ___EmotionJSX(EuiFieldSearch, _extends({
        inputRef: function inputRef(input) {
          return _this2.inputElement = input;
        },
        fullWidth: true,
        defaultValue: query,
        incremental: incremental,
        "aria-label": ariaLabel
      }, rest));
    }
  }]);

  return EuiSearchBox;
}(Component);

_defineProperty(EuiSearchBox, "defaultProps", {
  placeholder: 'Search...',
  incremental: false
});