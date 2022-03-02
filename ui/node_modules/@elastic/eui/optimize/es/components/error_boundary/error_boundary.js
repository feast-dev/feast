import _extends from "@babel/runtime/helpers/extends";
import _objectWithoutProperties from "@babel/runtime/helpers/objectWithoutProperties";
import _classCallCheck from "@babel/runtime/helpers/classCallCheck";
import _createClass from "@babel/runtime/helpers/createClass";
import _inherits from "@babel/runtime/helpers/inherits";
import _possibleConstructorReturn from "@babel/runtime/helpers/possibleConstructorReturn";
import _getPrototypeOf from "@babel/runtime/helpers/getPrototypeOf";

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
import { EuiTitle } from '../title';
import { EuiCodeBlock } from '../code';
import { EuiI18n } from '../i18n';
import { jsx as ___EmotionJSX } from "@emotion/react";
export var EuiErrorBoundary = /*#__PURE__*/function (_Component) {
  _inherits(EuiErrorBoundary, _Component);

  var _super = _createSuper(EuiErrorBoundary);

  function EuiErrorBoundary(props) {
    var _this;

    _classCallCheck(this, EuiErrorBoundary);

    _this = _super.call(this, props);
    var errorState = {
      hasError: false,
      error: undefined
    };
    _this.state = errorState;
    return _this;
  }

  _createClass(EuiErrorBoundary, [{
    key: "componentDidCatch",
    value: function componentDidCatch(_ref) {
      var message = _ref.message,
          stack = _ref.stack;
      // Display fallback UI
      // Only Chrome includes the `message` property as part of `stack`.
      // For consistency, rebuild the full error text from the Error subparts.
      var idx = (stack === null || stack === void 0 ? void 0 : stack.indexOf(message)) || -1;
      var stackStr = idx > -1 ? stack === null || stack === void 0 ? void 0 : stack.substr(idx + message.length + 1) : stack;
      var error = "Error: ".concat(message, "\n").concat(stackStr);
      this.setState({
        hasError: true,
        error: error
      });
    }
  }, {
    key: "render",
    value: function render() {
      var _this$props = this.props,
          children = _this$props.children,
          _dataTestSubj = _this$props['data-test-subj'],
          rest = _objectWithoutProperties(_this$props, ["children", "data-test-subj"]);

      var dataTestSubj = classNames('euiErrorBoundary', _dataTestSubj);

      if (this.state.hasError) {
        // You can render any custom fallback UI
        return ___EmotionJSX("div", _extends({
          className: "euiErrorBoundary",
          "data-test-subj": dataTestSubj
        }, rest), ___EmotionJSX(EuiCodeBlock, null, ___EmotionJSX(EuiTitle, {
          size: "xs"
        }, ___EmotionJSX("p", null, ___EmotionJSX(EuiI18n, {
          token: "euiErrorBoundary.error",
          default: "Error"
        }))), this.state.error));
      }

      return children;
    }
  }]);

  return EuiErrorBoundary;
}(Component);