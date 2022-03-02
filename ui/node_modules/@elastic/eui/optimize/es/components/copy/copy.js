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
import { copyToClipboard } from '../../services';
import { EuiToolTip } from '../tool_tip';
import { jsx as ___EmotionJSX } from "@emotion/react";
export var EuiCopy = /*#__PURE__*/function (_Component) {
  _inherits(EuiCopy, _Component);

  var _super = _createSuper(EuiCopy);

  function EuiCopy(props) {
    var _this;

    _classCallCheck(this, EuiCopy);

    _this = _super.call(this, props);

    _defineProperty(_assertThisInitialized(_this), "copy", function () {
      var isCopied = copyToClipboard(_this.props.textToCopy);

      if (isCopied) {
        _this.setState({
          tooltipText: _this.props.afterMessage
        });
      }
    });

    _defineProperty(_assertThisInitialized(_this), "resetTooltipText", function () {
      _this.setState({
        tooltipText: _this.props.beforeMessage
      });
    });

    _this.state = {
      tooltipText: _this.props.beforeMessage
    };
    return _this;
  }

  _createClass(EuiCopy, [{
    key: "render",
    value: function render() {
      var _this$props = this.props,
          children = _this$props.children,
          textToCopy = _this$props.textToCopy,
          beforeMessage = _this$props.beforeMessage,
          afterMessage = _this$props.afterMessage,
          rest = _objectWithoutProperties(_this$props, ["children", "textToCopy", "beforeMessage", "afterMessage"]);

      return (// See `src/components/tool_tip/tool_tip.js` for explanation of below eslint-disable
        // eslint-disable-next-line jsx-a11y/mouse-events-have-key-events
        ___EmotionJSX(EuiToolTip, _extends({
          content: this.state.tooltipText,
          onMouseOut: this.resetTooltipText
        }, rest), children(this.copy))
      );
    }
  }]);

  return EuiCopy;
}(Component);

_defineProperty(EuiCopy, "defaultProps", {
  afterMessage: 'Copied'
});