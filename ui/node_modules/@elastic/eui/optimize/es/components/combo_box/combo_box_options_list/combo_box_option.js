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
import { keys } from '../../../services';
import { jsx as ___EmotionJSX } from "@emotion/react";
export var EuiComboBoxOption = /*#__PURE__*/function (_Component) {
  _inherits(EuiComboBoxOption, _Component);

  var _super = _createSuper(EuiComboBoxOption);

  function EuiComboBoxOption() {
    var _this;

    _classCallCheck(this, EuiComboBoxOption);

    for (var _len = arguments.length, args = new Array(_len), _key = 0; _key < _len; _key++) {
      args[_key] = arguments[_key];
    }

    _this = _super.call.apply(_super, [this].concat(args));

    _defineProperty(_assertThisInitialized(_this), "onClick", function () {
      var _this$props = _this.props,
          onClick = _this$props.onClick,
          option = _this$props.option,
          disabled = _this$props.disabled;

      if (disabled) {
        return;
      }

      onClick(option);
    });

    _defineProperty(_assertThisInitialized(_this), "onKeyDown", function (event) {
      if (event.key === keys.ENTER || event.key === keys.SPACE) {
        event.preventDefault();
        event.stopPropagation();
        var _this$props2 = _this.props,
            onEnterKey = _this$props2.onEnterKey,
            option = _this$props2.option,
            disabled = _this$props2.disabled;

        if (disabled) {
          return;
        }

        onEnterKey(option);
      }
    });

    return _this;
  }

  _createClass(EuiComboBoxOption, [{
    key: "render",
    value: function render() {
      var _this$props3 = this.props,
          children = _this$props3.children,
          className = _this$props3.className,
          disabled = _this$props3.disabled,
          isFocused = _this$props3.isFocused,
          onClick = _this$props3.onClick,
          onEnterKey = _this$props3.onEnterKey,
          option = _this$props3.option,
          optionRef = _this$props3.optionRef,
          rest = _objectWithoutProperties(_this$props3, ["children", "className", "disabled", "isFocused", "onClick", "onEnterKey", "option", "optionRef"]);

      var classes = classNames('euiComboBoxOption', className, {
        'euiComboBoxOption-isDisabled': disabled,
        'euiComboBoxOption-isFocused': isFocused
      });
      var label = option.label;
      return ___EmotionJSX("button", _extends({
        "aria-disabled": disabled,
        "aria-selected": isFocused,
        className: classes,
        onClick: this.onClick,
        onKeyDown: this.onKeyDown,
        ref: optionRef,
        role: "option",
        title: label,
        type: "button"
      }, rest), children);
    }
  }]);

  return EuiComboBoxOption;
}(Component);