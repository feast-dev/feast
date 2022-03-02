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
import { keysOf } from '../../common';
import { jsx as ___EmotionJSX } from "@emotion/react";
var typeToClassNameMap = {
  inList: 'euiCheckbox--inList'
};
export var TYPES = keysOf(typeToClassNameMap);
export var EuiCheckbox = /*#__PURE__*/function (_Component) {
  _inherits(EuiCheckbox, _Component);

  var _super = _createSuper(EuiCheckbox);

  function EuiCheckbox() {
    var _this;

    _classCallCheck(this, EuiCheckbox);

    for (var _len = arguments.length, args = new Array(_len), _key = 0; _key < _len; _key++) {
      args[_key] = arguments[_key];
    }

    _this = _super.call.apply(_super, [this].concat(args));

    _defineProperty(_assertThisInitialized(_this), "inputRef", undefined);

    _defineProperty(_assertThisInitialized(_this), "setInputRef", function (input) {
      _this.inputRef = input;

      if (_this.props.inputRef) {
        _this.props.inputRef(input);
      }

      _this.invalidateIndeterminate();
    });

    return _this;
  }

  _createClass(EuiCheckbox, [{
    key: "componentDidMount",
    value: function componentDidMount() {
      this.invalidateIndeterminate();
    }
  }, {
    key: "componentDidUpdate",
    value: function componentDidUpdate() {
      this.invalidateIndeterminate();
    }
  }, {
    key: "render",
    value: function render() {
      var _this$props = this.props,
          className = _this$props.className,
          id = _this$props.id,
          checked = _this$props.checked,
          label = _this$props.label,
          onChange = _this$props.onChange,
          type = _this$props.type,
          disabled = _this$props.disabled,
          compressed = _this$props.compressed,
          indeterminate = _this$props.indeterminate,
          inputRef = _this$props.inputRef,
          labelProps = _this$props.labelProps,
          rest = _objectWithoutProperties(_this$props, ["className", "id", "checked", "label", "onChange", "type", "disabled", "compressed", "indeterminate", "inputRef", "labelProps"]);

      var classes = classNames('euiCheckbox', type && typeToClassNameMap[type], {
        'euiCheckbox--noLabel': !label,
        'euiCheckbox--compressed': compressed
      }, className);
      var labelClasses = classNames('euiCheckbox__label', labelProps === null || labelProps === void 0 ? void 0 : labelProps.className);
      var optionalLabel;

      if (label) {
        optionalLabel = ___EmotionJSX("label", _extends({}, labelProps, {
          className: labelClasses,
          htmlFor: id
        }), label);
      }

      return ___EmotionJSX("div", {
        className: classes
      }, ___EmotionJSX("input", _extends({
        className: "euiCheckbox__input",
        type: "checkbox",
        id: id,
        checked: checked,
        onChange: onChange,
        disabled: disabled,
        ref: this.setInputRef
      }, rest)), ___EmotionJSX("div", {
        className: "euiCheckbox__square"
      }), optionalLabel);
    }
  }, {
    key: "invalidateIndeterminate",
    value: function invalidateIndeterminate() {
      if (this.inputRef) {
        this.inputRef.indeterminate = this.props.indeterminate;
      }
    }
  }]);

  return EuiCheckbox;
}(Component);

_defineProperty(EuiCheckbox, "defaultProps", {
  checked: false,
  disabled: false,
  indeterminate: false,
  compressed: false
});