function _typeof(obj) { "@babel/helpers - typeof"; if (typeof Symbol === "function" && typeof Symbol.iterator === "symbol") { _typeof = function _typeof(obj) { return typeof obj; }; } else { _typeof = function _typeof(obj) { return obj && typeof Symbol === "function" && obj.constructor === Symbol && obj !== Symbol.prototype ? "symbol" : typeof obj; }; } return _typeof(obj); }

function _extends() { _extends = Object.assign || function (target) { for (var i = 1; i < arguments.length; i++) { var source = arguments[i]; for (var key in source) { if (Object.prototype.hasOwnProperty.call(source, key)) { target[key] = source[key]; } } } return target; }; return _extends.apply(this, arguments); }

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
import React, { Component } from 'react';
import PropTypes from "prop-types";
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

EuiCheckbox.propTypes = {
  id: PropTypes.string.isRequired,
  checked: PropTypes.bool,
  onChange: PropTypes.any.isRequired,
  // overriding to make it required
  inputRef: PropTypes.func,
  label: PropTypes.node,
  type: PropTypes.oneOf(["inList"]),
  disabled: PropTypes.bool,

  /**
     * when `true` creates a shorter height checkbox row
     */
  compressed: PropTypes.bool,
  indeterminate: PropTypes.bool,

  /**
     * Object of props passed to the <label/>
     */
  labelProps: PropTypes.shape({
    className: PropTypes.string,
    "aria-label": PropTypes.string,
    "data-test-subj": PropTypes.string
  }),
  className: PropTypes.string,
  "aria-label": PropTypes.string,
  "data-test-subj": PropTypes.string
};