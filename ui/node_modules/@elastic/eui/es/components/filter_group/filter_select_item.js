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
import { EuiFlexGroup, EuiFlexItem } from '../flex';
import { EuiIcon } from '../icon';
import { jsx as ___EmotionJSX } from "@emotion/react";

var resolveIconAndColor = function resolveIconAndColor(checked) {
  if (!checked) {
    return {
      icon: 'empty'
    };
  }

  return checked === 'on' ? {
    icon: 'check',
    color: 'text'
  } : {
    icon: 'cross',
    color: 'text'
  };
};

export var EuiFilterSelectItem = /*#__PURE__*/function (_Component) {
  _inherits(EuiFilterSelectItem, _Component);

  var _super = _createSuper(EuiFilterSelectItem);

  function EuiFilterSelectItem() {
    var _this;

    _classCallCheck(this, EuiFilterSelectItem);

    for (var _len = arguments.length, args = new Array(_len), _key = 0; _key < _len; _key++) {
      args[_key] = arguments[_key];
    }

    _this = _super.call.apply(_super, [this].concat(args));

    _defineProperty(_assertThisInitialized(_this), "buttonRef", null);

    _defineProperty(_assertThisInitialized(_this), "state", {
      hasFocus: false
    });

    _defineProperty(_assertThisInitialized(_this), "focus", function () {
      if (_this.buttonRef) {
        _this.buttonRef.focus();
      }
    });

    _defineProperty(_assertThisInitialized(_this), "hasFocus", function () {
      return _this.state.hasFocus;
    });

    return _this;
  }

  _createClass(EuiFilterSelectItem, [{
    key: "render",
    value: function render() {
      var _this2 = this;

      var _this$props = this.props,
          children = _this$props.children,
          className = _this$props.className,
          disabled = _this$props.disabled,
          checked = _this$props.checked,
          isFocused = _this$props.isFocused,
          showIcons = _this$props.showIcons,
          rest = _objectWithoutProperties(_this$props, ["children", "className", "disabled", "checked", "isFocused", "showIcons"]);

      var classes = classNames('euiFilterSelectItem', {
        'euiFilterSelectItem-isFocused': isFocused
      }, className);
      var iconNode;

      if (showIcons) {
        var _resolveIconAndColor = resolveIconAndColor(checked),
            icon = _resolveIconAndColor.icon,
            color = _resolveIconAndColor.color;

        iconNode = ___EmotionJSX(EuiFlexItem, {
          grow: false
        }, ___EmotionJSX(EuiIcon, {
          color: color,
          type: icon
        }));
      }

      return ___EmotionJSX("button", _extends({
        ref: function ref(_ref) {
          return _this2.buttonRef = _ref;
        },
        role: "option",
        type: "button",
        "aria-selected": isFocused,
        className: classes,
        disabled: disabled,
        "aria-disabled": disabled
      }, rest), ___EmotionJSX(EuiFlexGroup, {
        alignItems: "center",
        gutterSize: "s",
        component: "span",
        responsive: false
      }, iconNode, ___EmotionJSX(EuiFlexItem, {
        className: "euiFilterSelectItem__content",
        component: "span"
      }, children)));
    }
  }]);

  return EuiFilterSelectItem;
}(Component);

_defineProperty(EuiFilterSelectItem, "defaultProps", {
  showIcons: true
});

EuiFilterSelectItem.propTypes = {
  checked: PropTypes.oneOf(["on", "off"]),
  showIcons: PropTypes.bool,
  isFocused: PropTypes.bool,
  className: PropTypes.string,
  "aria-label": PropTypes.string,
  "data-test-subj": PropTypes.string
};