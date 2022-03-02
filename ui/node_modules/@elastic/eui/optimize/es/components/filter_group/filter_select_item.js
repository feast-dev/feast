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