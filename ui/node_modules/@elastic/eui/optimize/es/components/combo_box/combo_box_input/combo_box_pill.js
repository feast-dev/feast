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
import { EuiBadge } from '../../badge';
import { EuiI18n } from '../../i18n';
import { jsx as ___EmotionJSX } from "@emotion/react";
export var EuiComboBoxPill = /*#__PURE__*/function (_Component) {
  _inherits(EuiComboBoxPill, _Component);

  var _super = _createSuper(EuiComboBoxPill);

  function EuiComboBoxPill() {
    var _this;

    _classCallCheck(this, EuiComboBoxPill);

    for (var _len = arguments.length, args = new Array(_len), _key = 0; _key < _len; _key++) {
      args[_key] = arguments[_key];
    }

    _this = _super.call.apply(_super, [this].concat(args));

    _defineProperty(_assertThisInitialized(_this), "onCloseButtonClick", function () {
      var _this$props = _this.props,
          onClose = _this$props.onClose,
          option = _this$props.option;

      if (onClose) {
        onClose(option);
      }
    });

    return _this;
  }

  _createClass(EuiComboBoxPill, [{
    key: "render",
    value: function render() {
      var _this2 = this;

      var _this$props2 = this.props,
          asPlainText = _this$props2.asPlainText,
          children = _this$props2.children,
          className = _this$props2.className,
          color = _this$props2.color,
          onClick = _this$props2.onClick,
          onClickAriaLabel = _this$props2.onClickAriaLabel,
          onClose = _this$props2.onClose,
          option = _this$props2.option,
          rest = _objectWithoutProperties(_this$props2, ["asPlainText", "children", "className", "color", "onClick", "onClickAriaLabel", "onClose", "option"]);

      var classes = classNames('euiComboBoxPill', {
        'euiComboBoxPill--plainText': asPlainText
      }, className);
      var onClickProps = onClick && onClickAriaLabel ? {
        onClick: onClick,
        onClickAriaLabel: onClickAriaLabel
      } : {};

      if (onClose) {
        return ___EmotionJSX(EuiI18n, {
          token: "euiComboBoxPill.removeSelection",
          default: "Remove {children} from selection in this group",
          values: {
            children: children
          }
        }, function (removeSelection) {
          return ___EmotionJSX(EuiBadge, _extends({
            className: classes,
            closeButtonProps: {
              tabIndex: -1
            },
            color: color,
            iconOnClick: _this2.onCloseButtonClick,
            iconOnClickAriaLabel: removeSelection,
            iconSide: "right",
            iconType: "cross",
            title: children
          }, onClickProps, rest), children);
        });
      }

      if (asPlainText) {
        return ___EmotionJSX("span", _extends({
          className: classes
        }, rest), children);
      }

      return ___EmotionJSX(EuiBadge, _extends({
        className: classes,
        color: color,
        title: children
      }, rest, onClickProps), children);
    }
  }]);

  return EuiComboBoxPill;
}(Component);

_defineProperty(EuiComboBoxPill, "defaultProps", {
  color: 'hollow'
});