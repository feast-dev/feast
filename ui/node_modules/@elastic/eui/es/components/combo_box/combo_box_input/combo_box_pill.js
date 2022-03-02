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

EuiComboBoxPill.propTypes = {
  asPlainText: PropTypes.bool,
  children: PropTypes.string,
  className: PropTypes.string,
  color: PropTypes.string,
  onClick: PropTypes.func,
  onClickAriaLabel: PropTypes.any,
  onClose: PropTypes.func,
  option: PropTypes.shape({
    isGroupLabelOption: PropTypes.bool,
    label: PropTypes.string.isRequired,
    key: PropTypes.string,
    options: PropTypes.arrayOf(PropTypes.shape({
      isGroupLabelOption: PropTypes.bool,
      label: PropTypes.string.isRequired,
      key: PropTypes.string,
      options: PropTypes.arrayOf(PropTypes.any.isRequired),
      value: PropTypes.any,
      className: PropTypes.string,
      "aria-label": PropTypes.string,
      "data-test-subj": PropTypes.string
    }).isRequired),
    value: PropTypes.any,
    className: PropTypes.string,
    "aria-label": PropTypes.string,
    "data-test-subj": PropTypes.string
  }).isRequired,
  "aria-label": PropTypes.string,
  "data-test-subj": PropTypes.string
};