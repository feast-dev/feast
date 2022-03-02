import _extends from "@babel/runtime/helpers/extends";
import _objectWithoutProperties from "@babel/runtime/helpers/objectWithoutProperties";
import _classCallCheck from "@babel/runtime/helpers/classCallCheck";
import _createClass from "@babel/runtime/helpers/createClass";
import _assertThisInitialized from "@babel/runtime/helpers/assertThisInitialized";
import _inherits from "@babel/runtime/helpers/inherits";
import _possibleConstructorReturn from "@babel/runtime/helpers/possibleConstructorReturn";
import _getPrototypeOf from "@babel/runtime/helpers/getPrototypeOf";
import _defineProperty from "@babel/runtime/helpers/defineProperty";

function ownKeys(object, enumerableOnly) { var keys = Object.keys(object); if (Object.getOwnPropertySymbols) { var symbols = Object.getOwnPropertySymbols(object); if (enumerableOnly) symbols = symbols.filter(function (sym) { return Object.getOwnPropertyDescriptor(object, sym).enumerable; }); keys.push.apply(keys, symbols); } return keys; }

function _objectSpread(target) { for (var i = 1; i < arguments.length; i++) { var source = arguments[i] != null ? arguments[i] : {}; if (i % 2) { ownKeys(Object(source), true).forEach(function (key) { _defineProperty(target, key, source[key]); }); } else if (Object.getOwnPropertyDescriptors) { Object.defineProperties(target, Object.getOwnPropertyDescriptors(source)); } else { ownKeys(Object(source)).forEach(function (key) { Object.defineProperty(target, key, Object.getOwnPropertyDescriptor(source, key)); }); } } return target; }

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
import { EuiButton } from '../../button';
import { EuiI18n } from '../../i18n';
import { EuiToolTip } from '../../tool_tip';
import { EuiHideFor, EuiShowFor } from '../../responsive';
import { jsx as ___EmotionJSX } from "@emotion/react";
export var EuiSuperUpdateButton = /*#__PURE__*/function (_Component) {
  _inherits(EuiSuperUpdateButton, _Component);

  var _super = _createSuper(EuiSuperUpdateButton);

  function EuiSuperUpdateButton() {
    var _this;

    _classCallCheck(this, EuiSuperUpdateButton);

    for (var _len = arguments.length, args = new Array(_len), _key = 0; _key < _len; _key++) {
      args[_key] = arguments[_key];
    }

    _this = _super.call.apply(_super, [this].concat(args));

    _defineProperty(_assertThisInitialized(_this), "_isMounted", false);

    _defineProperty(_assertThisInitialized(_this), "tooltipTimeout", void 0);

    _defineProperty(_assertThisInitialized(_this), "tooltip", null);

    _defineProperty(_assertThisInitialized(_this), "setTootipRef", function (node) {
      _this.tooltip = node;
    });

    _defineProperty(_assertThisInitialized(_this), "showTooltip", function () {
      if (!_this._isMounted || !_this.tooltip) {
        return;
      }

      _this.tooltip.showToolTip();
    });

    _defineProperty(_assertThisInitialized(_this), "hideTooltip", function () {
      if (!_this._isMounted || !_this.tooltip) {
        return;
      }

      _this.tooltip.hideToolTip();
    });

    return _this;
  }

  _createClass(EuiSuperUpdateButton, [{
    key: "componentWillUnmount",
    value: function componentWillUnmount() {
      this._isMounted = false;
    }
  }, {
    key: "componentDidMount",
    value: function componentDidMount() {
      this._isMounted = true;
    }
  }, {
    key: "componentDidUpdate",
    value: function componentDidUpdate() {
      var _this2 = this;

      if (this.props.showTooltip && this.props.needsUpdate && !this.props.isDisabled && !this.props.isLoading) {
        this.showTooltip();
        this.tooltipTimeout = setTimeout(function () {
          _this2.hideTooltip();
        }, 2000);
      }
    }
  }, {
    key: "render",
    value: function render() {
      var _this$props = this.props,
          className = _this$props.className,
          needsUpdate = _this$props.needsUpdate,
          isLoading = _this$props.isLoading,
          isDisabled = _this$props.isDisabled,
          onClick = _this$props.onClick,
          toolTipProps = _this$props.toolTipProps,
          showTooltip = _this$props.showTooltip,
          iconOnly = _this$props.iconOnly,
          _responsive = _this$props.responsive,
          restTextProps = _this$props.textProps,
          fill = _this$props.fill,
          rest = _objectWithoutProperties(_this$props, ["className", "needsUpdate", "isLoading", "isDisabled", "onClick", "toolTipProps", "showTooltip", "iconOnly", "responsive", "textProps", "fill"]); // Force responsive for "all" if `iconOnly = true`


      var responsive = iconOnly ? 'all' : _responsive;
      var classes = classNames('euiSuperUpdateButton', className);

      var buttonText = ___EmotionJSX(EuiI18n, {
        token: "euiSuperUpdateButton.refreshButtonLabel",
        default: "Refresh"
      });

      if (needsUpdate || isLoading) {
        buttonText = isLoading ? ___EmotionJSX(EuiI18n, {
          token: "euiSuperUpdateButton.updatingButtonLabel",
          default: "Updating"
        }) : ___EmotionJSX(EuiI18n, {
          token: "euiSuperUpdateButton.updateButtonLabel",
          default: "Update"
        });
      }

      var tooltipContent;

      if (isDisabled) {
        tooltipContent = ___EmotionJSX(EuiI18n, {
          token: "euiSuperUpdateButton.cannotUpdateTooltip",
          default: "Cannot update"
        });
      } else if (needsUpdate && !isLoading) {
        tooltipContent = ___EmotionJSX(EuiI18n, {
          token: "euiSuperUpdateButton.clickToApplyTooltip",
          default: "Click to apply"
        });
      }

      var sharedButtonProps = {
        color: needsUpdate || isLoading ? 'success' : 'primary',
        iconType: needsUpdate || isLoading ? 'kqlFunction' : 'refresh',
        isDisabled: isDisabled,
        onClick: onClick,
        isLoading: isLoading
      };
      return ___EmotionJSX(EuiToolTip, _extends({
        ref: this.setTootipRef,
        content: tooltipContent,
        position: "bottom"
      }, toolTipProps), ___EmotionJSX(React.Fragment, null, ___EmotionJSX(EuiShowFor, {
        sizes: responsive || 'none'
      }, ___EmotionJSX(EuiButton, _extends({
        className: classes,
        minWidth: 0
      }, sharedButtonProps, {
        fill: fill,
        textProps: _objectSpread(_objectSpread({}, restTextProps), {}, {
          className: classNames('euiScreenReaderOnly', restTextProps === null || restTextProps === void 0 ? void 0 : restTextProps.className)
        })
      }, rest), buttonText)), ___EmotionJSX(EuiHideFor, {
        sizes: responsive || 'none'
      }, ___EmotionJSX(EuiButton, _extends({
        className: classes,
        minWidth: 118
      }, sharedButtonProps, {
        fill: fill,
        textProps: restTextProps
      }, rest), buttonText))));
    }
  }]);

  return EuiSuperUpdateButton;
}(Component);

_defineProperty(EuiSuperUpdateButton, "defaultProps", {
  needsUpdate: false,
  isLoading: false,
  isDisabled: false,
  showTooltip: false,
  responsive: ['xs', 's'],
  fill: true
});