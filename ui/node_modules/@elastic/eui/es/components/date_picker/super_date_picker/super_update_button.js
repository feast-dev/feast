function _typeof(obj) { "@babel/helpers - typeof"; if (typeof Symbol === "function" && typeof Symbol.iterator === "symbol") { _typeof = function _typeof(obj) { return typeof obj; }; } else { _typeof = function _typeof(obj) { return obj && typeof Symbol === "function" && obj.constructor === Symbol && obj !== Symbol.prototype ? "symbol" : typeof obj; }; } return _typeof(obj); }

function _extends() { _extends = Object.assign || function (target) { for (var i = 1; i < arguments.length; i++) { var source = arguments[i]; for (var key in source) { if (Object.prototype.hasOwnProperty.call(source, key)) { target[key] = source[key]; } } } return target; }; return _extends.apply(this, arguments); }

function ownKeys(object, enumerableOnly) { var keys = Object.keys(object); if (Object.getOwnPropertySymbols) { var symbols = Object.getOwnPropertySymbols(object); if (enumerableOnly) symbols = symbols.filter(function (sym) { return Object.getOwnPropertyDescriptor(object, sym).enumerable; }); keys.push.apply(keys, symbols); } return keys; }

function _objectSpread(target) { for (var i = 1; i < arguments.length; i++) { var source = arguments[i] != null ? arguments[i] : {}; if (i % 2) { ownKeys(Object(source), true).forEach(function (key) { _defineProperty(target, key, source[key]); }); } else if (Object.getOwnPropertyDescriptors) { Object.defineProperties(target, Object.getOwnPropertyDescriptors(source)); } else { ownKeys(Object(source)).forEach(function (key) { Object.defineProperty(target, key, Object.getOwnPropertyDescriptor(source, key)); }); } } return target; }

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

EuiSuperUpdateButton.propTypes = {
  isDisabled: PropTypes.bool,
  isLoading: PropTypes.bool,
  needsUpdate: PropTypes.bool,
  onClick: PropTypes.func.isRequired,

  /**
     * Show the "Click to apply" tooltip
     */
  showTooltip: PropTypes.bool,

  /**
     * Passes props to `EuiToolTip`
     */
  toolTipProps: PropTypes.shape({
    /**
       * Passes onto the the trigger.
       */
    anchorClassName: PropTypes.string,

    /**
       * The in-view trigger for your tooltip.
       */
    children: PropTypes.element.isRequired,

    /**
       * Passes onto the tooltip itself, not the trigger.
       */
    className: PropTypes.string,

    /**
       * The main content of your tooltip.
       */
    content: PropTypes.node,

    /**
       * Common display alternatives for the anchor wrapper
       */
    display: PropTypes.oneOf(["inlineBlock", "block"]),

    /**
       * Delay before showing tooltip. Good for repeatable items.
       */
    delay: PropTypes.oneOf(["regular", "long"]).isRequired,

    /**
       * An optional title for your tooltip.
       */
    title: PropTypes.node,

    /**
       * Unless you provide one, this will be randomly generated.
       */
    id: PropTypes.string,

    /**
       * Suggested position. If there is not enough room for it this will be changed.
       */
    position: PropTypes.oneOf(["top", "right", "bottom", "left"]).isRequired,

    /**
       * If supplied, called when mouse movement causes the tool tip to be
       * hidden.
       */
    onMouseOut: PropTypes.func
  }),

  /**
     * Returns an IconButton instead
     */
  iconOnly: PropTypes.bool,

  /**
     * Forces state to be `iconOnly` when within provided breakpoints.
     * Remove completely with `false` or provide your own list of breakpoints.
     */
  responsive: PropTypes.oneOfType([PropTypes.oneOf([false]), PropTypes.arrayOf(PropTypes.oneOf(["xs", "s", "m", "l", "xl"]).isRequired).isRequired])
};