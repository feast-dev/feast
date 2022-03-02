import _extends from "@babel/runtime/helpers/extends";
import _objectWithoutProperties from "@babel/runtime/helpers/objectWithoutProperties";
import _classCallCheck from "@babel/runtime/helpers/classCallCheck";
import _createClass from "@babel/runtime/helpers/createClass";
import _inherits from "@babel/runtime/helpers/inherits";
import _possibleConstructorReturn from "@babel/runtime/helpers/possibleConstructorReturn";
import _getPrototypeOf from "@babel/runtime/helpers/getPrototypeOf";

function _createSuper(Derived) { var hasNativeReflectConstruct = _isNativeReflectConstruct(); return function _createSuperInternal() { var Super = _getPrototypeOf(Derived), result; if (hasNativeReflectConstruct) { var NewTarget = _getPrototypeOf(this).constructor; result = Reflect.construct(Super, arguments, NewTarget); } else { result = Super.apply(this, arguments); } return _possibleConstructorReturn(this, result); }; }

function _isNativeReflectConstruct() { if (typeof Reflect === "undefined" || !Reflect.construct) return false; if (Reflect.construct.sham) return false; if (typeof Proxy === "function") return true; try { Date.prototype.toString.call(Reflect.construct(Date, [], function () {})); return true; } catch (e) { return false; } }

/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */
import React, { Fragment, Component } from 'react';
import { EuiLoadingSpinner } from '../../loading';
import { EuiFormControlLayoutClearButton } from './form_control_layout_clear_button';
import { EuiFormControlLayoutCustomIcon } from './form_control_layout_custom_icon';
import { jsx as ___EmotionJSX } from "@emotion/react";
export var ICON_SIDES = ['left', 'right'];

function isIconShape(icon) {
  return !!icon && icon.hasOwnProperty('type');
}

export var EuiFormControlLayoutIcons = /*#__PURE__*/function (_Component) {
  _inherits(EuiFormControlLayoutIcons, _Component);

  var _super = _createSuper(EuiFormControlLayoutIcons);

  function EuiFormControlLayoutIcons() {
    _classCallCheck(this, EuiFormControlLayoutIcons);

    return _super.apply(this, arguments);
  }

  _createClass(EuiFormControlLayoutIcons, [{
    key: "render",
    value: function render() {
      var icon = this.props.icon;
      var iconSide = isIconShape(icon) && icon.side ? icon.side : 'left';
      var customIcon = this.renderCustomIcon();
      var loadingSpinner = this.renderLoadingSpinner();
      var clearButton = this.renderClearButton();
      var leftIcons;

      if (customIcon && iconSide === 'left') {
        leftIcons = ___EmotionJSX("div", {
          className: "euiFormControlLayoutIcons"
        }, customIcon);
      }

      var rightIcons; // If the icon is on the right, it should be placed after the clear button in the DOM.

      if (clearButton || loadingSpinner || customIcon && iconSide === 'right') {
        rightIcons = ___EmotionJSX("div", {
          className: "euiFormControlLayoutIcons euiFormControlLayoutIcons--right"
        }, clearButton, loadingSpinner, iconSide === 'right' ? customIcon : undefined);
      }

      return ___EmotionJSX(Fragment, null, leftIcons, rightIcons);
    }
  }, {
    key: "renderCustomIcon",
    value: function renderCustomIcon() {
      var _this$props = this.props,
          icon = _this$props.icon,
          compressed = _this$props.compressed;

      if (!icon) {
        return null;
      } // Normalize the icon to an object if it's a string.


      var iconProps = isIconShape(icon) ? icon : {
        type: icon
      };

      var iconRef = iconProps.ref,
          side = iconProps.side,
          iconRest = _objectWithoutProperties(iconProps, ["ref", "side"]);

      return ___EmotionJSX(EuiFormControlLayoutCustomIcon, _extends({
        size: compressed ? 's' : 'm',
        iconRef: iconRef
      }, iconRest));
    }
  }, {
    key: "renderLoadingSpinner",
    value: function renderLoadingSpinner() {
      var _this$props2 = this.props,
          isLoading = _this$props2.isLoading,
          compressed = _this$props2.compressed;

      if (!isLoading) {
        return null;
      }

      return ___EmotionJSX(EuiLoadingSpinner, {
        size: compressed ? 's' : 'm'
      });
    }
  }, {
    key: "renderClearButton",
    value: function renderClearButton() {
      var _this$props3 = this.props,
          clear = _this$props3.clear,
          compressed = _this$props3.compressed;

      if (!clear) {
        return null;
      }

      return ___EmotionJSX(EuiFormControlLayoutClearButton, _extends({
        size: compressed ? 's' : 'm'
      }, clear));
    }
  }]);

  return EuiFormControlLayoutIcons;
}(Component);