function _extends() { _extends = Object.assign || function (target) { for (var i = 1; i < arguments.length; i++) { var source = arguments[i]; for (var key in source) { if (Object.prototype.hasOwnProperty.call(source, key)) { target[key] = source[key]; } } } return target; }; return _extends.apply(this, arguments); }

function _typeof(obj) { "@babel/helpers - typeof"; if (typeof Symbol === "function" && typeof Symbol.iterator === "symbol") { _typeof = function _typeof(obj) { return typeof obj; }; } else { _typeof = function _typeof(obj) { return obj && typeof Symbol === "function" && obj.constructor === Symbol && obj !== Symbol.prototype ? "symbol" : typeof obj; }; } return _typeof(obj); }

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

/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */
import React, { cloneElement, Component } from 'react';
import PropTypes from "prop-types";
import classNames from 'classnames';
import { keysOf } from '../common';
import { EuiIcon } from '../icon';
import { EuiToolTip } from '../tool_tip';
import { getSecureRelForTarget } from '../../services';
import { validateHref } from '../../services/security/href_validator';
import { jsx as ___EmotionJSX } from "@emotion/react";
var sizeToClassNameMap = {
  s: 'euiContextMenuItem--small',
  m: null
};
export var SIZES = keysOf(sizeToClassNameMap);
var layoutAlignToClassNames = {
  center: null,
  top: 'euiContextMenu__itemLayout--top',
  bottom: 'euiContextMenu__itemLayout--bottom'
};
export var LAYOUT_ALIGN = keysOf(layoutAlignToClassNames);
export var EuiContextMenuItem = /*#__PURE__*/function (_Component) {
  _inherits(EuiContextMenuItem, _Component);

  var _super = _createSuper(EuiContextMenuItem);

  function EuiContextMenuItem() {
    _classCallCheck(this, EuiContextMenuItem);

    return _super.apply(this, arguments);
  }

  _createClass(EuiContextMenuItem, [{
    key: "render",
    value: function render() {
      var _this$props = this.props,
          children = _this$props.children,
          className = _this$props.className,
          hasPanel = _this$props.hasPanel,
          icon = _this$props.icon,
          buttonRef = _this$props.buttonRef,
          _disabled = _this$props.disabled,
          _this$props$layoutAli = _this$props.layoutAlign,
          layoutAlign = _this$props$layoutAli === void 0 ? 'center' : _this$props$layoutAli,
          toolTipTitle = _this$props.toolTipTitle,
          toolTipContent = _this$props.toolTipContent,
          _this$props$toolTipPo = _this$props.toolTipPosition,
          toolTipPosition = _this$props$toolTipPo === void 0 ? 'right' : _this$props$toolTipPo,
          href = _this$props.href,
          target = _this$props.target,
          rel = _this$props.rel,
          size = _this$props.size,
          rest = _objectWithoutProperties(_this$props, ["children", "className", "hasPanel", "icon", "buttonRef", "disabled", "layoutAlign", "toolTipTitle", "toolTipContent", "toolTipPosition", "href", "target", "rel", "size"]);

      var iconInstance;
      var isHrefValid = !href || validateHref(href);
      var disabled = _disabled || !isHrefValid;

      if (icon) {
        switch (_typeof(icon)) {
          case 'string':
            iconInstance = ___EmotionJSX(EuiIcon, {
              type: icon,
              size: "m",
              className: "euiContextMenu__icon",
              color: "inherit" // forces the icon to inherit its parent color

            });
            break;

          default:
            // Assume it's already an instance of an icon.
            iconInstance = /*#__PURE__*/cloneElement(icon, {
              className: 'euiContextMenu__icon'
            });
        }
      }

      var arrow;

      if (hasPanel) {
        arrow = ___EmotionJSX(EuiIcon, {
          type: "arrowRight",
          size: "m",
          className: "euiContextMenu__arrow"
        });
      }

      var classes = classNames('euiContextMenuItem', size && sizeToClassNameMap[size], className, {
        'euiContextMenuItem-isDisabled': disabled
      });
      var layoutClasses = classNames('euiContextMenu__itemLayout', layoutAlignToClassNames[layoutAlign]);

      var buttonInner = ___EmotionJSX("span", {
        className: layoutClasses
      }, iconInstance, ___EmotionJSX("span", {
        className: "euiContextMenuItem__text"
      }, children), arrow);

      var button; // <a> elements don't respect the `disabled` attribute. So if we're disabled, we'll just pretend
      // this is a button and piggyback off its disabled styles.

      if (href && !disabled) {
        var secureRel = getSecureRelForTarget({
          href: href,
          target: target,
          rel: rel
        });
        button = ___EmotionJSX("a", _extends({
          className: classes,
          href: href,
          target: target,
          rel: secureRel,
          ref: buttonRef
        }, rest), buttonInner);
      } else {
        button = ___EmotionJSX("button", _extends({
          disabled: disabled,
          className: classes,
          type: "button",
          ref: buttonRef
        }, rest), buttonInner);
      }

      if (toolTipContent) {
        return ___EmotionJSX(EuiToolTip, {
          title: toolTipTitle ? toolTipTitle : null,
          content: toolTipContent,
          anchorClassName: "eui-displayBlock",
          position: toolTipPosition
        }, button);
      } else {
        return button;
      }
    }
  }]);

  return EuiContextMenuItem;
}(Component);
EuiContextMenuItem.propTypes = {
  className: PropTypes.string,
  "aria-label": PropTypes.string,
  "data-test-subj": PropTypes.string,
  icon: PropTypes.oneOfType([PropTypes.element.isRequired, PropTypes.string.isRequired, PropTypes.any.isRequired]),
  hasPanel: PropTypes.bool,
  disabled: PropTypes.bool,
  onClick: PropTypes.func,
  buttonRef: PropTypes.any,

  /**
     * Required if using a tooltip. Add an optional tooltip on hover
     */
  toolTipContent: PropTypes.node,

  /**
     * Optional title for the tooltip
     */
  toolTipTitle: PropTypes.node,

  /**
     * Dictates the position of the tooltip.
     */
  toolTipPosition: PropTypes.oneOf(["top", "right", "bottom", "left"]),
  href: PropTypes.string,
  target: PropTypes.string,
  rel: PropTypes.string,

  /**
     * How to align icon with content of button
     */
  layoutAlign: PropTypes.oneOf(["center", "top", "bottom"]),

  /**
     * Reduce the size to `s` when in need of a more compressed menu
     */
  size: PropTypes.oneOf(["s", "m"])
};