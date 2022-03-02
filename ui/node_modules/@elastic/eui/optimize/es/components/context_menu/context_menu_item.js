import _extends from "@babel/runtime/helpers/extends";
import _typeof from "@babel/runtime/helpers/typeof";
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
import React, { cloneElement, Component } from 'react';
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