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
import React, { cloneElement, Component } from 'react';
import classNames from 'classnames';
import { EuiFormControlLayoutIcons } from './form_control_layout_icons';
import { EuiFormLabel } from '../form_label';
import { jsx as ___EmotionJSX } from "@emotion/react";
export { ICON_SIDES } from './form_control_layout_icons';
export var EuiFormControlLayout = /*#__PURE__*/function (_Component) {
  _inherits(EuiFormControlLayout, _Component);

  var _super = _createSuper(EuiFormControlLayout);

  function EuiFormControlLayout() {
    _classCallCheck(this, EuiFormControlLayout);

    return _super.apply(this, arguments);
  }

  _createClass(EuiFormControlLayout, [{
    key: "render",
    value: function render() {
      var _this$props = this.props,
          children = _this$props.children,
          icon = _this$props.icon,
          clear = _this$props.clear,
          fullWidth = _this$props.fullWidth,
          isLoading = _this$props.isLoading,
          isDisabled = _this$props.isDisabled,
          compressed = _this$props.compressed,
          className = _this$props.className,
          prepend = _this$props.prepend,
          append = _this$props.append,
          readOnly = _this$props.readOnly,
          inputId = _this$props.inputId,
          rest = _objectWithoutProperties(_this$props, ["children", "icon", "clear", "fullWidth", "isLoading", "isDisabled", "compressed", "className", "prepend", "append", "readOnly", "inputId"]);

      var classes = classNames('euiFormControlLayout', {
        'euiFormControlLayout--fullWidth': fullWidth,
        'euiFormControlLayout--compressed': compressed,
        'euiFormControlLayout--readOnly': readOnly,
        'euiFormControlLayout--group': prepend || append,
        'euiFormControlLayout-isDisabled': isDisabled
      }, className);
      var prependNodes = this.renderSideNode('prepend', prepend, inputId);
      var appendNodes = this.renderSideNode('append', append, inputId);
      return ___EmotionJSX("div", _extends({
        className: classes
      }, rest), prependNodes, ___EmotionJSX("div", {
        className: "euiFormControlLayout__childrenWrapper"
      }, children, ___EmotionJSX(EuiFormControlLayoutIcons, {
        icon: icon,
        clear: clear,
        compressed: compressed,
        isLoading: isLoading
      })), appendNodes);
    }
  }, {
    key: "renderSideNode",
    value: function renderSideNode(side, nodes, inputId) {
      var _this = this;

      if (!nodes) {
        return;
      }

      if (typeof nodes === 'string') {
        return this.createFormLabel(side, nodes, inputId);
      }

      var appendNodes = React.Children.map(nodes, function (item, index) {
        return typeof item === 'string' ? _this.createFormLabel(side, item, inputId) : _this.createSideNode(side, item, index);
      });
      return appendNodes;
    }
  }, {
    key: "createFormLabel",
    value: function createFormLabel(side, string, inputId) {
      return ___EmotionJSX(EuiFormLabel, {
        htmlFor: inputId,
        className: "euiFormControlLayout__".concat(side)
      }, string);
    }
  }, {
    key: "createSideNode",
    value: function createSideNode(side, node, key) {
      return /*#__PURE__*/cloneElement(node, {
        className: classNames("euiFormControlLayout__".concat(side), node.props.className),
        key: key
      });
    }
  }]);

  return EuiFormControlLayout;
}(Component);