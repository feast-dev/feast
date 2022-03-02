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
import { EuiPopover } from './popover';
import { EuiPortal } from '../portal';
import { jsx as ___EmotionJSX } from "@emotion/react";

/**
 * Injects the EuiPopover next to the button via EuiPortal
 * then the button element is moved into the popover dom.
 * On unmount, the button is moved back to its original location.
 */
export var EuiWrappingPopover = /*#__PURE__*/function (_Component) {
  _inherits(EuiWrappingPopover, _Component);

  var _super = _createSuper(EuiWrappingPopover);

  function EuiWrappingPopover() {
    var _this;

    _classCallCheck(this, EuiWrappingPopover);

    for (var _len = arguments.length, args = new Array(_len), _key = 0; _key < _len; _key++) {
      args[_key] = arguments[_key];
    }

    _this = _super.call.apply(_super, [this].concat(args));

    _defineProperty(_assertThisInitialized(_this), "portal", null);

    _defineProperty(_assertThisInitialized(_this), "anchor", null);

    _defineProperty(_assertThisInitialized(_this), "setPortalRef", function (node) {
      _this.portal = node;
    });

    _defineProperty(_assertThisInitialized(_this), "setAnchorRef", function (node) {
      _this.anchor = node;
    });

    return _this;
  }

  _createClass(EuiWrappingPopover, [{
    key: "componentDidMount",
    value: function componentDidMount() {
      if (this.anchor) {
        this.anchor.insertAdjacentElement('beforebegin', this.props.button);
      }
    }
  }, {
    key: "componentWillUnmount",
    value: function componentWillUnmount() {
      if (this.props.button.parentNode) {
        if (this.portal) {
          this.portal.insertAdjacentElement('beforebegin', this.props.button);
        }
      }
    }
  }, {
    key: "render",
    value: function render() {
      var _this$props = this.props,
          button = _this$props.button,
          rest = _objectWithoutProperties(_this$props, ["button"]);

      return ___EmotionJSX(EuiPortal, {
        portalRef: this.setPortalRef,
        insert: {
          sibling: this.props.button,
          position: 'after'
        }
      }, ___EmotionJSX(EuiPopover, _extends({}, rest, {
        button: ___EmotionJSX("div", {
          ref: this.setAnchorRef,
          className: "euiWrappingPopover__anchor"
        })
      })));
    }
  }]);

  return EuiWrappingPopover;
}(Component);