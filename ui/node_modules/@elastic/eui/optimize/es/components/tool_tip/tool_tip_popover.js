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
import { jsx as ___EmotionJSX } from "@emotion/react";
export var EuiToolTipPopover = /*#__PURE__*/function (_Component) {
  _inherits(EuiToolTipPopover, _Component);

  var _super = _createSuper(EuiToolTipPopover);

  function EuiToolTipPopover() {
    var _this;

    _classCallCheck(this, EuiToolTipPopover);

    for (var _len = arguments.length, args = new Array(_len), _key = 0; _key < _len; _key++) {
      args[_key] = arguments[_key];
    }

    _this = _super.call.apply(_super, [this].concat(args));

    _defineProperty(_assertThisInitialized(_this), "popover", void 0);

    _defineProperty(_assertThisInitialized(_this), "updateDimensions", function () {
      requestAnimationFrame(function () {
        // Because of this delay, sometimes `positionToolTip` becomes unavailable.
        if (_this.popover) {
          _this.props.positionToolTip();
        }
      });
    });

    _defineProperty(_assertThisInitialized(_this), "setPopoverRef", function (ref) {
      _this.popover = ref;

      if (_this.props.popoverRef) {
        _this.props.popoverRef(ref);
      }
    });

    return _this;
  }

  _createClass(EuiToolTipPopover, [{
    key: "componentDidMount",
    value: function componentDidMount() {
      document.body.classList.add('euiBody-hasPortalContent');
      window.addEventListener('resize', this.updateDimensions);
    }
  }, {
    key: "componentWillUnmount",
    value: function componentWillUnmount() {
      document.body.classList.remove('euiBody-hasPortalContent');
      window.removeEventListener('resize', this.updateDimensions);
    }
  }, {
    key: "render",
    value: function render() {
      var _this$props = this.props,
          children = _this$props.children,
          title = _this$props.title,
          className = _this$props.className,
          positionToolTip = _this$props.positionToolTip,
          popoverRef = _this$props.popoverRef,
          rest = _objectWithoutProperties(_this$props, ["children", "title", "className", "positionToolTip", "popoverRef"]);

      var classes = classNames('euiToolTipPopover', className);
      var optionalTitle;

      if (title) {
        optionalTitle = ___EmotionJSX("div", {
          className: "euiToolTip__title"
        }, title);
      }

      return ___EmotionJSX("div", _extends({
        className: classes,
        ref: this.setPopoverRef
      }, rest), optionalTitle, children);
    }
  }]);

  return EuiToolTipPopover;
}(Component);