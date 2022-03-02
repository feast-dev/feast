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
import { keysOf } from '../common';
import { EuiLoadingSpinner } from '../loading';
import { EuiResizeObserver } from '../observer/resize_observer';
import { EuiI18n } from '../i18n';
import { htmlIdGenerator } from '../../services';
import { EuiButtonIcon } from '../button';
import { jsx as ___EmotionJSX } from "@emotion/react";
var paddingSizeToClassNameMap = {
  none: '',
  xs: 'euiAccordion__padding--xs',
  s: 'euiAccordion__padding--s',
  m: 'euiAccordion__padding--m',
  l: 'euiAccordion__padding--l',
  xl: 'euiAccordion__padding--xl'
};
export var PADDING_SIZES = keysOf(paddingSizeToClassNameMap);
export var EuiAccordion = /*#__PURE__*/function (_Component) {
  _inherits(EuiAccordion, _Component);

  var _super = _createSuper(EuiAccordion);

  function EuiAccordion() {
    var _this;

    _classCallCheck(this, EuiAccordion);

    for (var _len = arguments.length, args = new Array(_len), _key = 0; _key < _len; _key++) {
      args[_key] = arguments[_key];
    }

    _this = _super.call.apply(_super, [this].concat(args));

    _defineProperty(_assertThisInitialized(_this), "childContent", null);

    _defineProperty(_assertThisInitialized(_this), "childWrapper", null);

    _defineProperty(_assertThisInitialized(_this), "state", {
      isOpen: _this.props.forceState ? _this.props.forceState === 'open' : _this.props.initialIsOpen
    });

    _defineProperty(_assertThisInitialized(_this), "setChildContentHeight", function () {
      var forceState = _this.props.forceState;
      requestAnimationFrame(function () {
        var height = _this.childContent && (forceState ? forceState === 'open' : _this.state.isOpen) ? _this.childContent.clientHeight : 0;
        _this.childWrapper && _this.childWrapper.setAttribute('style', "height: ".concat(height, "px"));
      });
    });

    _defineProperty(_assertThisInitialized(_this), "onToggle", function () {
      var forceState = _this.props.forceState;

      if (forceState) {
        _this.props.onToggle && _this.props.onToggle(forceState === 'open' ? false : true);
      } else {
        _this.setState(function (prevState) {
          return {
            isOpen: !prevState.isOpen
          };
        }, function () {
          if (_this.state.isOpen && _this.childWrapper) {
            _this.childWrapper.focus();
          }

          _this.props.onToggle && _this.props.onToggle(_this.state.isOpen);
        });
      }
    });

    _defineProperty(_assertThisInitialized(_this), "setChildContentRef", function (node) {
      _this.childContent = node;
    });

    _defineProperty(_assertThisInitialized(_this), "generatedId", htmlIdGenerator()());

    return _this;
  }

  _createClass(EuiAccordion, [{
    key: "componentDidMount",
    value: function componentDidMount() {
      this.setChildContentHeight();
    }
  }, {
    key: "componentDidUpdate",
    value: function componentDidUpdate() {
      this.setChildContentHeight();
    }
  }, {
    key: "render",
    value: function render() {
      var _buttonProps$id,
          _this2 = this;

      var _this$props = this.props,
          children = _this$props.children,
          buttonContent = _this$props.buttonContent,
          className = _this$props.className,
          id = _this$props.id,
          _this$props$element = _this$props.element,
          Element = _this$props$element === void 0 ? 'div' : _this$props$element,
          buttonClassName = _this$props.buttonClassName,
          buttonContentClassName = _this$props.buttonContentClassName,
          extraAction = _this$props.extraAction,
          paddingSize = _this$props.paddingSize,
          initialIsOpen = _this$props.initialIsOpen,
          arrowDisplay = _this$props.arrowDisplay,
          forceState = _this$props.forceState,
          isLoading = _this$props.isLoading,
          isLoadingMessage = _this$props.isLoadingMessage,
          buttonProps = _this$props.buttonProps,
          _this$props$buttonEle = _this$props.buttonElement,
          _ButtonElement = _this$props$buttonEle === void 0 ? 'button' : _this$props$buttonEle,
          arrowProps = _this$props.arrowProps,
          rest = _objectWithoutProperties(_this$props, ["children", "buttonContent", "className", "id", "element", "buttonClassName", "buttonContentClassName", "extraAction", "paddingSize", "initialIsOpen", "arrowDisplay", "forceState", "isLoading", "isLoadingMessage", "buttonProps", "buttonElement", "arrowProps"]);

      var isOpen = forceState ? forceState === 'open' : this.state.isOpen; // Force button element to be a legend if the element is a fieldset

      var ButtonElement = Element === 'fieldset' ? 'legend' : _ButtonElement;
      var buttonElementIsFocusable = ButtonElement === 'button'; // Force visibility of arrow button if button element is not focusable

      var _arrowDisplay = arrowDisplay === 'none' && !buttonElementIsFocusable ? 'left' : arrowDisplay;

      var classes = classNames('euiAccordion', {
        'euiAccordion-isOpen': isOpen
      }, className);
      var paddingClass = paddingSize ? classNames(paddingSizeToClassNameMap[paddingSize]) : undefined;
      var childrenClasses = classNames(paddingClass, {
        'euiAccordion__children-isLoading': isLoading
      });
      var buttonClasses = classNames('euiAccordion__button', buttonClassName, buttonProps === null || buttonProps === void 0 ? void 0 : buttonProps.className);
      var buttonContentClasses = classNames('euiAccordion__buttonContent', buttonContentClassName);
      var iconButtonClasses = classNames('euiAccordion__iconButton', {
        'euiAccordion__iconButton-isOpen': isOpen,
        'euiAccordion__iconButton--right': _arrowDisplay === 'right'
      }, arrowProps === null || arrowProps === void 0 ? void 0 : arrowProps.className);
      var iconButton;
      var buttonId = (_buttonProps$id = buttonProps === null || buttonProps === void 0 ? void 0 : buttonProps.id) !== null && _buttonProps$id !== void 0 ? _buttonProps$id : this.generatedId;

      if (_arrowDisplay !== 'none') {
        iconButton = ___EmotionJSX(EuiButtonIcon, _extends({
          color: "text"
        }, arrowProps, {
          className: iconButtonClasses,
          iconType: "arrowRight",
          onClick: this.onToggle,
          "aria-controls": id,
          "aria-expanded": isOpen,
          "aria-labelledby": buttonId,
          tabIndex: buttonElementIsFocusable ? -1 : 0
        }));
      }

      var optionalAction = null;

      if (extraAction) {
        optionalAction = ___EmotionJSX("div", {
          className: "euiAccordion__optionalAction"
        }, isLoading ? ___EmotionJSX(EuiLoadingSpinner, null) : extraAction);
      }

      var childrenContent;

      if (isLoading && isLoadingMessage) {
        childrenContent = ___EmotionJSX(React.Fragment, null, ___EmotionJSX(EuiLoadingSpinner, {
          className: "euiAccordion__spinner"
        }), ___EmotionJSX("span", null, isLoadingMessage && isLoadingMessage !== true ? isLoadingMessage : ___EmotionJSX(EuiI18n, {
          token: "euiAccordion.isLoading",
          default: "Loading"
        })));
      } else {
        childrenContent = children;
      }

      var button = ___EmotionJSX(ButtonElement, _extends({}, buttonProps, {
        id: buttonId,
        className: buttonClasses,
        "aria-controls": id,
        "aria-expanded": isOpen,
        onClick: this.onToggle,
        type: ButtonElement === 'button' ? 'button' : undefined
      }), ___EmotionJSX("span", {
        className: buttonContentClasses
      }, buttonContent));

      return ___EmotionJSX(Element, _extends({
        className: classes
      }, rest), ___EmotionJSX("div", {
        className: "euiAccordion__triggerWrapper"
      }, _arrowDisplay === 'left' && iconButton, button, optionalAction, _arrowDisplay === 'right' && iconButton), ___EmotionJSX("div", {
        className: "euiAccordion__childWrapper",
        ref: function ref(node) {
          _this2.childWrapper = node;
        },
        tabIndex: -1,
        role: "region",
        "aria-labelledby": buttonId,
        id: id
      }, ___EmotionJSX(EuiResizeObserver, {
        onResize: this.setChildContentHeight
      }, function (resizeRef) {
        return ___EmotionJSX("div", {
          ref: function ref(_ref) {
            _this2.setChildContentRef(_ref);

            resizeRef(_ref);
          }
        }, ___EmotionJSX("div", {
          className: childrenClasses
        }, childrenContent));
      })));
    }
  }]);

  return EuiAccordion;
}(Component);

_defineProperty(EuiAccordion, "defaultProps", {
  initialIsOpen: false,
  paddingSize: 'none',
  arrowDisplay: 'left',
  isLoading: false,
  isLoadingMessage: false,
  element: 'div',
  buttonElement: 'button'
});