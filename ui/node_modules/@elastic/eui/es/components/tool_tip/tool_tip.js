function _typeof(obj) { "@babel/helpers - typeof"; if (typeof Symbol === "function" && typeof Symbol.iterator === "symbol") { _typeof = function _typeof(obj) { return typeof obj; }; } else { _typeof = function _typeof(obj) { return obj && typeof Symbol === "function" && obj.constructor === Symbol && obj !== Symbol.prototype ? "symbol" : typeof obj; }; } return _typeof(obj); }

function ownKeys(object, enumerableOnly) { var keys = Object.keys(object); if (Object.getOwnPropertySymbols) { var symbols = Object.getOwnPropertySymbols(object); if (enumerableOnly) symbols = symbols.filter(function (sym) { return Object.getOwnPropertyDescriptor(object, sym).enumerable; }); keys.push.apply(keys, symbols); } return keys; }

function _objectSpread(target) { for (var i = 1; i < arguments.length; i++) { var source = arguments[i] != null ? arguments[i] : {}; if (i % 2) { ownKeys(Object(source), true).forEach(function (key) { _defineProperty(target, key, source[key]); }); } else if (Object.getOwnPropertyDescriptors) { Object.defineProperties(target, Object.getOwnPropertyDescriptors(source)); } else { ownKeys(Object(source)).forEach(function (key) { Object.defineProperty(target, key, Object.getOwnPropertyDescriptor(source, key)); }); } } return target; }

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
import React, { Component, cloneElement, Fragment } from 'react';
import PropTypes from "prop-types";
import classNames from 'classnames';
import { keysOf } from '../common';
import { EuiPortal } from '../portal';
import { EuiToolTipPopover } from './tool_tip_popover';
import { enqueueStateChange } from '../../services/react';
import { findPopoverPosition, htmlIdGenerator } from '../../services';
import { EuiResizeObserver } from '../observer/resize_observer';
import { jsx as ___EmotionJSX } from "@emotion/react";
var positionsToClassNameMap = {
  top: 'euiToolTip--top',
  right: 'euiToolTip--right',
  bottom: 'euiToolTip--bottom',
  left: 'euiToolTip--left'
};
export var POSITIONS = keysOf(positionsToClassNameMap);
var delayToMsMap = {
  regular: 250,
  long: 250 * 5
};
var displayToClassNameMap = {
  inlineBlock: undefined,
  block: 'euiToolTipAnchor--displayBlock'
};
var DEFAULT_TOOLTIP_STYLES = {
  // position the tooltip content near the top-left
  // corner of the window so it can't create scrollbars
  // 50,50 because who knows what negative margins, padding, etc
  top: 50,
  left: 50,
  // just in case, avoid any potential flicker by hiding
  // the tooltip before it is positioned
  opacity: 0,
  // prevent accidental mouse interaction while positioning
  visibility: 'hidden'
};
export var EuiToolTip = /*#__PURE__*/function (_Component) {
  _inherits(EuiToolTip, _Component);

  var _super = _createSuper(EuiToolTip);

  function EuiToolTip() {
    var _this;

    _classCallCheck(this, EuiToolTip);

    for (var _len = arguments.length, args = new Array(_len), _key = 0; _key < _len; _key++) {
      args[_key] = arguments[_key];
    }

    _this = _super.call.apply(_super, [this].concat(args));

    _defineProperty(_assertThisInitialized(_this), "_isMounted", false);

    _defineProperty(_assertThisInitialized(_this), "anchor", null);

    _defineProperty(_assertThisInitialized(_this), "popover", null);

    _defineProperty(_assertThisInitialized(_this), "timeoutId", void 0);

    _defineProperty(_assertThisInitialized(_this), "state", {
      visible: false,
      hasFocus: false,
      calculatedPosition: _this.props.position,
      toolTipStyles: DEFAULT_TOOLTIP_STYLES,
      arrowStyles: undefined,
      id: _this.props.id || htmlIdGenerator()()
    });

    _defineProperty(_assertThisInitialized(_this), "clearAnimationTimeout", function () {
      if (_this.timeoutId) {
        _this.timeoutId = clearTimeout(_this.timeoutId);
      }
    });

    _defineProperty(_assertThisInitialized(_this), "testAnchor", function () {
      // when the tooltip is visible, this checks if the anchor is still part of document
      // this fixes when the react root is removed from the dom without unmounting
      // https://github.com/elastic/eui/issues/1105
      if (document.body.contains(_this.anchor) === false) {
        // the anchor is no longer part of `document`
        _this.hideToolTip();
      } else {
        if (_this.state.visible) {
          // if still visible, keep checking
          requestAnimationFrame(_this.testAnchor);
        }
      }
    });

    _defineProperty(_assertThisInitialized(_this), "setPopoverRef", function (ref) {
      _this.popover = ref; // if the popover has been unmounted, clear
      // any previous knowledge about its size

      if (ref == null) {
        _this.setState({
          toolTipStyles: DEFAULT_TOOLTIP_STYLES,
          arrowStyles: undefined
        });
      }
    });

    _defineProperty(_assertThisInitialized(_this), "showToolTip", function () {
      if (!_this.timeoutId) {
        _this.timeoutId = setTimeout(function () {
          enqueueStateChange(function () {
            return _this.setState({
              visible: true
            });
          });
        }, delayToMsMap[_this.props.delay]);
      }
    });

    _defineProperty(_assertThisInitialized(_this), "positionToolTip", function () {
      var requestedPosition = _this.props.position;

      if (!_this.anchor || !_this.popover) {
        return;
      }

      var _findPopoverPosition = findPopoverPosition({
        anchor: _this.anchor,
        popover: _this.popover,
        position: requestedPosition,
        offset: 16,
        // offset popover 16px from the anchor
        arrowConfig: {
          arrowWidth: 12,
          arrowBuffer: 4
        }
      }),
          position = _findPopoverPosition.position,
          left = _findPopoverPosition.left,
          top = _findPopoverPosition.top,
          arrow = _findPopoverPosition.arrow; // If encroaching the right edge of the window:
      // When `props.content` changes and is longer than `prevProps.content`, the tooltip width remains and
      // the resizeObserver callback will fire twice (once for vertical resize caused by text line wrapping,
      // once for a subsequent position correction) and cause a flash rerender and reposition.
      // To prevent this, we can orient from the right so that text line wrapping does not occur, negating
      // the second resizeObserver callback call.


      var windowWidth = document.documentElement.clientWidth || window.innerWidth;
      var useRightValue = windowWidth / 2 < left;
      var toolTipStyles = {
        top: top,
        left: useRightValue ? 'auto' : left,
        right: useRightValue ? windowWidth - left - _this.popover.offsetWidth : 'auto'
      };

      _this.setState({
        visible: true,
        calculatedPosition: position,
        toolTipStyles: toolTipStyles,
        arrowStyles: arrow
      });
    });

    _defineProperty(_assertThisInitialized(_this), "hideToolTip", function () {
      _this.clearAnimationTimeout();

      enqueueStateChange(function () {
        if (_this._isMounted) {
          _this.setState({
            visible: false
          });
        }
      });
    });

    _defineProperty(_assertThisInitialized(_this), "onFocus", function () {
      _this.setState({
        hasFocus: true
      });

      _this.showToolTip();
    });

    _defineProperty(_assertThisInitialized(_this), "onBlur", function () {
      _this.setState({
        hasFocus: false
      });

      _this.hideToolTip();
    });

    _defineProperty(_assertThisInitialized(_this), "onMouseOut", function (event) {
      // Prevent mousing over children from hiding the tooltip by testing for whether the mouse has
      // left the anchor for a non-child.
      if (_this.anchor === event.relatedTarget || _this.anchor != null && !_this.anchor.contains(event.relatedTarget)) {
        if (!_this.state.hasFocus) {
          _this.hideToolTip();
        }
      }

      if (_this.props.onMouseOut) {
        _this.props.onMouseOut(event);
      }
    });

    return _this;
  }

  _createClass(EuiToolTip, [{
    key: "componentDidMount",
    value: function componentDidMount() {
      this._isMounted = true;
    }
  }, {
    key: "componentWillUnmount",
    value: function componentWillUnmount() {
      this.clearAnimationTimeout();
      this._isMounted = false;
    }
  }, {
    key: "componentDidUpdate",
    value: function componentDidUpdate(prevProps, prevState) {
      if (prevState.visible === false && this.state.visible === true) {
        requestAnimationFrame(this.testAnchor);
      }
    }
  }, {
    key: "render",
    value: function render() {
      var _this2 = this;

      var _this$props = this.props,
          children = _this$props.children,
          className = _this$props.className,
          anchorClassName = _this$props.anchorClassName,
          content = _this$props.content,
          title = _this$props.title,
          delay = _this$props.delay,
          display = _this$props.display,
          rest = _objectWithoutProperties(_this$props, ["children", "className", "anchorClassName", "content", "title", "delay", "display"]);

      var _this$state = this.state,
          arrowStyles = _this$state.arrowStyles,
          id = _this$state.id,
          toolTipStyles = _this$state.toolTipStyles,
          visible = _this$state.visible;
      var classes = classNames('euiToolTip', positionsToClassNameMap[this.state.calculatedPosition], className);
      var anchorClasses = classNames('euiToolTipAnchor', display ? displayToClassNameMap[display] : null, anchorClassName);
      var tooltip;

      if (visible && (content || title)) {
        tooltip = ___EmotionJSX(EuiPortal, null, ___EmotionJSX(EuiToolTipPopover, _extends({
          className: classes,
          style: toolTipStyles,
          positionToolTip: this.positionToolTip,
          popoverRef: this.setPopoverRef,
          title: title,
          id: id,
          role: "tooltip"
        }, rest), ___EmotionJSX("div", {
          style: arrowStyles,
          className: "euiToolTip__arrow"
        }), ___EmotionJSX(EuiResizeObserver, {
          onResize: this.positionToolTip
        }, function (resizeRef) {
          return ___EmotionJSX("div", {
            ref: resizeRef
          }, content);
        })));
      }

      var anchor = // eslint-disable-next-line jsx-a11y/mouse-events-have-key-events
      ___EmotionJSX("span", {
        ref: function ref(anchor) {
          return _this2.anchor = anchor;
        },
        className: anchorClasses,
        onMouseOver: this.showToolTip,
        onMouseOut: this.onMouseOut
      }, /*#__PURE__*/cloneElement(children, _objectSpread({
        onFocus: function onFocus(e) {
          _this2.onFocus();

          children.props.onFocus && children.props.onFocus(e);
        },
        onBlur: function onBlur(e) {
          _this2.onBlur();

          children.props.onBlur && children.props.onBlur(e);
        }
      }, visible && {
        'aria-describedby': this.state.id
      })));

      return ___EmotionJSX(Fragment, null, anchor, tooltip);
    }
  }]);

  return EuiToolTip;
}(Component);

_defineProperty(EuiToolTip, "defaultProps", {
  position: 'top',
  delay: 'regular',
  display: 'inlineBlock'
});

EuiToolTip.propTypes = {
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
};