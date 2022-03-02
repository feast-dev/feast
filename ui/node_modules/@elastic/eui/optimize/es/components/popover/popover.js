import _extends from "@babel/runtime/helpers/extends";
import _objectWithoutProperties from "@babel/runtime/helpers/objectWithoutProperties";
import _toConsumableArray from "@babel/runtime/helpers/toConsumableArray";
import _classCallCheck from "@babel/runtime/helpers/classCallCheck";
import _assertThisInitialized from "@babel/runtime/helpers/assertThisInitialized";
import _createClass from "@babel/runtime/helpers/createClass";
import _inherits from "@babel/runtime/helpers/inherits";
import _possibleConstructorReturn from "@babel/runtime/helpers/possibleConstructorReturn";
import _getPrototypeOf from "@babel/runtime/helpers/getPrototypeOf";
import _defineProperty from "@babel/runtime/helpers/defineProperty";
import _typeof from "@babel/runtime/helpers/typeof";
import _slicedToArray from "@babel/runtime/helpers/slicedToArray";

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
import tabbable from 'tabbable';
import { EuiFocusTrap } from '../focus_trap';
import { cascadingMenuKeys, getTransitionTimings, getWaitDuration, performOnFrame, htmlIdGenerator } from '../../services';
import { EuiScreenReaderOnly } from '../accessibility';
import { EuiPanel } from '../panel';
import { EuiPortal } from '../portal';
import { EuiMutationObserver } from '../observer/mutation_observer';
import { findPopoverPosition, getElementZIndex } from '../../services/popover';
import { EuiI18n } from '../i18n';
import { EuiOutsideClickDetector } from '../outside_click_detector';
import { jsx as ___EmotionJSX } from "@emotion/react";
export var popoverAnchorPosition = ['upCenter', 'upLeft', 'upRight', 'downCenter', 'downLeft', 'downRight', 'leftCenter', 'leftUp', 'leftDown', 'rightCenter', 'rightUp', 'rightDown'];
var anchorPositionToPopoverPositionMap = {
  up: 'top',
  right: 'right',
  down: 'bottom',
  left: 'left'
};
export function getPopoverPositionFromAnchorPosition(anchorPosition) {
  // maps the anchor position to the matching popover position
  // e.g. "upLeft" -> "top", "downRight" -> "bottom"
  // extract the first positional word from anchorPosition:
  // starts at the beginning (" ^ ") of anchorPosition and
  // captures all of the characters (" (.*?) ") until the
  // first capital letter (" [A-Z] ") is encountered
  var _ref = anchorPosition.match(/^(.*?)[A-Z]/),
      _ref2 = _slicedToArray(_ref, 2),
      primaryPosition = _ref2[1];

  return anchorPositionToPopoverPositionMap[primaryPosition];
}
export function getPopoverAlignFromAnchorPosition(anchorPosition) {
  // maps the gravity to the matching popover position
  // e.g. "upLeft" -> "left", "rightDown" -> "bottom"
  // extract the second positional word from anchorPosition:
  // starts a capture group at the first capital letter
  // and includes everything after it
  var _ref3 = anchorPosition.match(/([A-Z].*)/),
      _ref4 = _slicedToArray(_ref3, 2),
      align = _ref4[1]; // this performs two tasks:
  // 1. normalizes the align position by lowercasing it
  // 2. `center` doesn't exist in the lookup map which converts it to `undefined` meaning no align


  return anchorPositionToPopoverPositionMap[align.toLowerCase()];
}
var anchorPositionToClassNameMap = {
  upCenter: 'euiPopover--anchorUpCenter',
  upLeft: 'euiPopover--anchorUpLeft',
  upRight: 'euiPopover--anchorUpRight',
  downCenter: 'euiPopover--anchorDownCenter',
  downLeft: 'euiPopover--anchorDownLeft',
  downRight: 'euiPopover--anchorDownRight',
  leftCenter: 'euiPopover--anchorLeftCenter',
  leftUp: 'euiPopover--anchorLeftUp',
  leftDown: 'euiPopover--anchorLeftDown',
  rightCenter: 'euiPopover--anchorRightCenter',
  rightUp: 'euiPopover--anchorRightUp',
  rightDown: 'euiPopover--anchorRightDown'
};
export var ANCHOR_POSITIONS = Object.keys(anchorPositionToClassNameMap);
var displayToClassNameMap = {
  inlineBlock: undefined,
  block: 'euiPopover--displayBlock'
};
export var DISPLAY = Object.keys(displayToClassNameMap);
var DEFAULT_POPOVER_STYLES = {
  top: 50,
  left: 50
};

function getElementFromInitialFocus(initialFocus) {
  var initialFocusType = _typeof(initialFocus);

  if (initialFocusType === 'string') {
    return document.querySelector(initialFocus);
  }

  if (initialFocusType === 'function') {
    return initialFocus();
  }

  return initialFocus;
}

var returnFocusConfig = {
  preventScroll: true
};
export var EuiPopover = /*#__PURE__*/function (_Component) {
  _inherits(EuiPopover, _Component);

  var _super = _createSuper(EuiPopover);

  _createClass(EuiPopover, null, [{
    key: "getDerivedStateFromProps",
    value: function getDerivedStateFromProps(nextProps, prevState) {
      if (prevState.prevProps.isOpen && !nextProps.isOpen) {
        return {
          prevProps: {
            isOpen: nextProps.isOpen
          },
          isClosing: true,
          isOpening: false
        };
      }

      if (prevState.prevProps.isOpen !== nextProps.isOpen) {
        return {
          prevProps: {
            isOpen: nextProps.isOpen
          }
        };
      }

      return null;
    }
  }]);

  function EuiPopover(props) {
    var _this;

    _classCallCheck(this, EuiPopover);

    _this = _super.call(this, props);

    _defineProperty(_assertThisInitialized(_this), "respositionTimeout", void 0);

    _defineProperty(_assertThisInitialized(_this), "closingTransitionTimeout", void 0);

    _defineProperty(_assertThisInitialized(_this), "closingTransitionAnimationFrame", void 0);

    _defineProperty(_assertThisInitialized(_this), "updateFocusAnimationFrame", void 0);

    _defineProperty(_assertThisInitialized(_this), "button", null);

    _defineProperty(_assertThisInitialized(_this), "panel", null);

    _defineProperty(_assertThisInitialized(_this), "hasSetInitialFocus", false);

    _defineProperty(_assertThisInitialized(_this), "descriptionId", htmlIdGenerator()());

    _defineProperty(_assertThisInitialized(_this), "closePopover", function () {
      if (_this.props.isOpen) {
        _this.props.closePopover();
      }
    });

    _defineProperty(_assertThisInitialized(_this), "onEscapeKey", function (event) {
      if (_this.props.isOpen) {
        event.preventDefault();
        event.stopPropagation();

        _this.closePopover();
      }
    });

    _defineProperty(_assertThisInitialized(_this), "onKeyDown", function (event) {
      if (event.key === cascadingMenuKeys.ESCAPE) {
        _this.onEscapeKey(event);
      }
    });

    _defineProperty(_assertThisInitialized(_this), "onClickOutside", function (event) {
      // only close the popover if the event source isn't the anchor button
      // otherwise, it is up to the anchor to toggle the popover's open status
      if (_this.button && _this.button.contains(event.target) === false) {
        _this.closePopover();
      }
    });

    _defineProperty(_assertThisInitialized(_this), "onOpenPopover", function () {
      clearTimeout(_this.closingTransitionTimeout);

      if (_this.closingTransitionAnimationFrame) {
        cancelAnimationFrame(_this.closingTransitionAnimationFrame);
      } // We need to set this state a beat after the render takes place, so that the CSS
      // transition can take effect.


      _this.closingTransitionAnimationFrame = window.requestAnimationFrame(function () {
        _this.setState({
          isOpening: true
        });
      }); // for each child element of `this.panel`, find any transition duration we should wait for before stabilizing

      var _Array$prototype$slic = Array.prototype.slice.call(_this.panel ? [_this.panel].concat(_toConsumableArray(Array.from(_this.panel.children))) : []).reduce(function (_ref5, element) {
        var durationMatch = _ref5.durationMatch,
            delayMatch = _ref5.delayMatch;
        var transitionTimings = getTransitionTimings(element);
        return {
          durationMatch: Math.max(durationMatch, transitionTimings.durationMatch),
          delayMatch: Math.max(delayMatch, transitionTimings.delayMatch)
        };
      }, {
        durationMatch: 0,
        delayMatch: 0
      }),
          durationMatch = _Array$prototype$slic.durationMatch,
          delayMatch = _Array$prototype$slic.delayMatch;

      clearTimeout(_this.respositionTimeout);
      _this.respositionTimeout = window.setTimeout(function () {
        _this.setState({
          isOpenStable: true
        }, function () {
          _this.positionPopoverFixed();

          _this.updateFocus();
        });
      }, durationMatch + delayMatch);
    });

    _defineProperty(_assertThisInitialized(_this), "onMutation", function (records) {
      var waitDuration = getWaitDuration(records);

      _this.positionPopoverFixed();

      performOnFrame(waitDuration, _this.positionPopoverFixed);
    });

    _defineProperty(_assertThisInitialized(_this), "positionPopover", function (allowEnforcePosition) {
      if (_this.button == null || _this.panel == null) return;
      var _ref6 = _this.props,
          anchorPosition = _ref6.anchorPosition;
      var position = getPopoverPositionFromAnchorPosition(anchorPosition);
      var forcePosition = undefined;

      if (allowEnforcePosition && _this.state.isOpenStable && _this.state.openPosition != null) {
        position = _this.state.openPosition;
        forcePosition = true;
      }

      var _findPopoverPosition = findPopoverPosition({
        container: _this.props.container,
        position: position,
        forcePosition: forcePosition,
        align: getPopoverAlignFromAnchorPosition(anchorPosition),
        anchor: _this.button,
        popover: _this.panel,
        offset: !_this.props.attachToAnchor && _this.props.hasArrow ? 16 + (_this.props.offset || 0) : 8 + (_this.props.offset || 0),
        arrowConfig: {
          arrowWidth: 24,
          arrowBuffer: 10
        },
        returnBoundingBox: _this.props.attachToAnchor,
        buffer: _this.props.buffer
      }),
          top = _findPopoverPosition.top,
          left = _findPopoverPosition.left,
          foundPosition = _findPopoverPosition.position,
          arrow = _findPopoverPosition.arrow,
          anchorBoundingBox = _findPopoverPosition.anchorBoundingBox; // the popover's z-index must inherit from the button
      // this keeps a button's popover under a flyout that would cover the button
      // but a popover triggered inside a flyout will appear over that flyout


      var zIndexProp = _this.props.zIndex;
      var zIndex = zIndexProp == null ? getElementZIndex(_this.button, _this.panel) + 2000 : zIndexProp;

      var popoverStyles = _objectSpread(_objectSpread({}, _this.props.panelStyle), {}, {
        top: top,
        left: _this.props.attachToAnchor && anchorBoundingBox ? anchorBoundingBox.left : left,
        zIndex: zIndex
      });

      var willRenderArrow = !_this.props.attachToAnchor && _this.props.hasArrow;
      var arrowStyles = willRenderArrow ? arrow : undefined;
      var arrowPosition = foundPosition;

      _this.setState({
        popoverStyles: popoverStyles,
        arrowStyles: arrowStyles,
        arrowPosition: arrowPosition,
        openPosition: foundPosition
      });
    });

    _defineProperty(_assertThisInitialized(_this), "positionPopoverFixed", function () {
      _this.positionPopover(true);
    });

    _defineProperty(_assertThisInitialized(_this), "positionPopoverFluid", function () {
      _this.positionPopover(false);
    });

    _defineProperty(_assertThisInitialized(_this), "panelRef", function (node) {
      _this.panel = node;
      _this.props.panelRef && _this.props.panelRef(node);

      if (node == null) {
        // panel has unmounted, restore the state defaults
        _this.setState({
          popoverStyles: DEFAULT_POPOVER_STYLES,
          arrowStyles: {},
          arrowPosition: null,
          openPosition: null,
          isOpenStable: false
        });

        window.removeEventListener('resize', _this.positionPopoverFluid);
      } else {
        // panel is coming into existence
        _this.positionPopoverFluid();

        window.addEventListener('resize', _this.positionPopoverFluid);
      }
    });

    _defineProperty(_assertThisInitialized(_this), "buttonRef", function (node) {
      _this.button = node;
      _this.props.buttonRef && _this.props.buttonRef(node);
    });

    _this.state = {
      prevProps: {
        isOpen: props.isOpen
      },
      suppressingPopover: props.isOpen,
      // only suppress if created with isOpen=true
      isClosing: false,
      isOpening: false,
      popoverStyles: DEFAULT_POPOVER_STYLES,
      arrowStyles: {},
      arrowPosition: null,
      openPosition: null,
      // once a stable position has been found, keep the contents on that side
      isOpenStable: false // wait for any initial opening transitions to finish before marking as stable

    };
    return _this;
  }

  _createClass(EuiPopover, [{
    key: "updateFocus",
    value: function updateFocus() {
      var _this2 = this;

      // Wait for the DOM to update.
      this.updateFocusAnimationFrame = window.requestAnimationFrame(function () {
        if (!_this2.props.ownFocus || !_this2.panel || _this2.props.initialFocus === false) {
          return;
        } // If we've already focused on something inside the panel, everything's fine.


        if (_this2.hasSetInitialFocus && _this2.panel.contains(document.activeElement)) {
          return;
        } // Otherwise let's focus the first tabbable item and expedite input from the user.


        var focusTarget;

        if (_this2.props.initialFocus != null) {
          focusTarget = getElementFromInitialFocus(_this2.props.initialFocus);
        } else {
          var tabbableItems = tabbable(_this2.panel);

          if (tabbableItems.length) {
            focusTarget = tabbableItems[0];
          }
        } // there's a race condition between the popover content becoming visible and this function call
        // if the element isn't visible yet (due to css styling) then it can't accept focus
        // so wait for another render and try again


        if (focusTarget == null) {
          // there isn't a focus target, one of two reasons:
          // #1 is the whole panel hidden? If so, schedule another check
          // #2 panel is visible but no tabbables exist, move focus to the panel
          var panelVisibility = window.getComputedStyle(_this2.panel).opacity;

          if (panelVisibility === '0') {
            // #1
            _this2.updateFocus();
          } else {
            // #2
            focusTarget = _this2.panel;
          }
        } else {
          // found an element to focus, but is it visible?
          var visibility = window.getComputedStyle(focusTarget).visibility;

          if (visibility === 'hidden') {
            // not visible, check again next render frame
            _this2.updateFocus();
          }
        }

        if (focusTarget != null) {
          _this2.hasSetInitialFocus = true;
          focusTarget.focus();
        }
      });
    }
  }, {
    key: "componentDidMount",
    value: function componentDidMount() {
      var _this3 = this;

      if (this.state.suppressingPopover) {
        // component was created with isOpen=true; now that it's mounted
        // stop suppressing and start opening
        // eslint-disable-next-line react/no-did-mount-set-state
        this.setState({
          suppressingPopover: false,
          isOpening: true
        }, function () {
          _this3.onOpenPopover();
        });
      }

      if (this.props.repositionOnScroll) {
        window.addEventListener('scroll', this.positionPopoverFixed, true);
      }
    }
  }, {
    key: "componentDidUpdate",
    value: function componentDidUpdate(prevProps) {
      var _this4 = this;

      // The popover is being opened.
      if (!prevProps.isOpen && this.props.isOpen) {
        this.onOpenPopover();
      } // update scroll listener


      if (prevProps.repositionOnScroll !== this.props.repositionOnScroll) {
        if (this.props.repositionOnScroll) {
          window.addEventListener('scroll', this.positionPopoverFixed, true);
        } else {
          window.removeEventListener('scroll', this.positionPopoverFixed, true);
        }
      } // The popover is being closed.


      if (prevProps.isOpen && !this.props.isOpen) {
        // If the user has just closed the popover, queue up the removal of the content after the
        // transition is complete.
        this.closingTransitionTimeout = window.setTimeout(function () {
          _this4.hasSetInitialFocus = false;

          _this4.setState({
            isClosing: false
          });
        }, 250);
      }
    }
  }, {
    key: "componentWillUnmount",
    value: function componentWillUnmount() {
      window.removeEventListener('scroll', this.positionPopoverFixed, true);
      clearTimeout(this.respositionTimeout);
      clearTimeout(this.closingTransitionTimeout);
      cancelAnimationFrame(this.closingTransitionAnimationFrame);
      cancelAnimationFrame(this.updateFocusAnimationFrame);
    }
  }, {
    key: "render",
    value: function render() {
      var _this5 = this;

      var _this$props = this.props,
          anchorClassName = _this$props.anchorClassName,
          anchorPosition = _this$props.anchorPosition,
          button = _this$props.button,
          buttonRef = _this$props.buttonRef,
          insert = _this$props.insert,
          isOpen = _this$props.isOpen,
          ownFocus = _this$props.ownFocus,
          children = _this$props.children,
          className = _this$props.className,
          closePopover = _this$props.closePopover,
          panelClassName = _this$props.panelClassName,
          panelPaddingSize = _this$props.panelPaddingSize,
          panelProps = _this$props.panelProps,
          panelRef = _this$props.panelRef,
          panelStyle = _this$props.panelStyle,
          popoverRef = _this$props.popoverRef,
          hasArrow = _this$props.hasArrow,
          arrowChildren = _this$props.arrowChildren,
          repositionOnScroll = _this$props.repositionOnScroll,
          zIndex = _this$props.zIndex,
          initialFocus = _this$props.initialFocus,
          attachToAnchor = _this$props.attachToAnchor,
          display = _this$props.display,
          onTrapDeactivation = _this$props.onTrapDeactivation,
          buffer = _this$props.buffer,
          ariaLabel = _this$props['aria-label'],
          ariaLabelledBy = _this$props['aria-labelledby'],
          container = _this$props.container,
          focusTrapProps = _this$props.focusTrapProps,
          tabIndexProp = _this$props.tabIndex,
          rest = _objectWithoutProperties(_this$props, ["anchorClassName", "anchorPosition", "button", "buttonRef", "insert", "isOpen", "ownFocus", "children", "className", "closePopover", "panelClassName", "panelPaddingSize", "panelProps", "panelRef", "panelStyle", "popoverRef", "hasArrow", "arrowChildren", "repositionOnScroll", "zIndex", "initialFocus", "attachToAnchor", "display", "onTrapDeactivation", "buffer", "aria-label", "aria-labelledby", "container", "focusTrapProps", "tabIndex"]);

      var classes = classNames('euiPopover', anchorPosition ? anchorPositionToClassNameMap[anchorPosition] : null, display ? displayToClassNameMap[display] : null, {
        'euiPopover-isOpen': this.state.isOpening
      }, className);
      var anchorClasses = classNames('euiPopover__anchor', anchorClassName);
      var panelClasses = classNames('euiPopover__panel', "euiPopover__panel--".concat(this.state.arrowPosition), {
        'euiPopover__panel-isOpen': this.state.isOpening
      }, {
        'euiPopover__panel-noArrow': !hasArrow || attachToAnchor
      }, {
        'euiPopover__panel-isAttached': attachToAnchor
      }, panelClassName, panelProps === null || panelProps === void 0 ? void 0 : panelProps.className);
      var panel;

      if (!this.state.suppressingPopover && (isOpen || this.state.isClosing)) {
        var tabIndex = tabIndexProp;

        var _initialFocus;

        var ariaDescribedby;
        var ariaLive;

        if (ownFocus) {
          tabIndex = tabIndexProp !== null && tabIndexProp !== void 0 ? tabIndexProp : 0;
          ariaLive = 'off';

          _initialFocus = function _initialFocus() {
            return _this5.panel;
          };
        } else {
          ariaLive = 'assertive';
        }

        var focusTrapScreenReaderText;

        if (ownFocus) {
          ariaDescribedby = this.descriptionId;
          focusTrapScreenReaderText = ___EmotionJSX(EuiScreenReaderOnly, null, ___EmotionJSX("p", {
            id: this.descriptionId
          }, ___EmotionJSX(EuiI18n, {
            token: "euiPopover.screenReaderAnnouncement",
            default: "You are in a dialog. To close this dialog, hit escape."
          })));
        }

        var arrowClassNames = classNames('euiPopover__panelArrow', "euiPopover__panelArrow--".concat(this.state.arrowPosition));
        var returnFocus = this.state.isOpenStable ? returnFocusConfig : false;
        panel = ___EmotionJSX(EuiPortal, {
          insert: insert
        }, ___EmotionJSX(EuiFocusTrap, _extends({
          clickOutsideDisables: true
        }, focusTrapProps, {
          returnFocus: returnFocus // Ignore temporary state of indecisive focus
          ,
          initialFocus: _initialFocus,
          onDeactivation: onTrapDeactivation,
          onClickOutside: this.onClickOutside,
          onEscapeKey: this.onEscapeKey,
          disabled: !ownFocus || !this.state.isOpenStable || this.state.isClosing
        }), ___EmotionJSX(EuiPanel, _extends({}, panelProps, {
          panelRef: this.panelRef,
          className: panelClasses,
          hasShadow: false,
          paddingSize: panelPaddingSize,
          tabIndex: tabIndex,
          "aria-live": ariaLive,
          role: "dialog",
          "aria-label": ariaLabel,
          "aria-labelledby": ariaLabelledBy,
          "aria-modal": "true",
          "aria-describedby": ariaDescribedby,
          style: _objectSpread(_objectSpread({}, this.state.popoverStyles), {}, {
            // Adding `will-change` to reduce risk of a blurry animation in Chrome 86+
            willChange: !this.state.isOpenStable ? 'transform, opacity' : undefined
          })
        }), ___EmotionJSX("div", {
          className: arrowClassNames,
          style: this.state.arrowStyles
        }, arrowChildren), focusTrapScreenReaderText, ___EmotionJSX(EuiMutationObserver, {
          observerOptions: {
            attributes: true,
            // element attribute changes
            childList: true,
            // added/removed elements
            characterData: true,
            // text changes
            subtree: true // watch all child elements

          },
          onMutation: this.onMutation
        }, function (mutationRef) {
          return ___EmotionJSX("div", {
            ref: mutationRef
          }, children);
        }))));
      } // react-focus-on and relataed do not register outside click detection
      // when disabled, so we still need to conditionally check for that ourselves


      if (ownFocus) {
        return ___EmotionJSX("div", _extends({
          className: classes,
          ref: popoverRef
        }, rest), ___EmotionJSX("div", {
          className: anchorClasses,
          ref: this.buttonRef
        }, button instanceof HTMLElement ? null : button), panel);
      } else {
        return ___EmotionJSX(EuiOutsideClickDetector, {
          onOutsideClick: this.closePopover
        }, ___EmotionJSX("div", _extends({
          className: classes,
          ref: popoverRef,
          onKeyDown: this.onKeyDown
        }, rest), ___EmotionJSX("div", {
          className: anchorClasses,
          ref: this.buttonRef
        }, button instanceof HTMLElement ? null : button), panel));
      }
    }
  }]);

  return EuiPopover;
}(Component);

_defineProperty(EuiPopover, "defaultProps", {
  isOpen: false,
  ownFocus: true,
  anchorPosition: 'downCenter',
  panelPaddingSize: 'm',
  hasArrow: true,
  display: 'inlineBlock'
});