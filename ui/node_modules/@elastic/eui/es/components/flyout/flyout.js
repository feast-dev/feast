function _extends() { _extends = Object.assign || function (target) { for (var i = 1; i < arguments.length; i++) { var source = arguments[i]; for (var key in source) { if (Object.prototype.hasOwnProperty.call(source, key)) { target[key] = source[key]; } } } return target; }; return _extends.apply(this, arguments); }

function ownKeys(object, enumerableOnly) { var keys = Object.keys(object); if (Object.getOwnPropertySymbols) { var symbols = Object.getOwnPropertySymbols(object); if (enumerableOnly) symbols = symbols.filter(function (sym) { return Object.getOwnPropertyDescriptor(object, sym).enumerable; }); keys.push.apply(keys, symbols); } return keys; }

function _objectSpread(target) { for (var i = 1; i < arguments.length; i++) { var source = arguments[i] != null ? arguments[i] : {}; if (i % 2) { ownKeys(Object(source), true).forEach(function (key) { _defineProperty(target, key, source[key]); }); } else if (Object.getOwnPropertyDescriptors) { Object.defineProperties(target, Object.getOwnPropertyDescriptors(source)); } else { ownKeys(Object(source)).forEach(function (key) { Object.defineProperty(target, key, Object.getOwnPropertyDescriptor(source, key)); }); } } return target; }

function _defineProperty(obj, key, value) { if (key in obj) { Object.defineProperty(obj, key, { value: value, enumerable: true, configurable: true, writable: true }); } else { obj[key] = value; } return obj; }

function _slicedToArray(arr, i) { return _arrayWithHoles(arr) || _iterableToArrayLimit(arr, i) || _unsupportedIterableToArray(arr, i) || _nonIterableRest(); }

function _nonIterableRest() { throw new TypeError("Invalid attempt to destructure non-iterable instance.\nIn order to be iterable, non-array objects must have a [Symbol.iterator]() method."); }

function _unsupportedIterableToArray(o, minLen) { if (!o) return; if (typeof o === "string") return _arrayLikeToArray(o, minLen); var n = Object.prototype.toString.call(o).slice(8, -1); if (n === "Object" && o.constructor) n = o.constructor.name; if (n === "Map" || n === "Set") return Array.from(o); if (n === "Arguments" || /^(?:Ui|I)nt(?:8|16|32)(?:Clamped)?Array$/.test(n)) return _arrayLikeToArray(o, minLen); }

function _arrayLikeToArray(arr, len) { if (len == null || len > arr.length) len = arr.length; for (var i = 0, arr2 = new Array(len); i < len; i++) { arr2[i] = arr[i]; } return arr2; }

function _iterableToArrayLimit(arr, i) { if (typeof Symbol === "undefined" || !(Symbol.iterator in Object(arr))) return; var _arr = []; var _n = true; var _d = false; var _e = undefined; try { for (var _i = arr[Symbol.iterator](), _s; !(_n = (_s = _i.next()).done); _n = true) { _arr.push(_s.value); if (i && _arr.length === i) break; } } catch (err) { _d = true; _e = err; } finally { try { if (!_n && _i["return"] != null) _i["return"](); } finally { if (_d) throw _e; } } return _arr; }

function _arrayWithHoles(arr) { if (Array.isArray(arr)) return arr; }

function _objectWithoutProperties(source, excluded) { if (source == null) return {}; var target = _objectWithoutPropertiesLoose(source, excluded); var key, i; if (Object.getOwnPropertySymbols) { var sourceSymbolKeys = Object.getOwnPropertySymbols(source); for (i = 0; i < sourceSymbolKeys.length; i++) { key = sourceSymbolKeys[i]; if (excluded.indexOf(key) >= 0) continue; if (!Object.prototype.propertyIsEnumerable.call(source, key)) continue; target[key] = source[key]; } } return target; }

function _objectWithoutPropertiesLoose(source, excluded) { if (source == null) return {}; var target = {}; var sourceKeys = Object.keys(source); var key, i; for (i = 0; i < sourceKeys.length; i++) { key = sourceKeys[i]; if (excluded.indexOf(key) >= 0) continue; target[key] = source[key]; } return target; }

/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */
import React, { useEffect, useState, forwardRef, Fragment } from 'react';
import classnames from 'classnames';
import { keys, EuiWindowEvent, useCombinedRefs, isWithinMinBreakpoint, throttle } from '../../services';
import { keysOf } from '../common';
import { EuiFocusTrap } from '../focus_trap';
import { EuiOverlayMask } from '../overlay_mask';
import { EuiButtonIcon } from '../button';
import { EuiI18n } from '../i18n';
import { useResizeObserver } from '../observer/resize_observer';
import { EuiOutsideClickDetector } from '../outside_click_detector';
import { EuiPortal } from '../portal';
import { jsx as ___EmotionJSX } from "@emotion/react";
var typeToClassNameMap = {
  push: 'euiFlyout--push',
  overlay: null
};
export var TYPES = keysOf(typeToClassNameMap);
var sideToClassNameMap = {
  left: 'euiFlyout--left',
  right: null
};
export var SIDES = keysOf(sideToClassNameMap);
var sizeToClassNameMap = {
  s: 'euiFlyout--small',
  m: 'euiFlyout--medium',
  l: 'euiFlyout--large'
};
export var SIZES = keysOf(sizeToClassNameMap);

/**
 * Custom type checker for named flyout sizes since the prop
 * `size` can also be CSSProperties['width'] (string | number)
 */
function isEuiFlyoutSizeNamed(value) {
  return SIZES.includes(value);
}

var paddingSizeToClassNameMap = {
  none: 'euiFlyout--paddingNone',
  s: 'euiFlyout--paddingSmall',
  m: 'euiFlyout--paddingMedium',
  l: 'euiFlyout--paddingLarge'
};
export var PADDING_SIZES = keysOf(paddingSizeToClassNameMap);
var defaultElement = 'div';
export var EuiFlyout = /*#__PURE__*/forwardRef(function (_ref, ref) {
  var className = _ref.className,
      children = _ref.children,
      as = _ref.as,
      _ref$hideCloseButton = _ref.hideCloseButton,
      hideCloseButton = _ref$hideCloseButton === void 0 ? false : _ref$hideCloseButton,
      closeButtonProps = _ref.closeButtonProps,
      closeButtonAriaLabel = _ref.closeButtonAriaLabel,
      _ref$closeButtonPosit = _ref.closeButtonPosition,
      closeButtonPosition = _ref$closeButtonPosit === void 0 ? 'inside' : _ref$closeButtonPosit,
      onClose = _ref.onClose,
      _ref$ownFocus = _ref.ownFocus,
      ownFocus = _ref$ownFocus === void 0 ? true : _ref$ownFocus,
      _ref$side = _ref.side,
      side = _ref$side === void 0 ? 'right' : _ref$side,
      _ref$size = _ref.size,
      size = _ref$size === void 0 ? 'm' : _ref$size,
      _ref$paddingSize = _ref.paddingSize,
      paddingSize = _ref$paddingSize === void 0 ? 'l' : _ref$paddingSize,
      _ref$maxWidth = _ref.maxWidth,
      maxWidth = _ref$maxWidth === void 0 ? false : _ref$maxWidth,
      style = _ref.style,
      maskProps = _ref.maskProps,
      _ref$type = _ref.type,
      type = _ref$type === void 0 ? 'overlay' : _ref$type,
      outsideClickCloses = _ref.outsideClickCloses,
      _ref$role = _ref.role,
      role = _ref$role === void 0 ? 'dialog' : _ref$role,
      _ref$pushMinBreakpoin = _ref.pushMinBreakpoint,
      pushMinBreakpoint = _ref$pushMinBreakpoin === void 0 ? 'l' : _ref$pushMinBreakpoin,
      rest = _objectWithoutProperties(_ref, ["className", "children", "as", "hideCloseButton", "closeButtonProps", "closeButtonAriaLabel", "closeButtonPosition", "onClose", "ownFocus", "side", "size", "paddingSize", "maxWidth", "style", "maskProps", "type", "outsideClickCloses", "role", "pushMinBreakpoint"]);

  var Element = as || defaultElement;
  /**
   * Setting the initial state of pushed based on the `type` prop
   * and if the current window size is large enough (larger than `pushMinBreakpoint`)
   */

  var _useState = useState(isWithinMinBreakpoint(typeof window === 'undefined' ? 0 : window.innerWidth, pushMinBreakpoint)),
      _useState2 = _slicedToArray(_useState, 2),
      windowIsLargeEnoughToPush = _useState2[0],
      setWindowIsLargeEnoughToPush = _useState2[1];

  var isPushed = type === 'push' && windowIsLargeEnoughToPush;
  /**
   * Watcher added to the window to maintain `isPushed` state depending on
   * the window size compared to the `pushBreakpoint`
   */

  var functionToCallOnWindowResize = throttle(function () {
    if (isWithinMinBreakpoint(window.innerWidth, pushMinBreakpoint)) {
      setWindowIsLargeEnoughToPush(true);
    } else {
      setWindowIsLargeEnoughToPush(false);
    } // reacts every 50ms to resize changes and always gets the final update

  }, 50);
  /**
   * Setting up the refs on the actual flyout element in order to
   * accommodate for the `isPushed` state by adding padding to the body equal to the width of the element
   */

  var _useState3 = useState(null),
      _useState4 = _slicedToArray(_useState3, 2),
      resizeRef = _useState4[0],
      setResizeRef = _useState4[1];

  var setRef = useCombinedRefs([setResizeRef, ref]); // TODO: Allow this hook to be conditional

  var dimensions = useResizeObserver(resizeRef);
  useEffect(function () {
    // This class doesn't actually do anything by EUI, but is nice to add for consumers (JIC)
    document.body.classList.add('euiBody--hasFlyout');
    /**
     * Accomodate for the `isPushed` state by adding padding to the body equal to the width of the element
     */

    if (type === 'push') {
      // Only add the event listener if we'll need to accommodate with padding
      window.addEventListener('resize', functionToCallOnWindowResize);

      if (isPushed) {
        if (side === 'right') {
          document.body.style.paddingRight = "".concat(dimensions.width, "px");
        } else if (side === 'left') {
          document.body.style.paddingLeft = "".concat(dimensions.width, "px");
        }
      }
    }

    return function () {
      document.body.classList.remove('euiBody--hasFlyout');

      if (type === 'push') {
        window.removeEventListener('resize', functionToCallOnWindowResize);

        if (side === 'right') {
          document.body.style.paddingRight = '';
        } else if (side === 'left') {
          document.body.style.paddingLeft = '';
        }
      }
    };
  }, [type, side, dimensions, isPushed, functionToCallOnWindowResize]);
  /**
   * ESC key closes flyout (always?)
   */

  var onKeyDown = function onKeyDown(event) {
    if (!isPushed && event.key === keys.ESCAPE) {
      event.preventDefault();
      onClose();
    }
  };

  var newStyle;
  var widthClassName;
  var sizeClassName; // Setting max-width

  if (maxWidth === true) {
    widthClassName = 'euiFlyout--maxWidth-default';
  } else if (maxWidth !== false) {
    var value = typeof maxWidth === 'number' ? "".concat(maxWidth, "px") : maxWidth;
    newStyle = _objectSpread(_objectSpread({}, style), {}, {
      maxWidth: value
    });
  } // Setting size


  if (isEuiFlyoutSizeNamed(size)) {
    sizeClassName = sizeToClassNameMap[size];
  } else if (newStyle) {
    newStyle.width = size;
  } else {
    newStyle = _objectSpread(_objectSpread({}, style), {}, {
      width: size
    });
  }

  var classes = classnames('euiFlyout', typeToClassNameMap[type], sideToClassNameMap[side], sizeClassName, paddingSizeToClassNameMap[paddingSize], widthClassName, className);
  var closeButton;

  if (onClose && !hideCloseButton) {
    var closeButtonClasses = classnames('euiFlyout__closeButton', "euiFlyout__closeButton--".concat(closeButtonPosition), closeButtonProps === null || closeButtonProps === void 0 ? void 0 : closeButtonProps.className);
    closeButton = ___EmotionJSX(EuiI18n, {
      token: "euiFlyout.closeAriaLabel",
      default: "Close this dialog"
    }, function (closeAriaLabel) {
      return ___EmotionJSX(EuiButtonIcon, _extends({
        display: closeButtonPosition === 'outside' ? 'fill' : 'empty',
        iconType: "cross",
        color: "text",
        "aria-label": closeButtonAriaLabel || closeAriaLabel,
        "data-test-subj": "euiFlyoutCloseButton"
      }, closeButtonProps, {
        className: closeButtonClasses,
        onClick: function onClick(e) {
          onClose();
          (closeButtonProps === null || closeButtonProps === void 0 ? void 0 : closeButtonProps.onClick) && closeButtonProps.onClick(e);
        }
      }));
    });
  }

  var flyoutContent = ___EmotionJSX(Element, _extends({}, rest, {
    role: role,
    className: classes,
    tabIndex: -1,
    style: newStyle || style,
    ref: setRef
  }), closeButton, children);
  /*
   * Trap focus even when `ownFocus={false}`, otherwise closing
   * the flyout won't return focus to the originating button.
   *
   * Set `clickOutsideDisables={true}` when `ownFocus={false}`
   * to allow non-keyboard users the ability to interact with
   * elements outside the flyout.
   */


  var flyout = ___EmotionJSX(EuiFocusTrap, {
    disabled: isPushed,
    clickOutsideDisables: !ownFocus
  }, flyoutContent);
  /**
   * Unless outsideClickCloses = true, then add the outside click detector
   */


  if (ownFocus === false && outsideClickCloses === true) {
    flyout = ___EmotionJSX(EuiFocusTrap, {
      disabled: isPushed,
      clickOutsideDisables: !ownFocus
    }, ___EmotionJSX(EuiOutsideClickDetector, {
      isDisabled: isPushed,
      onOutsideClick: function onOutsideClick() {
        return onClose();
      }
    }, flyoutContent));
  } // If ownFocus is set, wrap with an overlay and allow the user to click it to close it.


  if (ownFocus && !isPushed) {
    flyout = ___EmotionJSX(EuiOverlayMask, _extends({
      onClick: outsideClickCloses === false ? undefined : onClose,
      headerZindexLocation: "below"
    }, maskProps), flyout);
  } else if (!isPushed) {
    // Otherwise still wrap within an EuiPortal so it appends (unless it is the push style)
    flyout = ___EmotionJSX(EuiPortal, null, flyout);
  }

  return ___EmotionJSX(Fragment, null, ___EmotionJSX(EuiWindowEvent, {
    event: "keydown",
    handler: onKeyDown
  }), flyout);
} // React.forwardRef interferes with the inferred element type
// Casting to ensure correct element prop type checking for `as`
// e.g., `href` is not on a `div`
); // Recast to allow `displayName`

EuiFlyout.displayName = 'EuiFlyout';