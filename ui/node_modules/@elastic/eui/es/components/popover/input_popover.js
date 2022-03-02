function _extends() { _extends = Object.assign || function (target) { for (var i = 1; i < arguments.length; i++) { var source = arguments[i]; for (var key in source) { if (Object.prototype.hasOwnProperty.call(source, key)) { target[key] = source[key]; } } } return target; }; return _extends.apply(this, arguments); }

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
import React, { useState, useEffect, useCallback } from 'react';
import PropTypes from "prop-types";
import classnames from 'classnames';
import tabbable from 'tabbable';
import { EuiFocusTrap } from '../focus_trap';
import { EuiPopover } from './popover';
import { EuiResizeObserver } from '../observer/resize_observer';
import { cascadingMenuKeys } from '../../services';
import { jsx as ___EmotionJSX } from "@emotion/react";
export var EuiInputPopover = function EuiInputPopover(_ref) {
  var children = _ref.children,
      className = _ref.className,
      _ref$disableFocusTrap = _ref.disableFocusTrap,
      disableFocusTrap = _ref$disableFocusTrap === void 0 ? false : _ref$disableFocusTrap,
      input = _ref.input,
      _ref$fullWidth = _ref.fullWidth,
      fullWidth = _ref$fullWidth === void 0 ? false : _ref$fullWidth,
      onPanelResize = _ref.onPanelResize,
      props = _objectWithoutProperties(_ref, ["children", "className", "disableFocusTrap", "input", "fullWidth", "onPanelResize"]);

  var _useState = useState(null),
      _useState2 = _slicedToArray(_useState, 2),
      inputEl = _useState2[0],
      setInputEl = _useState2[1];

  var _useState3 = useState(),
      _useState4 = _slicedToArray(_useState3, 2),
      inputElWidth = _useState4[0],
      setInputElWidth = _useState4[1];

  var _useState5 = useState(null),
      _useState6 = _slicedToArray(_useState5, 2),
      panelEl = _useState6[0],
      setPanelEl = _useState6[1];

  var inputRef = function inputRef(node) {
    return setInputEl(node);
  };

  var panelRef = function panelRef(node) {
    return setPanelEl(node);
  };

  var setPanelWidth = useCallback(function (width) {
    if (panelEl && (!!inputElWidth || !!width)) {
      var newWidth = !!width ? width : inputElWidth;
      panelEl.style.width = "".concat(newWidth, "px");

      if (onPanelResize) {
        onPanelResize(newWidth);
      }
    }
  }, [panelEl, inputElWidth, onPanelResize]);
  var onResize = useCallback(function () {
    if (inputEl) {
      var _width = inputEl.getBoundingClientRect().width;
      setInputElWidth(_width);
      setPanelWidth(_width);
    }
  }, [inputEl, setPanelWidth]);
  useEffect(function () {
    onResize();
  }, [onResize]);
  useEffect(function () {
    setPanelWidth();
  }, [setPanelWidth]);

  var onKeyDown = function onKeyDown(event) {
    if (panelEl && event.key === cascadingMenuKeys.TAB) {
      var tabbableItems = tabbable(panelEl).filter(function (el) {
        return Array.from(el.attributes).map(function (el) {
          return el.name;
        }).indexOf('data-focus-guard') < 0;
      });

      if (disableFocusTrap || tabbableItems.length && tabbableItems[tabbableItems.length - 1] === document.activeElement) {
        props.closePopover();
      }
    }
  };

  var classes = classnames('euiInputPopover', {
    'euiInputPopover--fullWidth': fullWidth
  }, className);
  return ___EmotionJSX(EuiPopover, _extends({
    ownFocus: false,
    button: ___EmotionJSX(EuiResizeObserver, {
      onResize: onResize
    }, function (resizeRef) {
      return ___EmotionJSX("div", {
        ref: resizeRef
      }, input);
    }),
    buttonRef: inputRef,
    panelRef: panelRef,
    className: classes
  }, props), ___EmotionJSX(EuiFocusTrap, {
    clickOutsideDisables: true,
    disabled: disableFocusTrap
  }, ___EmotionJSX("div", {
    onKeyDown: onKeyDown
  }, children)));
};
EuiInputPopover.propTypes = {
  className: PropTypes.string,
  "aria-label": PropTypes.string,
  "data-test-subj": PropTypes.string,
  disableFocusTrap: PropTypes.bool,
  fullWidth: PropTypes.bool,
  input: PropTypes.any.isRequired,
  inputRef: PropTypes.any,
  onPanelResize: PropTypes.func
};
EuiInputPopover.defaultProps = {
  anchorPosition: 'downLeft',
  attachToAnchor: true,
  display: 'block',
  panelPaddingSize: 's'
};