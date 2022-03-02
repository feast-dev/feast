import _extends from "@babel/runtime/helpers/extends";
import _slicedToArray from "@babel/runtime/helpers/slicedToArray";
import _objectWithoutProperties from "@babel/runtime/helpers/objectWithoutProperties";

/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */
import React, { useState, useEffect, useCallback } from 'react';
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
EuiInputPopover.defaultProps = {
  anchorPosition: 'downLeft',
  attachToAnchor: true,
  display: 'block',
  panelPaddingSize: 's'
};