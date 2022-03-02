import _extends from "@babel/runtime/helpers/extends";
import _defineProperty from "@babel/runtime/helpers/defineProperty";
import _slicedToArray from "@babel/runtime/helpers/slicedToArray";
import _objectWithoutProperties from "@babel/runtime/helpers/objectWithoutProperties";

function ownKeys(object, enumerableOnly) { var keys = Object.keys(object); if (Object.getOwnPropertySymbols) { var symbols = Object.getOwnPropertySymbols(object); if (enumerableOnly) symbols = symbols.filter(function (sym) { return Object.getOwnPropertyDescriptor(object, sym).enumerable; }); keys.push.apply(keys, symbols); } return keys; }

function _objectSpread(target) { for (var i = 1; i < arguments.length; i++) { var source = arguments[i] != null ? arguments[i] : {}; if (i % 2) { ownKeys(Object(source), true).forEach(function (key) { _defineProperty(target, key, source[key]); }); } else if (Object.getOwnPropertyDescriptors) { Object.defineProperties(target, Object.getOwnPropertyDescriptors(source)); } else { ownKeys(Object(source)).forEach(function (key) { Object.defineProperty(target, key, Object.getOwnPropertyDescriptor(source, key)); }); } } return target; }

/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */
import React, { useEffect, useRef, useCallback } from 'react';
import classNames from 'classnames';
import { keys } from '../../services';
import { useResizeObserver } from '../observer/resize_observer';
import { EuiResizableContainerContextProvider } from './context';
import { euiResizableButtonWithControls } from './resizable_button';
import { euiResizablePanelWithControls, getModeType } from './resizable_panel';
import { useContainerCallbacks, getPosition } from './helpers';
import { jsx as ___EmotionJSX } from "@emotion/react";
var containerDirections = {
  vertical: 'vertical',
  horizontal: 'horizontal'
};
var initialState = {
  isDragging: false,
  currentResizerPos: -1,
  prevPanelId: null,
  nextPanelId: null,
  containerSize: 1,
  panels: {},
  resizers: {}
};
export var EuiResizableContainer = function EuiResizableContainer(_ref) {
  var _ref$direction = _ref.direction,
      direction = _ref$direction === void 0 ? 'horizontal' : _ref$direction,
      children = _ref.children,
      className = _ref.className,
      onPanelWidthChange = _ref.onPanelWidthChange,
      onToggleCollapsed = _ref.onToggleCollapsed,
      rest = _objectWithoutProperties(_ref, ["direction", "children", "className", "onPanelWidthChange", "onToggleCollapsed"]);

  var containerRef = useRef(null);
  var isHorizontal = direction === containerDirections.horizontal;
  var classes = classNames('euiResizableContainer', {
    'euiResizableContainer--vertical': !isHorizontal,
    'euiResizableContainer--horizontal': isHorizontal
  }, className);

  var _useContainerCallback = useContainerCallbacks({
    initialState: _objectSpread(_objectSpread({}, initialState), {}, {
      isHorizontal: isHorizontal
    }),
    containerRef: containerRef,
    onPanelWidthChange: onPanelWidthChange
  }),
      _useContainerCallback2 = _slicedToArray(_useContainerCallback, 2),
      actions = _useContainerCallback2[0],
      reducerState = _useContainerCallback2[1];

  var containerSize = useResizeObserver(containerRef.current, isHorizontal ? 'width' : 'height');
  var initialize = useCallback(function () {
    actions.initContainer(isHorizontal);
  }, [actions, isHorizontal]);
  useEffect(function () {
    if (containerSize.width > 0 && containerSize.height > 0) {
      initialize();
    }
  }, [initialize, containerSize]);
  var onMouseDown = useCallback(function (event) {
    var currentTarget = event.currentTarget;
    var prevPanel = currentTarget.previousElementSibling;
    var nextPanel = currentTarget.nextElementSibling;
    if (!prevPanel || !nextPanel) return;
    var prevPanelId = prevPanel.id;
    var nextPanelId = nextPanel.id;
    var position = getPosition(event, isHorizontal);
    actions.dragStart({
      position: position,
      prevPanelId: prevPanelId,
      nextPanelId: nextPanelId
    });
  }, [actions, isHorizontal]);
  var onMouseMove = useCallback(function (event) {
    if (!reducerState.prevPanelId || !reducerState.nextPanelId || !reducerState.isDragging) return;
    var position = getPosition(event, isHorizontal);
    actions.dragMove({
      position: position,
      prevPanelId: reducerState.prevPanelId,
      nextPanelId: reducerState.nextPanelId
    });
  }, [actions, isHorizontal, reducerState.prevPanelId, reducerState.nextPanelId, reducerState.isDragging]);
  var onKeyDown = useCallback(function (event) {
    var key = event.key,
        currentTarget = event.currentTarget;
    var shouldResizeHorizontalPanel = isHorizontal && (key === keys.ARROW_LEFT || key === keys.ARROW_RIGHT);
    var shouldResizeVerticalPanel = !isHorizontal && (key === keys.ARROW_UP || key === keys.ARROW_DOWN);
    var prevPanelId = currentTarget.previousElementSibling.id;
    var nextPanelId = currentTarget.nextElementSibling.id;
    var direction;

    if (key === keys.ARROW_DOWN || key === keys.ARROW_RIGHT) {
      direction = 'forward';
    }

    if (key === keys.ARROW_UP || key === keys.ARROW_LEFT) {
      direction = 'backward';
    }

    if (direction === 'forward' || direction === 'backward' && (shouldResizeHorizontalPanel || shouldResizeVerticalPanel) && prevPanelId && nextPanelId) {
      event.preventDefault();
      actions.keyMove({
        direction: direction,
        prevPanelId: prevPanelId,
        nextPanelId: nextPanelId
      });
    }
  }, [actions, isHorizontal]);
  var onMouseUp = useCallback(function () {
    actions.reset();
  }, [actions]); // eslint-disable-next-line react-hooks/exhaustive-deps

  var EuiResizableButton = useCallback(euiResizableButtonWithControls({
    onKeyDown: onKeyDown,
    onMouseDown: onMouseDown,
    onTouchStart: onMouseDown,
    onFocus: actions.resizerFocus,
    onBlur: actions.resizerBlur,
    isHorizontal: isHorizontal,
    registration: {
      register: actions.registerResizer,
      deregister: actions.deregisterResizer
    }
  }), [actions, isHorizontal]); // eslint-disable-next-line react-hooks/exhaustive-deps

  var EuiResizablePanel = useCallback(euiResizablePanelWithControls({
    isHorizontal: isHorizontal,
    registration: {
      register: actions.registerPanel,
      deregister: actions.deregisterPanel
    },
    onToggleCollapsed: onToggleCollapsed,
    onToggleCollapsedInternal: actions.togglePanel
  }), [actions, isHorizontal]);

  var render = function render() {
    var DEFAULT = 'custom';
    var content = children(EuiResizablePanel, EuiResizableButton, {
      togglePanel: actions.togglePanel
    });
    var modes = /*#__PURE__*/React.isValidElement(content) ? content.props.children.map(function (el) {
      return getModeType(el.props.mode) || DEFAULT;
    }) : null;

    if (modes && (['collapsible', 'main'].every(function (i) {
      return modes.includes(i);
    }) || modes.every(function (i) {
      return i === DEFAULT;
    }))) {
      return content;
    } else {
      throw new Error('Both `collapsible` and `main` mode panels are required.');
    }
  };

  return ___EmotionJSX(EuiResizableContainerContextProvider, {
    registry: {
      panels: reducerState.panels,
      resizers: reducerState.resizers
    }
  }, ___EmotionJSX("div", _extends({
    className: classes,
    ref: containerRef,
    onMouseMove: reducerState.isDragging ? onMouseMove : undefined,
    onMouseUp: onMouseUp,
    onMouseLeave: onMouseUp,
    onTouchMove: onMouseMove,
    onTouchEnd: onMouseUp
  }, rest), render()));
};