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
import React, { useEffect, useRef, useCallback } from 'react';
import PropTypes from "prop-types";
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
EuiResizableContainer.propTypes = {
  /**
     * Specify the container direction
     */
  direction: PropTypes.oneOf(["vertical", "horizontal"]),

  /**
     * Pure function which accepts Panel and Resizer components in arguments
     * and returns a component tree
     */
  children: PropTypes.func.isRequired,

  /**
     * Pure function which accepts an object where keys are IDs of panels, which sizes were changed,
     * and values are actual sizes in percents
     */
  onPanelWidthChange: PropTypes.func,
  onToggleCollapsed: PropTypes.func,
  style: PropTypes.any,
  className: PropTypes.string,
  "aria-label": PropTypes.string,
  "data-test-subj": PropTypes.string
};