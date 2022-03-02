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
import React, { useState, createContext } from 'react';
import { DragDropContext } from 'react-beautiful-dnd'; // export interface EuiDragDropContextProps extends DragDropContextProps {}

import { jsx as ___EmotionJSX } from "@emotion/react";
export var EuiDragDropContextContext = /*#__PURE__*/createContext({
  isDraggingType: null
});
export var EuiDragDropContext = function EuiDragDropContext(_ref) {
  var onBeforeDragStart = _ref.onBeforeDragStart,
      onDragStart = _ref.onDragStart,
      onDragUpdate = _ref.onDragUpdate,
      onDragEnd = _ref.onDragEnd,
      children = _ref.children,
      rest = _objectWithoutProperties(_ref, ["onBeforeDragStart", "onDragStart", "onDragUpdate", "onDragEnd", "children"]);

  var _useState = useState(null),
      _useState2 = _slicedToArray(_useState, 2),
      isDraggingType = _useState2[0],
      setIsDraggingType = _useState2[1];

  var euiOnDragStart = function euiOnDragStart(start, provided) {
    setIsDraggingType(start.type);

    if (onDragStart) {
      onDragStart(start, provided);
    }
  };

  var euiOnDragEnd = function euiOnDragEnd(result, provided) {
    setIsDraggingType(null);

    if (onDragEnd) {
      onDragEnd(result, provided);
    }
  };

  return ___EmotionJSX(DragDropContext, _extends({
    onBeforeDragStart: onBeforeDragStart,
    onDragStart: euiOnDragStart,
    onDragUpdate: onDragUpdate,
    onDragEnd: euiOnDragEnd
  }, rest), ___EmotionJSX(EuiDragDropContextContext.Provider, {
    value: {
      isDraggingType: isDraggingType
    }
  }, children));
};