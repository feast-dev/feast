"use strict";

var _interopRequireWildcard = require("@babel/runtime/helpers/interopRequireWildcard");

var _interopRequireDefault = require("@babel/runtime/helpers/interopRequireDefault");

Object.defineProperty(exports, "__esModule", {
  value: true
});
exports.EuiDragDropContext = exports.EuiDragDropContextContext = void 0;

var _extends2 = _interopRequireDefault(require("@babel/runtime/helpers/extends"));

var _slicedToArray2 = _interopRequireDefault(require("@babel/runtime/helpers/slicedToArray"));

var _objectWithoutProperties2 = _interopRequireDefault(require("@babel/runtime/helpers/objectWithoutProperties"));

var _react = _interopRequireWildcard(require("react"));

var _reactBeautifulDnd = require("react-beautiful-dnd");

var _react2 = require("@emotion/react");

/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */
var EuiDragDropContextContext = /*#__PURE__*/(0, _react.createContext)({
  isDraggingType: null
});
exports.EuiDragDropContextContext = EuiDragDropContextContext;

var EuiDragDropContext = function EuiDragDropContext(_ref) {
  var onBeforeDragStart = _ref.onBeforeDragStart,
      onDragStart = _ref.onDragStart,
      onDragUpdate = _ref.onDragUpdate,
      onDragEnd = _ref.onDragEnd,
      children = _ref.children,
      rest = (0, _objectWithoutProperties2.default)(_ref, ["onBeforeDragStart", "onDragStart", "onDragUpdate", "onDragEnd", "children"]);

  var _useState = (0, _react.useState)(null),
      _useState2 = (0, _slicedToArray2.default)(_useState, 2),
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

  return (0, _react2.jsx)(_reactBeautifulDnd.DragDropContext, (0, _extends2.default)({
    onBeforeDragStart: onBeforeDragStart,
    onDragStart: euiOnDragStart,
    onDragUpdate: onDragUpdate,
    onDragEnd: euiOnDragEnd
  }, rest), (0, _react2.jsx)(EuiDragDropContextContext.Provider, {
    value: {
      isDraggingType: isDraggingType
    }
  }, children));
};

exports.EuiDragDropContext = EuiDragDropContext;