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