function _extends() { _extends = Object.assign || function (target) { for (var i = 1; i < arguments.length; i++) { var source = arguments[i]; for (var key in source) { if (Object.prototype.hasOwnProperty.call(source, key)) { target[key] = source[key]; } } } return target; }; return _extends.apply(this, arguments); }

function _objectWithoutProperties(source, excluded) { if (source == null) return {}; var target = _objectWithoutPropertiesLoose(source, excluded); var key, i; if (Object.getOwnPropertySymbols) { var sourceSymbolKeys = Object.getOwnPropertySymbols(source); for (i = 0; i < sourceSymbolKeys.length; i++) { key = sourceSymbolKeys[i]; if (excluded.indexOf(key) >= 0) continue; if (!Object.prototype.propertyIsEnumerable.call(source, key)) continue; target[key] = source[key]; } } return target; }

function _objectWithoutPropertiesLoose(source, excluded) { if (source == null) return {}; var target = {}; var sourceKeys = Object.keys(source); var key, i; for (i = 0; i < sourceKeys.length; i++) { key = sourceKeys[i]; if (excluded.indexOf(key) >= 0) continue; target[key] = source[key]; } return target; }

/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */
import React, { useContext } from 'react';
import PropTypes from "prop-types";
import { Droppable } from 'react-beautiful-dnd';
import classNames from 'classnames';
import { EuiDragDropContextContext } from './drag_drop_context';
import { jsx as ___EmotionJSX } from "@emotion/react";
var spacingToClassNameMap = {
  none: null,
  s: 'euiDroppable--s',
  m: 'euiDroppable--m',
  l: 'euiDroppable--l'
};
export var EuiDroppableContext = /*#__PURE__*/React.createContext({
  cloneItems: false
});
export var EuiDroppable = function EuiDroppable(_ref) {
  var droppableId = _ref.droppableId,
      direction = _ref.direction,
      _ref$isDropDisabled = _ref.isDropDisabled,
      isDropDisabled = _ref$isDropDisabled === void 0 ? false : _ref$isDropDisabled,
      children = _ref.children,
      className = _ref.className,
      _ref$cloneDraggables = _ref.cloneDraggables,
      cloneDraggables = _ref$cloneDraggables === void 0 ? false : _ref$cloneDraggables,
      _ref$spacing = _ref.spacing,
      spacing = _ref$spacing === void 0 ? 'none' : _ref$spacing,
      style = _ref.style,
      _ref$type = _ref.type,
      type = _ref$type === void 0 ? 'EUI_DEFAULT' : _ref$type,
      _ref$withPanel = _ref.withPanel,
      withPanel = _ref$withPanel === void 0 ? false : _ref$withPanel,
      _ref$grow = _ref.grow,
      grow = _ref$grow === void 0 ? false : _ref$grow,
      _ref$dataTestSubj = _ref['data-test-subj'],
      dataTestSubj = _ref$dataTestSubj === void 0 ? 'droppable' : _ref$dataTestSubj,
      rest = _objectWithoutProperties(_ref, ["droppableId", "direction", "isDropDisabled", "children", "className", "cloneDraggables", "spacing", "style", "type", "withPanel", "grow", "data-test-subj"]);

  var _useContext = useContext(EuiDragDropContextContext),
      isDraggingType = _useContext.isDraggingType;

  var dropIsDisabled = cloneDraggables ? true : isDropDisabled;
  return ___EmotionJSX(Droppable, _extends({
    isDropDisabled: dropIsDisabled,
    droppableId: droppableId,
    direction: direction,
    type: type
  }, rest), function (provided, snapshot) {
    var classes = classNames('euiDroppable', {
      'euiDroppable--isDisabled': dropIsDisabled,
      'euiDroppable--isDraggingOver': snapshot.isDraggingOver,
      'euiDroppable--isDraggingType': isDraggingType === type,
      'euiDroppable--withPanel': withPanel,
      'euiDroppable--grow': grow,
      'euiDroppable--noGrow': !grow
    }, spacingToClassNameMap[spacing], className);
    var placeholderClasses = classNames('euiDroppable__placeholder', {
      'euiDroppable__placeholder--isHidden': cloneDraggables
    });
    var DroppableElement = typeof children === 'function' ? children(provided, snapshot) : children;
    return ___EmotionJSX("div", _extends({}, provided.droppableProps, {
      ref: provided.innerRef,
      style: style,
      "data-test-subj": dataTestSubj,
      className: classes
    }), ___EmotionJSX(EuiDroppableContext.Provider, {
      value: {
        cloneItems: cloneDraggables
      }
    }, DroppableElement), ___EmotionJSX("div", {
      className: placeholderClasses
    }, provided.placeholder));
  });
};
EuiDroppable.propTypes = {
  /**
     * ReactNode to render as this component's content
     */
  children: PropTypes.oneOfType([PropTypes.element.isRequired, PropTypes.arrayOf(PropTypes.element.isRequired).isRequired, PropTypes.any.isRequired]).isRequired,
  className: PropTypes.string,

  /**
     * Makes its items immutable. Dragging creates cloned items that can be dropped elsewhere.
     */
  cloneDraggables: PropTypes.bool,
  style: PropTypes.any,

  /**
     * Adds padding to the droppable area
     */
  spacing: PropTypes.oneOf(["none", "s", "m", "l"]),

  /**
     * Adds an EuiPanel style to the droppable area
     */
  withPanel: PropTypes.bool,

  /**
     * Allow the panel to flex-grow?
     */
  grow: PropTypes.bool,
  "aria-label": PropTypes.string,
  "data-test-subj": PropTypes.string
};