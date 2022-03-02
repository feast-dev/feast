"use strict";

var _interopRequireDefault = require("@babel/runtime/helpers/interopRequireDefault");

Object.defineProperty(exports, "__esModule", {
  value: true
});
exports.EuiDataGridCellPopover = EuiDataGridCellPopover;

var _extends2 = _interopRequireDefault(require("@babel/runtime/helpers/extends"));

var _react = _interopRequireDefault(require("react"));

var _services = require("../../../services");

var _button_empty = require("../../button/button_empty");

var _flex = require("../../flex");

var _popover = require("../../popover");

var _react2 = require("@emotion/react");

/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */
function EuiDataGridCellPopover(_ref) {
  var anchorContent = _ref.anchorContent,
      cellContentProps = _ref.cellContentProps,
      cellContentsRef = _ref.cellContentsRef,
      closePopover = _ref.closePopover,
      column = _ref.column,
      panelRefFn = _ref.panelRefFn,
      PopoverContent = _ref.popoverContent,
      popoverIsOpen = _ref.popoverIsOpen,
      renderCellValue = _ref.renderCellValue,
      rowIndex = _ref.rowIndex;
  var CellElement = renderCellValue;
  return (0, _react2.jsx)(_popover.EuiPopover, {
    hasArrow: false,
    anchorClassName: "euiDataGridRowCell__expand",
    button: anchorContent,
    isOpen: popoverIsOpen,
    panelRef: panelRefFn,
    panelClassName: "euiDataGridRowCell__popover",
    panelPaddingSize: "s",
    display: "block",
    closePopover: closePopover,
    panelProps: {
      'data-test-subj': 'euiDataGridExpansionPopover'
    },
    onKeyDown: function onKeyDown(event) {
      if (event.key === _services.keys.F2 || event.key === _services.keys.ESCAPE) {
        event.preventDefault();
        event.stopPropagation();
        closePopover();
      }
    }
  }, popoverIsOpen ? (0, _react2.jsx)(_react.default.Fragment, null, (0, _react2.jsx)(PopoverContent, {
    cellContentsElement: cellContentsRef
  }, (0, _react2.jsx)(CellElement, (0, _extends2.default)({}, cellContentProps, {
    isDetails: true
  }))), column && column.cellActions && column.cellActions.length ? (0, _react2.jsx)(_popover.EuiPopoverFooter, null, (0, _react2.jsx)(_flex.EuiFlexGroup, {
    gutterSize: "s"
  }, column.cellActions.map(function (Action, idx) {
    var CellButtonElement = Action;
    return (0, _react2.jsx)(_flex.EuiFlexItem, {
      key: idx
    }, (0, _react2.jsx)(CellButtonElement, {
      rowIndex: rowIndex,
      columnId: column.id,
      Component: function Component(props) {
        return (0, _react2.jsx)(_button_empty.EuiButtonEmpty, (0, _extends2.default)({}, props, {
          size: "s"
        }));
      },
      isExpanded: true,
      closePopover: closePopover
    }));
  }))) : null) : null);
}