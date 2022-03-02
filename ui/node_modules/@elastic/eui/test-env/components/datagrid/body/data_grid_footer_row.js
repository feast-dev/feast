"use strict";

var _interopRequireWildcard = require("@babel/runtime/helpers/interopRequireWildcard");

var _interopRequireDefault = require("@babel/runtime/helpers/interopRequireDefault");

Object.defineProperty(exports, "__esModule", {
  value: true
});
exports.EuiDataGridFooterRow = void 0;

var _extends2 = _interopRequireDefault(require("@babel/runtime/helpers/extends"));

var _objectWithoutProperties2 = _interopRequireDefault(require("@babel/runtime/helpers/objectWithoutProperties"));

var _classnames = _interopRequireDefault(require("classnames"));

var _react = _interopRequireWildcard(require("react"));

var _data_grid_cell = require("./data_grid_cell");

var _popover_utils = require("./popover_utils");

var _react2 = require("@emotion/react");

/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */
var EuiDataGridFooterRow = /*#__PURE__*/(0, _react.memo)( /*#__PURE__*/(0, _react.forwardRef)(function (_ref, ref) {
  var leadingControlColumns = _ref.leadingControlColumns,
      trailingControlColumns = _ref.trailingControlColumns,
      columns = _ref.columns,
      schema = _ref.schema,
      popoverContents = _ref.popoverContents,
      columnWidths = _ref.columnWidths,
      defaultColumnWidth = _ref.defaultColumnWidth,
      className = _ref.className,
      renderCellValue = _ref.renderCellValue,
      rowIndex = _ref.rowIndex,
      interactiveCellId = _ref.interactiveCellId,
      _dataTestSubj = _ref['data-test-subj'],
      _ref$visibleRowIndex = _ref.visibleRowIndex,
      visibleRowIndex = _ref$visibleRowIndex === void 0 ? rowIndex : _ref$visibleRowIndex,
      rest = (0, _objectWithoutProperties2.default)(_ref, ["leadingControlColumns", "trailingControlColumns", "columns", "schema", "popoverContents", "columnWidths", "defaultColumnWidth", "className", "renderCellValue", "rowIndex", "interactiveCellId", "data-test-subj", "visibleRowIndex"]);
  var classes = (0, _classnames.default)('euiDataGridRow', 'euiDataGridFooter', className);
  var dataTestSubj = (0, _classnames.default)('dataGridRow', 'dataGridFooterRow', _dataTestSubj);
  var sharedCellProps = {
    rowIndex: rowIndex,
    visibleRowIndex: visibleRowIndex,
    interactiveCellId: interactiveCellId,
    isExpandable: true
  };
  return (0, _react2.jsx)("div", (0, _extends2.default)({
    ref: ref,
    role: "row",
    className: classes,
    "data-test-subj": dataTestSubj
  }, rest), leadingControlColumns.map(function (_ref2, i) {
    var id = _ref2.id,
        width = _ref2.width;
    return (0, _react2.jsx)(_data_grid_cell.EuiDataGridCell, (0, _extends2.default)({}, sharedCellProps, {
      key: "".concat(id, "-").concat(rowIndex),
      colIndex: i,
      columnId: id,
      popoverContent: _popover_utils.DefaultColumnFormatter,
      width: width,
      renderCellValue: function renderCellValue() {
        return null;
      },
      className: "euiDataGridFooterCell euiDataGridRowCell--controlColumn"
    }));
  }), columns.map(function (_ref3, i) {
    var id = _ref3.id;
    var columnType = schema[id] ? schema[id].columnType : null;
    var popoverContent = columnType && popoverContents[columnType] || _popover_utils.DefaultColumnFormatter;
    var width = columnWidths[id] || defaultColumnWidth;
    var columnPosition = i + leadingControlColumns.length;
    return (0, _react2.jsx)(_data_grid_cell.EuiDataGridCell, (0, _extends2.default)({}, sharedCellProps, {
      key: "".concat(id, "-").concat(rowIndex),
      colIndex: columnPosition,
      columnId: id,
      columnType: columnType,
      popoverContent: popoverContent,
      width: width || undefined,
      renderCellValue: renderCellValue,
      className: "euiDataGridFooterCell"
    }));
  }), trailingControlColumns.map(function (_ref4, i) {
    var id = _ref4.id,
        width = _ref4.width;
    var colIndex = i + columns.length + leadingControlColumns.length;
    return (0, _react2.jsx)(_data_grid_cell.EuiDataGridCell, (0, _extends2.default)({}, sharedCellProps, {
      key: "".concat(id, "-").concat(rowIndex),
      colIndex: colIndex,
      columnId: id,
      popoverContent: _popover_utils.DefaultColumnFormatter,
      width: width,
      renderCellValue: function renderCellValue() {
        return null;
      },
      className: "euiDataGridFooterCell euiDataGridRowCell--controlColumn"
    }));
  }));
}));
exports.EuiDataGridFooterRow = EuiDataGridFooterRow;
EuiDataGridFooterRow.displayName = 'EuiDataGridFooterRow';