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
import classnames from 'classnames';
import React, { forwardRef, memo } from 'react';
import { EuiDataGridCell } from './data_grid_cell';
import { DefaultColumnFormatter } from './popover_utils';
import { jsx as ___EmotionJSX } from "@emotion/react";
var EuiDataGridFooterRow = /*#__PURE__*/memo( /*#__PURE__*/forwardRef(function (_ref, ref) {
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
      rest = _objectWithoutProperties(_ref, ["leadingControlColumns", "trailingControlColumns", "columns", "schema", "popoverContents", "columnWidths", "defaultColumnWidth", "className", "renderCellValue", "rowIndex", "interactiveCellId", "data-test-subj", "visibleRowIndex"]);

  var classes = classnames('euiDataGridRow', 'euiDataGridFooter', className);
  var dataTestSubj = classnames('dataGridRow', 'dataGridFooterRow', _dataTestSubj);
  var sharedCellProps = {
    rowIndex: rowIndex,
    visibleRowIndex: visibleRowIndex,
    interactiveCellId: interactiveCellId,
    isExpandable: true
  };
  return ___EmotionJSX("div", _extends({
    ref: ref,
    role: "row",
    className: classes,
    "data-test-subj": dataTestSubj
  }, rest), leadingControlColumns.map(function (_ref2, i) {
    var id = _ref2.id,
        width = _ref2.width;
    return ___EmotionJSX(EuiDataGridCell, _extends({}, sharedCellProps, {
      key: "".concat(id, "-").concat(rowIndex),
      colIndex: i,
      columnId: id,
      popoverContent: DefaultColumnFormatter,
      width: width,
      renderCellValue: function renderCellValue() {
        return null;
      },
      className: "euiDataGridFooterCell euiDataGridRowCell--controlColumn"
    }));
  }), columns.map(function (_ref3, i) {
    var id = _ref3.id;
    var columnType = schema[id] ? schema[id].columnType : null;
    var popoverContent = columnType && popoverContents[columnType] || DefaultColumnFormatter;
    var width = columnWidths[id] || defaultColumnWidth;
    var columnPosition = i + leadingControlColumns.length;
    return ___EmotionJSX(EuiDataGridCell, _extends({}, sharedCellProps, {
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
    return ___EmotionJSX(EuiDataGridCell, _extends({}, sharedCellProps, {
      key: "".concat(id, "-").concat(rowIndex),
      colIndex: colIndex,
      columnId: id,
      popoverContent: DefaultColumnFormatter,
      width: width,
      renderCellValue: function renderCellValue() {
        return null;
      },
      className: "euiDataGridFooterCell euiDataGridRowCell--controlColumn"
    }));
  }));
}));
EuiDataGridFooterRow.displayName = 'EuiDataGridFooterRow';
export { EuiDataGridFooterRow };