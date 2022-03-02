import _extends from "@babel/runtime/helpers/extends";
import _objectWithoutProperties from "@babel/runtime/helpers/objectWithoutProperties";

/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */
import classnames from 'classnames';
import React, { forwardRef } from 'react';
import { EuiDataGridControlHeaderCell } from './data_grid_control_header_cell';
import { EuiDataGridHeaderCell } from './data_grid_header_cell';
import { jsx as ___EmotionJSX } from "@emotion/react";
var EuiDataGridHeaderRow = /*#__PURE__*/forwardRef(function (props, ref) {
  var _props$leadingControl = props.leadingControlColumns,
      leadingControlColumns = _props$leadingControl === void 0 ? [] : _props$leadingControl,
      _props$trailingContro = props.trailingControlColumns,
      trailingControlColumns = _props$trailingContro === void 0 ? [] : _props$trailingContro,
      columns = props.columns,
      schema = props.schema,
      schemaDetectors = props.schemaDetectors,
      columnWidths = props.columnWidths,
      defaultColumnWidth = props.defaultColumnWidth,
      className = props.className,
      setColumnWidth = props.setColumnWidth,
      setVisibleColumns = props.setVisibleColumns,
      switchColumnPos = props.switchColumnPos,
      headerIsInteractive = props.headerIsInteractive,
      _dataTestSubj = props['data-test-subj'],
      rest = _objectWithoutProperties(props, ["leadingControlColumns", "trailingControlColumns", "columns", "schema", "schemaDetectors", "columnWidths", "defaultColumnWidth", "className", "setColumnWidth", "setVisibleColumns", "switchColumnPos", "headerIsInteractive", "data-test-subj"]);

  var classes = classnames('euiDataGridHeader', className);
  var dataTestSubj = classnames('dataGridHeader', _dataTestSubj);
  return ___EmotionJSX("div", _extends({
    role: "row",
    ref: ref,
    className: classes,
    "data-test-subj": dataTestSubj
  }, rest), leadingControlColumns.map(function (controlColumn, index) {
    return ___EmotionJSX(EuiDataGridControlHeaderCell, {
      key: controlColumn.id,
      index: index,
      controlColumn: controlColumn,
      headerIsInteractive: headerIsInteractive
    });
  }), columns.map(function (column, index) {
    return ___EmotionJSX(EuiDataGridHeaderCell, {
      key: column.id,
      column: column,
      columns: columns,
      index: index + leadingControlColumns.length,
      columnWidths: columnWidths,
      schema: schema,
      schemaDetectors: schemaDetectors,
      setColumnWidth: setColumnWidth,
      setVisibleColumns: setVisibleColumns,
      switchColumnPos: switchColumnPos,
      defaultColumnWidth: defaultColumnWidth,
      headerIsInteractive: headerIsInteractive
    });
  }), trailingControlColumns.map(function (controlColumn, index) {
    return ___EmotionJSX(EuiDataGridControlHeaderCell, {
      key: controlColumn.id,
      index: index + leadingControlColumns.length + columns.length,
      controlColumn: controlColumn,
      headerIsInteractive: headerIsInteractive
    });
  }));
});
EuiDataGridHeaderRow.displayName = 'EuiDataGridHeaderRow';
export { EuiDataGridHeaderRow };