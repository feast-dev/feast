/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */
import React from 'react';
import { EuiDataGridHeaderCellWrapper } from './data_grid_header_cell_wrapper';
import { jsx as ___EmotionJSX } from "@emotion/react";
export var EuiDataGridControlHeaderCell = function EuiDataGridControlHeaderCell(_ref) {
  var controlColumn = _ref.controlColumn,
      index = _ref.index,
      headerIsInteractive = _ref.headerIsInteractive;
  var HeaderCellRender = controlColumn.headerCellRender,
      width = controlColumn.width,
      id = controlColumn.id;
  return ___EmotionJSX(EuiDataGridHeaderCellWrapper, {
    className: "euiDataGridHeaderCell--controlColumn",
    id: id,
    index: index,
    width: width,
    headerIsInteractive: headerIsInteractive
  }, ___EmotionJSX("div", {
    className: "euiDataGridHeaderCell__content"
  }, ___EmotionJSX(HeaderCellRender, null)));
};