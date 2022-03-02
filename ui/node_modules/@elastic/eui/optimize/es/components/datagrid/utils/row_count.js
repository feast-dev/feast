/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */
export var computeVisibleRows = function computeVisibleRows(_ref) {
  var pagination = _ref.pagination,
      rowCount = _ref.rowCount;
  var startRow = pagination ? pagination.pageIndex * pagination.pageSize : 0;
  var endRow = pagination ? (pagination.pageIndex + 1) * pagination.pageSize : rowCount;
  endRow = Math.min(endRow, rowCount);
  var visibleRowCount = endRow - startRow;
  return {
    startRow: startRow,
    endRow: endRow,
    visibleRowCount: visibleRowCount
  };
};