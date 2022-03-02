"use strict";

var _interopRequireWildcard = require("@babel/runtime/helpers/interopRequireWildcard");

Object.defineProperty(exports, "__esModule", {
  value: true
});
exports.PaginationBar = exports.defaults = void 0;

var _react = _interopRequireWildcard(require("react"));

var _spacer = require("../spacer");

var _table = require("../table");

var _react2 = require("@emotion/react");

/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */
var defaults = {
  pageSizeOptions: [10, 25, 50]
};
exports.defaults = defaults;

var PaginationBar = function PaginationBar(_ref) {
  var pagination = _ref.pagination,
      onPageSizeChange = _ref.onPageSizeChange,
      onPageChange = _ref.onPageChange,
      ariaControls = _ref['aria-controls'],
      ariaLabel = _ref['aria-label'];
  var pageSizeOptions = pagination.pageSizeOptions ? pagination.pageSizeOptions : defaults.pageSizeOptions;
  var pageCount = Math.ceil(pagination.totalItemCount / pagination.pageSize);
  (0, _react.useEffect)(function () {
    if (pageCount < pagination.pageIndex + 1) {
      onPageChange(pageCount - 1);
    }
  }, [pageCount, onPageChange, pagination]);
  return (0, _react2.jsx)("div", null, (0, _react2.jsx)(_spacer.EuiSpacer, {
    size: "m"
  }), (0, _react2.jsx)(_table.EuiTablePagination, {
    activePage: pagination.pageIndex,
    hidePerPageOptions: pagination.hidePerPageOptions,
    itemsPerPage: pagination.pageSize,
    itemsPerPageOptions: pageSizeOptions,
    pageCount: pageCount,
    onChangeItemsPerPage: onPageSizeChange,
    onChangePage: onPageChange,
    "aria-controls": ariaControls,
    "aria-label": ariaLabel
  }));
};

exports.PaginationBar = PaginationBar;