/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */
import React, { useEffect } from 'react';
import { EuiSpacer } from '../spacer';
import { EuiTablePagination } from '../table';
import { jsx as ___EmotionJSX } from "@emotion/react";
export var defaults = {
  pageSizeOptions: [10, 25, 50]
};
export var PaginationBar = function PaginationBar(_ref) {
  var pagination = _ref.pagination,
      onPageSizeChange = _ref.onPageSizeChange,
      onPageChange = _ref.onPageChange,
      ariaControls = _ref['aria-controls'],
      ariaLabel = _ref['aria-label'];
  var pageSizeOptions = pagination.pageSizeOptions ? pagination.pageSizeOptions : defaults.pageSizeOptions;
  var pageCount = Math.ceil(pagination.totalItemCount / pagination.pageSize);
  useEffect(function () {
    if (pageCount < pagination.pageIndex + 1) {
      onPageChange(pageCount - 1);
    }
  }, [pageCount, onPageChange, pagination]);
  return ___EmotionJSX("div", null, ___EmotionJSX(EuiSpacer, {
    size: "m"
  }), ___EmotionJSX(EuiTablePagination, {
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