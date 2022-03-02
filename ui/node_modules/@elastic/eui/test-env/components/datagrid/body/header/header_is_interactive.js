"use strict";

var _interopRequireDefault = require("@babel/runtime/helpers/interopRequireDefault");

Object.defineProperty(exports, "__esModule", {
  value: true
});
exports.useHeaderIsInteractive = void 0;

var _slicedToArray2 = _interopRequireDefault(require("@babel/runtime/helpers/slicedToArray"));

var _react = require("react");

var _tabbable = _interopRequireDefault(require("tabbable"));

/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */
var useHeaderIsInteractive = function useHeaderIsInteractive(gridElement) {
  var _useState = (0, _react.useState)(false),
      _useState2 = (0, _slicedToArray2.default)(_useState, 2),
      headerIsInteractive = _useState2[0],
      setHeaderIsInteractive = _useState2[1];

  var handleHeaderChange = (0, _react.useCallback)(function (headerRow) {
    var tabbables = (0, _tabbable.default)(headerRow);
    var managed = headerRow.querySelectorAll('[data-euigrid-tab-managed]');
    var hasInteractives = tabbables.length > 0 || managed.length > 0;

    if (hasInteractives !== headerIsInteractive) {
      setHeaderIsInteractive(hasInteractives);
    }
  }, [headerIsInteractive]); // Set headerIsInteractive on data grid init/load

  (0, _react.useEffect)(function () {
    if (gridElement) {
      var headerElement = gridElement.querySelector('[data-test-subj~=dataGridHeader]');

      if (headerElement) {
        handleHeaderChange(headerElement);
      }
    }
  }, [gridElement, handleHeaderChange]); // Update headerIsInteractive if the header changes (e.g., columns are hidden)
  // Used in header mutation observer set in EuiDataGridBody

  var handleHeaderMutation = (0, _react.useCallback)(function (records) {
    var _records = (0, _slicedToArray2.default)(records, 1),
        target = _records[0].target; // find the wrapping header div


    var headerRow = target.parentElement;

    while (headerRow && (headerRow.getAttribute('data-test-subj') || '').split(/\s+/).indexOf('dataGridHeader') === -1) {
      headerRow = headerRow.parentElement;
    }

    if (headerRow) handleHeaderChange(headerRow);
  }, [handleHeaderChange]);
  return {
    headerIsInteractive: headerIsInteractive,
    handleHeaderMutation: handleHeaderMutation
  };
};

exports.useHeaderIsInteractive = useHeaderIsInteractive;