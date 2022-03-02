import _slicedToArray from "@babel/runtime/helpers/slicedToArray";

/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */
import { useCallback, useEffect, useState } from 'react';
import tabbable from 'tabbable';
export var useHeaderIsInteractive = function useHeaderIsInteractive(gridElement) {
  var _useState = useState(false),
      _useState2 = _slicedToArray(_useState, 2),
      headerIsInteractive = _useState2[0],
      setHeaderIsInteractive = _useState2[1];

  var handleHeaderChange = useCallback(function (headerRow) {
    var tabbables = tabbable(headerRow);
    var managed = headerRow.querySelectorAll('[data-euigrid-tab-managed]');
    var hasInteractives = tabbables.length > 0 || managed.length > 0;

    if (hasInteractives !== headerIsInteractive) {
      setHeaderIsInteractive(hasInteractives);
    }
  }, [headerIsInteractive]); // Set headerIsInteractive on data grid init/load

  useEffect(function () {
    if (gridElement) {
      var headerElement = gridElement.querySelector('[data-test-subj~=dataGridHeader]');

      if (headerElement) {
        handleHeaderChange(headerElement);
      }
    }
  }, [gridElement, handleHeaderChange]); // Update headerIsInteractive if the header changes (e.g., columns are hidden)
  // Used in header mutation observer set in EuiDataGridBody

  var handleHeaderMutation = useCallback(function (records) {
    var _records = _slicedToArray(records, 1),
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