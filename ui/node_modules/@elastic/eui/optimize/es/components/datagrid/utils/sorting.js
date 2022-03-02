/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */
import { createContext, useMemo, useCallback } from 'react';
import { defaultComparator } from './data_grid_schema';
export var DataGridSortingContext = /*#__PURE__*/createContext({
  sorting: undefined,
  sortedRowMap: {},
  getCorrectRowIndex: function getCorrectRowIndex(number) {
    return number;
  }
});
export var useSorting = function useSorting(_ref) {
  var sorting = _ref.sorting,
      inMemory = _ref.inMemory,
      inMemoryValues = _ref.inMemoryValues,
      schema = _ref.schema,
      schemaDetectors = _ref.schemaDetectors,
      startRow = _ref.startRow;
  var sortingColumns = sorting === null || sorting === void 0 ? void 0 : sorting.columns;
  var sortedRowMap = useMemo(function () {
    var rowMap = {};

    if ((inMemory === null || inMemory === void 0 ? void 0 : inMemory.level) === 'sorting' && sortingColumns != null && sortingColumns.length > 0) {
      var inMemoryRowIndices = Object.keys(inMemoryValues);
      var wrappedValues = [];

      for (var i = 0; i < inMemoryRowIndices.length; i++) {
        var inMemoryRow = inMemoryValues[inMemoryRowIndices[i]];
        wrappedValues.push({
          index: i,
          values: inMemoryRow
        });
      }

      wrappedValues.sort(function (a, b) {
        for (var _i = 0; _i < sortingColumns.length; _i++) {
          var column = sortingColumns[_i];
          var aValue = a.values[column.id];
          var bValue = b.values[column.id]; // get the comparator, based on schema

          var comparator = defaultComparator;

          if (schema.hasOwnProperty(column.id)) {
            var columnType = schema[column.id].columnType;

            for (var _i2 = 0; _i2 < schemaDetectors.length; _i2++) {
              var detector = schemaDetectors[_i2];

              if (detector.type === columnType && detector.hasOwnProperty('comparator')) {
                comparator = detector.comparator;
              }
            }
          }

          var result = comparator(aValue, bValue, column.direction); // only return if the columns are unequal, otherwise allow the next sort-by column to run

          if (result !== 0) return result;
        }

        return 0;
      });

      for (var _i3 = 0; _i3 < wrappedValues.length; _i3++) {
        rowMap[_i3] = wrappedValues[_i3].index;
      }
    }

    return rowMap;
  }, [inMemory === null || inMemory === void 0 ? void 0 : inMemory.level, inMemoryValues, sortingColumns, schema, schemaDetectors]);
  var getCorrectRowIndex = useCallback(function (rowIndex) {
    var rowIndexWithOffset = rowIndex;

    if (rowIndex - startRow < 0) {
      rowIndexWithOffset = rowIndex + startRow;
    }

    var correctRowIndex = sortedRowMap.hasOwnProperty(rowIndexWithOffset) ? sortedRowMap[rowIndexWithOffset] : rowIndexWithOffset;
    return correctRowIndex;
  }, [startRow, sortedRowMap]);
  return {
    sorting: sorting,
    sortedRowMap: sortedRowMap,
    getCorrectRowIndex: getCorrectRowIndex
  };
};