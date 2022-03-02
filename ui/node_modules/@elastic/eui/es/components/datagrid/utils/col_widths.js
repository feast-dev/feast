function ownKeys(object, enumerableOnly) { var keys = Object.keys(object); if (Object.getOwnPropertySymbols) { var symbols = Object.getOwnPropertySymbols(object); if (enumerableOnly) symbols = symbols.filter(function (sym) { return Object.getOwnPropertyDescriptor(object, sym).enumerable; }); keys.push.apply(keys, symbols); } return keys; }

function _objectSpread(target) { for (var i = 1; i < arguments.length; i++) { var source = arguments[i] != null ? arguments[i] : {}; if (i % 2) { ownKeys(Object(source), true).forEach(function (key) { _defineProperty(target, key, source[key]); }); } else if (Object.getOwnPropertyDescriptors) { Object.defineProperties(target, Object.getOwnPropertyDescriptors(source)); } else { ownKeys(Object(source)).forEach(function (key) { Object.defineProperty(target, key, Object.getOwnPropertyDescriptor(source, key)); }); } } return target; }

function _defineProperty(obj, key, value) { if (key in obj) { Object.defineProperty(obj, key, { value: value, enumerable: true, configurable: true, writable: true }); } else { obj[key] = value; } return obj; }

function _slicedToArray(arr, i) { return _arrayWithHoles(arr) || _iterableToArrayLimit(arr, i) || _unsupportedIterableToArray(arr, i) || _nonIterableRest(); }

function _nonIterableRest() { throw new TypeError("Invalid attempt to destructure non-iterable instance.\nIn order to be iterable, non-array objects must have a [Symbol.iterator]() method."); }

function _iterableToArrayLimit(arr, i) { if (typeof Symbol === "undefined" || !(Symbol.iterator in Object(arr))) return; var _arr = []; var _n = true; var _d = false; var _e = undefined; try { for (var _i = arr[Symbol.iterator](), _s; !(_n = (_s = _i.next()).done); _n = true) { _arr.push(_s.value); if (i && _arr.length === i) break; } } catch (err) { _d = true; _e = err; } finally { try { if (!_n && _i["return"] != null) _i["return"](); } finally { if (_d) throw _e; } } return _arr; }

function _arrayWithHoles(arr) { if (Array.isArray(arr)) return arr; }

function _toConsumableArray(arr) { return _arrayWithoutHoles(arr) || _iterableToArray(arr) || _unsupportedIterableToArray(arr) || _nonIterableSpread(); }

function _nonIterableSpread() { throw new TypeError("Invalid attempt to spread non-iterable instance.\nIn order to be iterable, non-array objects must have a [Symbol.iterator]() method."); }

function _unsupportedIterableToArray(o, minLen) { if (!o) return; if (typeof o === "string") return _arrayLikeToArray(o, minLen); var n = Object.prototype.toString.call(o).slice(8, -1); if (n === "Object" && o.constructor) n = o.constructor.name; if (n === "Map" || n === "Set") return Array.from(o); if (n === "Arguments" || /^(?:Ui|I)nt(?:8|16|32)(?:Clamped)?Array$/.test(n)) return _arrayLikeToArray(o, minLen); }

function _iterableToArray(iter) { if (typeof Symbol !== "undefined" && Symbol.iterator in Object(iter)) return Array.from(iter); }

function _arrayWithoutHoles(arr) { if (Array.isArray(arr)) return _arrayLikeToArray(arr); }

function _arrayLikeToArray(arr, len) { if (len == null || len > arr.length) len = arr.length; for (var i = 0, arr2 = new Array(len); i < len; i++) { arr2[i] = arr[i]; } return arr2; }

/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */
import { useCallback, useEffect, useRef, useState } from 'react';
import { IS_JEST_ENVIRONMENT } from '../../../test';
var DEFAULT_COLUMN_WIDTH = 100;
export var useDefaultColumnWidth = function useDefaultColumnWidth(gridWidth, leadingControlColumns, trailingControlColumns, columns) {
  var computeDefaultWidth = useCallback(function () {
    if (IS_JEST_ENVIRONMENT) return DEFAULT_COLUMN_WIDTH;
    if (gridWidth === 0) return null; // we can't tell what size to compute yet

    var controlColumnWidths = [].concat(_toConsumableArray(leadingControlColumns), _toConsumableArray(trailingControlColumns)).reduce(function (claimedWidth, controlColumn) {
      return claimedWidth + controlColumn.width;
    }, 0);
    var columnsWithWidths = columns.filter(doesColumnHaveAnInitialWidth);
    var definedColumnsWidth = columnsWithWidths.reduce(function (claimedWidth, column) {
      return claimedWidth + column.initialWidth;
    }, 0);
    var claimedWidth = controlColumnWidths + definedColumnsWidth;
    var widthToFill = gridWidth - claimedWidth;
    var unsizedColumnCount = columns.length - columnsWithWidths.length;

    if (unsizedColumnCount === 0) {
      return DEFAULT_COLUMN_WIDTH;
    }

    return Math.max(widthToFill / unsizedColumnCount, DEFAULT_COLUMN_WIDTH);
  }, [gridWidth, columns, leadingControlColumns, trailingControlColumns]);

  var _useState = useState(computeDefaultWidth),
      _useState2 = _slicedToArray(_useState, 2),
      defaultColumnWidth = _useState2[0],
      setDefaultColumnWidth = _useState2[1];

  useEffect(function () {
    var columnWidth = computeDefaultWidth();
    setDefaultColumnWidth(columnWidth);
  }, [computeDefaultWidth]);
  return defaultColumnWidth;
};
export var doesColumnHaveAnInitialWidth = function doesColumnHaveAnInitialWidth(column) {
  return column.hasOwnProperty('initialWidth');
};
export var useColumnWidths = function useColumnWidths(_ref) {
  var columns = _ref.columns,
      leadingControlColumns = _ref.leadingControlColumns,
      trailingControlColumns = _ref.trailingControlColumns,
      defaultColumnWidth = _ref.defaultColumnWidth,
      onColumnResize = _ref.onColumnResize;
  var hasMounted = useRef(false);
  var computeColumnWidths = useCallback(function () {
    return columns.filter(doesColumnHaveAnInitialWidth).reduce(function (initialWidths, column) {
      initialWidths[column.id] = column.initialWidth;
      return initialWidths;
    }, {});
  }, [columns]);

  var _useState3 = useState(computeColumnWidths),
      _useState4 = _slicedToArray(_useState3, 2),
      columnWidths = _useState4[0],
      setColumnWidths = _useState4[1];

  useEffect(function () {
    if (!hasMounted.current) {
      hasMounted.current = true;
      return;
    }

    setColumnWidths(computeColumnWidths());
  }, [computeColumnWidths]);
  var setColumnWidth = useCallback(function (columnId, width) {
    setColumnWidths(_objectSpread(_objectSpread({}, columnWidths), {}, _defineProperty({}, columnId, width)));

    if (onColumnResize) {
      onColumnResize({
        columnId: columnId,
        width: width
      });
    }
  }, [columnWidths, onColumnResize]); // Used by react-window to determine actual column widths

  var getColumnWidth = useCallback(function (index) {
    if (index < leadingControlColumns.length) {
      // this is a leading control column
      return leadingControlColumns[index].width;
    } else if (index >= leadingControlColumns.length + columns.length) {
      // this is a trailing control column
      return trailingControlColumns[index - leadingControlColumns.length - columns.length].width;
    } // normal data column


    var columnId = columns[index - leadingControlColumns.length].id;
    return columnWidths[columnId] || defaultColumnWidth || DEFAULT_COLUMN_WIDTH;
  }, [columns, leadingControlColumns, trailingControlColumns, columnWidths, defaultColumnWidth]);
  return {
    columnWidths: columnWidths,
    setColumnWidth: setColumnWidth,
    getColumnWidth: getColumnWidth
  };
};