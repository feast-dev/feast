"use strict";

var _interopRequireDefault = require("@babel/runtime/helpers/interopRequireDefault");

Object.defineProperty(exports, "__esModule", {
  value: true
});
exports.useColumnWidths = exports.doesColumnHaveAnInitialWidth = exports.useDefaultColumnWidth = void 0;

var _defineProperty2 = _interopRequireDefault(require("@babel/runtime/helpers/defineProperty"));

var _slicedToArray2 = _interopRequireDefault(require("@babel/runtime/helpers/slicedToArray"));

var _toConsumableArray2 = _interopRequireDefault(require("@babel/runtime/helpers/toConsumableArray"));

var _react = require("react");

var _test = require("../../../test");

function ownKeys(object, enumerableOnly) { var keys = Object.keys(object); if (Object.getOwnPropertySymbols) { var symbols = Object.getOwnPropertySymbols(object); if (enumerableOnly) symbols = symbols.filter(function (sym) { return Object.getOwnPropertyDescriptor(object, sym).enumerable; }); keys.push.apply(keys, symbols); } return keys; }

function _objectSpread(target) { for (var i = 1; i < arguments.length; i++) { var source = arguments[i] != null ? arguments[i] : {}; if (i % 2) { ownKeys(Object(source), true).forEach(function (key) { (0, _defineProperty2.default)(target, key, source[key]); }); } else if (Object.getOwnPropertyDescriptors) { Object.defineProperties(target, Object.getOwnPropertyDescriptors(source)); } else { ownKeys(Object(source)).forEach(function (key) { Object.defineProperty(target, key, Object.getOwnPropertyDescriptor(source, key)); }); } } return target; }

var DEFAULT_COLUMN_WIDTH = 100;

var useDefaultColumnWidth = function useDefaultColumnWidth(gridWidth, leadingControlColumns, trailingControlColumns, columns) {
  var computeDefaultWidth = (0, _react.useCallback)(function () {
    if (_test.IS_JEST_ENVIRONMENT) return DEFAULT_COLUMN_WIDTH;
    if (gridWidth === 0) return null; // we can't tell what size to compute yet

    var controlColumnWidths = [].concat((0, _toConsumableArray2.default)(leadingControlColumns), (0, _toConsumableArray2.default)(trailingControlColumns)).reduce(function (claimedWidth, controlColumn) {
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

  var _useState = (0, _react.useState)(computeDefaultWidth),
      _useState2 = (0, _slicedToArray2.default)(_useState, 2),
      defaultColumnWidth = _useState2[0],
      setDefaultColumnWidth = _useState2[1];

  (0, _react.useEffect)(function () {
    var columnWidth = computeDefaultWidth();
    setDefaultColumnWidth(columnWidth);
  }, [computeDefaultWidth]);
  return defaultColumnWidth;
};

exports.useDefaultColumnWidth = useDefaultColumnWidth;

var doesColumnHaveAnInitialWidth = function doesColumnHaveAnInitialWidth(column) {
  return column.hasOwnProperty('initialWidth');
};

exports.doesColumnHaveAnInitialWidth = doesColumnHaveAnInitialWidth;

var useColumnWidths = function useColumnWidths(_ref) {
  var columns = _ref.columns,
      leadingControlColumns = _ref.leadingControlColumns,
      trailingControlColumns = _ref.trailingControlColumns,
      defaultColumnWidth = _ref.defaultColumnWidth,
      onColumnResize = _ref.onColumnResize;
  var hasMounted = (0, _react.useRef)(false);
  var computeColumnWidths = (0, _react.useCallback)(function () {
    return columns.filter(doesColumnHaveAnInitialWidth).reduce(function (initialWidths, column) {
      initialWidths[column.id] = column.initialWidth;
      return initialWidths;
    }, {});
  }, [columns]);

  var _useState3 = (0, _react.useState)(computeColumnWidths),
      _useState4 = (0, _slicedToArray2.default)(_useState3, 2),
      columnWidths = _useState4[0],
      setColumnWidths = _useState4[1];

  (0, _react.useEffect)(function () {
    if (!hasMounted.current) {
      hasMounted.current = true;
      return;
    }

    setColumnWidths(computeColumnWidths());
  }, [computeColumnWidths]);
  var setColumnWidth = (0, _react.useCallback)(function (columnId, width) {
    setColumnWidths(_objectSpread(_objectSpread({}, columnWidths), {}, (0, _defineProperty2.default)({}, columnId, width)));

    if (onColumnResize) {
      onColumnResize({
        columnId: columnId,
        width: width
      });
    }
  }, [columnWidths, onColumnResize]); // Used by react-window to determine actual column widths

  var getColumnWidth = (0, _react.useCallback)(function (index) {
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

exports.useColumnWidths = useColumnWidths;