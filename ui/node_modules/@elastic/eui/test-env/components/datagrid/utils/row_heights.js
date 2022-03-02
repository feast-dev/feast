"use strict";

var _interopRequireDefault = require("@babel/runtime/helpers/interopRequireDefault");

Object.defineProperty(exports, "__esModule", {
  value: true
});
exports.useDefaultRowHeight = exports.useRowHeightUtils = exports.RowHeightUtils = exports.DEFAULT_ROW_HEIGHT = exports.AUTO_HEIGHT = exports.cellPaddingsMap = void 0;

var _slicedToArray2 = _interopRequireDefault(require("@babel/runtime/helpers/slicedToArray"));

var _toConsumableArray2 = _interopRequireDefault(require("@babel/runtime/helpers/toConsumableArray"));

var _classCallCheck2 = _interopRequireDefault(require("@babel/runtime/helpers/classCallCheck"));

var _createClass2 = _interopRequireDefault(require("@babel/runtime/helpers/createClass"));

var _defineProperty2 = _interopRequireDefault(require("@babel/runtime/helpers/defineProperty"));

var _react = require("react");

var _predicate = require("../../../services/predicate");

var _sorting = require("./sorting");

/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */
// TODO: Once JS variables are available, use them here instead of hard-coded maps
var cellPaddingsMap = {
  s: 4,
  m: 6,
  l: 8
};
exports.cellPaddingsMap = cellPaddingsMap;
var AUTO_HEIGHT = 'auto';
exports.AUTO_HEIGHT = AUTO_HEIGHT;
var DEFAULT_ROW_HEIGHT = 34;
exports.DEFAULT_ROW_HEIGHT = DEFAULT_ROW_HEIGHT;

var RowHeightUtils = /*#__PURE__*/function () {
  function RowHeightUtils() {
    var _this = this;

    (0, _classCallCheck2.default)(this, RowHeightUtils);
    (0, _defineProperty2.default)(this, "styles", {
      paddingTop: 0,
      paddingBottom: 0
    });
    (0, _defineProperty2.default)(this, "getStylesForCell", function (rowHeightsOptions, rowIndex) {
      var height = _this.getRowHeightOption(rowIndex, rowHeightsOptions);

      if (height === AUTO_HEIGHT) {
        return {};
      }

      var lineCount = _this.getLineCount(height);

      if (lineCount) {
        return {
          WebkitLineClamp: lineCount,
          display: '-webkit-box',
          WebkitBoxOrient: 'vertical',
          height: '100%',
          overflow: 'hidden'
        };
      }

      return {
        height: '100%',
        overflow: 'hidden'
      };
    });
    (0, _defineProperty2.default)(this, "heightsCache", new Map());
    (0, _defineProperty2.default)(this, "timerId", void 0);
    (0, _defineProperty2.default)(this, "grid", void 0);
    (0, _defineProperty2.default)(this, "lastUpdatedRow", Infinity);
    (0, _defineProperty2.default)(this, "rerenderGridBody", function () {});
  }

  (0, _createClass2.default)(RowHeightUtils, [{
    key: "getRowHeightOption",
    value: function getRowHeightOption(rowIndex, rowHeightsOptions) {
      var _rowHeightsOptions$ro, _rowHeightsOptions$ro2;

      return (_rowHeightsOptions$ro = rowHeightsOptions === null || rowHeightsOptions === void 0 ? void 0 : (_rowHeightsOptions$ro2 = rowHeightsOptions.rowHeights) === null || _rowHeightsOptions$ro2 === void 0 ? void 0 : _rowHeightsOptions$ro2[rowIndex]) !== null && _rowHeightsOptions$ro !== void 0 ? _rowHeightsOptions$ro : rowHeightsOptions === null || rowHeightsOptions === void 0 ? void 0 : rowHeightsOptions.defaultHeight;
    }
  }, {
    key: "isRowHeightOverride",
    value: function isRowHeightOverride(rowIndex, rowHeightsOptions) {
      var _rowHeightsOptions$ro3;

      return (rowHeightsOptions === null || rowHeightsOptions === void 0 ? void 0 : (_rowHeightsOptions$ro3 = rowHeightsOptions.rowHeights) === null || _rowHeightsOptions$ro3 === void 0 ? void 0 : _rowHeightsOptions$ro3[rowIndex]) != null;
    }
  }, {
    key: "getCalculatedHeight",
    value: function getCalculatedHeight(heightOption, defaultHeight, rowIndex, isRowHeightOverride) {
      if ((0, _predicate.isObject)(heightOption) && heightOption.height) {
        return Math.max(heightOption.height, defaultHeight);
      }

      if (heightOption && (0, _predicate.isNumber)(heightOption)) {
        return Math.max(heightOption, defaultHeight);
      }

      if ((0, _predicate.isObject)(heightOption) && heightOption.lineCount) {
        if (isRowHeightOverride) {
          return this.getRowHeight(rowIndex) || defaultHeight; // lineCount overrides are stored in the heights cache
        } else {
          return defaultHeight; // default lineCount height is set in minRowHeight state in grid_row_body
        }
      }

      if (heightOption === AUTO_HEIGHT && rowIndex != null) {
        return this.getRowHeight(rowIndex);
      }

      return defaultHeight;
    }
    /**
     * Styles utils
     */

  }, {
    key: "cacheStyles",
    value: function cacheStyles(gridStyles) {
      this.styles = {
        paddingTop: cellPaddingsMap[gridStyles.cellPadding],
        paddingBottom: cellPaddingsMap[gridStyles.cellPadding]
      };
    }
  }, {
    key: "getLineCount",

    /**
     * Line count utils
     */
    value: function getLineCount(option) {
      return (0, _predicate.isObject)(option) ? option.lineCount : undefined;
    }
  }, {
    key: "calculateHeightForLineCount",
    value: function calculateHeightForLineCount(cellRef, lineCount, excludePadding) {
      var computedStyles = window.getComputedStyle(cellRef, null);
      var lineHeight = parseInt(computedStyles.lineHeight, 10);
      var contentHeight = Math.ceil(lineCount * lineHeight);
      return excludePadding ? contentHeight : contentHeight + this.styles.paddingTop + this.styles.paddingBottom;
    }
    /**
     * Auto height utils
     */

  }, {
    key: "isAutoHeight",
    value: function isAutoHeight(rowIndex, rowHeightsOptions) {
      var height = this.getRowHeightOption(rowIndex, rowHeightsOptions);

      if (height === AUTO_HEIGHT) {
        return true;
      }

      return false;
    }
  }, {
    key: "getRowHeight",
    value: function getRowHeight(rowIndex) {
      var rowHeights = this.heightsCache.get(rowIndex);
      if (rowHeights == null) return 0;
      var rowHeightValues = Array.from(rowHeights.values());
      if (!rowHeightValues.length) return 0;
      return Math.max.apply(Math, (0, _toConsumableArray2.default)(rowHeightValues));
    }
  }, {
    key: "setRowHeight",
    value: function setRowHeight(rowIndex, colId) {
      var height = arguments.length > 2 && arguments[2] !== undefined ? arguments[2] : DEFAULT_ROW_HEIGHT;
      var visibleRowIndex = arguments.length > 3 ? arguments[3] : undefined;
      var rowHeights = this.heightsCache.get(rowIndex) || new Map();
      var adaptedHeight = Math.ceil(height + this.styles.paddingTop + this.styles.paddingBottom);

      if (rowHeights.get(colId) === adaptedHeight) {
        return;
      }

      rowHeights.set(colId, adaptedHeight);
      this.heightsCache.set(rowIndex, rowHeights);
      this.resetRow(visibleRowIndex);
      this.rerenderGridBody();
    }
  }, {
    key: "pruneHiddenColumnHeights",
    value: function pruneHiddenColumnHeights(visibleColumns) {
      var visibleColumnIds = new Set(visibleColumns.map(function (_ref) {
        var id = _ref.id;
        return id;
      }));
      var didModify = false;
      this.heightsCache.forEach(function (rowHeights) {
        var existingColumnIds = Array.from(rowHeights.keys());
        existingColumnIds.forEach(function (existingColumnId) {
          if (visibleColumnIds.has(existingColumnId) === false) {
            didModify = true;
            rowHeights.delete(existingColumnId);
          }
        });
      });

      if (didModify) {
        this.resetRow(0);
      }
    }
  }, {
    key: "resetRow",
    value: function resetRow(visibleRowIndex) {
      var _this2 = this;

      // save the first row index of batch, reassigning it only
      // if this visible row index less than lastUpdatedRow
      this.lastUpdatedRow = Math.min(this.lastUpdatedRow, visibleRowIndex);
      clearTimeout(this.timerId);
      this.timerId = window.setTimeout(function () {
        return _this2.resetGrid();
      }, 0);
    }
  }, {
    key: "resetGrid",
    value: function resetGrid() {
      var _this$grid;

      (_this$grid = this.grid) === null || _this$grid === void 0 ? void 0 : _this$grid.resetAfterRowIndex(this.lastUpdatedRow);
      this.lastUpdatedRow = Infinity;
    }
  }, {
    key: "setGrid",
    value: function setGrid(grid) {
      this.grid = grid;
    }
  }, {
    key: "setRerenderGridBody",
    value: function setRerenderGridBody(rerenderGridBody) {
      this.rerenderGridBody = rerenderGridBody;
    }
  }]);
  return RowHeightUtils;
}();
/**
 * Hook for instantiating RowHeightUtils, and also updating
 * internal vars from outside props via useEffects
 */


exports.RowHeightUtils = RowHeightUtils;

var useRowHeightUtils = function useRowHeightUtils(_ref2) {
  var gridRef = _ref2.gridRef,
      gridStyles = _ref2.gridStyles,
      columns = _ref2.columns;
  var rowHeightUtils = (0, _react.useMemo)(function () {
    return new RowHeightUtils();
  }, []); // Update rowHeightUtils with grid ref

  (0, _react.useEffect)(function () {
    if (gridRef) rowHeightUtils.setGrid(gridRef);
  }, [gridRef, rowHeightUtils]); // Re-cache styles whenever grid density changes

  (0, _react.useEffect)(function () {
    rowHeightUtils.cacheStyles({
      cellPadding: gridStyles.cellPadding
    });
  }, [gridStyles.cellPadding, rowHeightUtils]); // Update row heights map to remove hidden columns whenever orderedVisibleColumns change

  (0, _react.useEffect)(function () {
    rowHeightUtils.pruneHiddenColumnHeights(columns);
  }, [rowHeightUtils, columns]);
  return rowHeightUtils;
};

exports.useRowHeightUtils = useRowHeightUtils;

var useDefaultRowHeight = function useDefaultRowHeight(_ref3) {
  var rowHeightsOptions = _ref3.rowHeightsOptions,
      rowHeightUtils = _ref3.rowHeightUtils;

  var _useContext = (0, _react.useContext)(_sorting.DataGridSortingContext),
      getCorrectRowIndex = _useContext.getCorrectRowIndex; // `minRowHeight` is primarily used by undefined & lineCount heights
  // and ignored by auto & static heights (unless the static height is < the min)


  var _useState = (0, _react.useState)(DEFAULT_ROW_HEIGHT),
      _useState2 = (0, _slicedToArray2.default)(_useState, 2),
      minRowHeight = _useState2[0],
      setRowHeight = _useState2[1]; // Default/fallback height for all rows


  var defaultRowHeight = (0, _react.useMemo)(function () {
    return (rowHeightsOptions === null || rowHeightsOptions === void 0 ? void 0 : rowHeightsOptions.defaultHeight) ? rowHeightUtils.getCalculatedHeight(rowHeightsOptions.defaultHeight, minRowHeight) : minRowHeight;
  }, [rowHeightsOptions, minRowHeight, rowHeightUtils]); // Used by react-window's Grid component to determine actual row heights

  var getRowHeight = (0, _react.useCallback)(function (rowIndex) {
    var correctRowIndex = getCorrectRowIndex(rowIndex);
    var rowHeight; // Account for row-specific height overrides

    var rowHeightOption = rowHeightUtils.getRowHeightOption(correctRowIndex, rowHeightsOptions);

    if (rowHeightOption) {
      rowHeight = rowHeightUtils.getCalculatedHeight(rowHeightOption, minRowHeight, correctRowIndex, rowHeightUtils.isRowHeightOverride(correctRowIndex, rowHeightsOptions));
    } // Use the row-specific height if it exists, if not, fall back to the default


    return rowHeight || defaultRowHeight;
  }, [minRowHeight, rowHeightsOptions, getCorrectRowIndex, rowHeightUtils, defaultRowHeight]);
  return {
    defaultRowHeight: defaultRowHeight,
    setRowHeight: setRowHeight,
    getRowHeight: getRowHeight
  };
};

exports.useDefaultRowHeight = useDefaultRowHeight;