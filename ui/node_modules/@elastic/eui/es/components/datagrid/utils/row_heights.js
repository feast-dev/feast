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

function _classCallCheck(instance, Constructor) { if (!(instance instanceof Constructor)) { throw new TypeError("Cannot call a class as a function"); } }

function _defineProperties(target, props) { for (var i = 0; i < props.length; i++) { var descriptor = props[i]; descriptor.enumerable = descriptor.enumerable || false; descriptor.configurable = true; if ("value" in descriptor) descriptor.writable = true; Object.defineProperty(target, descriptor.key, descriptor); } }

function _createClass(Constructor, protoProps, staticProps) { if (protoProps) _defineProperties(Constructor.prototype, protoProps); if (staticProps) _defineProperties(Constructor, staticProps); return Constructor; }

function _defineProperty(obj, key, value) { if (key in obj) { Object.defineProperty(obj, key, { value: value, enumerable: true, configurable: true, writable: true }); } else { obj[key] = value; } return obj; }

/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */
import { useEffect, useState, useMemo, useCallback, useContext } from 'react';
import { isObject, isNumber } from '../../../services/predicate';
import { DataGridSortingContext } from './sorting'; // TODO: Once JS variables are available, use them here instead of hard-coded maps

export var cellPaddingsMap = {
  s: 4,
  m: 6,
  l: 8
};
export var AUTO_HEIGHT = 'auto';
export var DEFAULT_ROW_HEIGHT = 34;
export var RowHeightUtils = /*#__PURE__*/function () {
  function RowHeightUtils() {
    var _this = this;

    _classCallCheck(this, RowHeightUtils);

    _defineProperty(this, "styles", {
      paddingTop: 0,
      paddingBottom: 0
    });

    _defineProperty(this, "getStylesForCell", function (rowHeightsOptions, rowIndex) {
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

    _defineProperty(this, "heightsCache", new Map());

    _defineProperty(this, "timerId", void 0);

    _defineProperty(this, "grid", void 0);

    _defineProperty(this, "lastUpdatedRow", Infinity);

    _defineProperty(this, "rerenderGridBody", function () {});
  }

  _createClass(RowHeightUtils, [{
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
      if (isObject(heightOption) && heightOption.height) {
        return Math.max(heightOption.height, defaultHeight);
      }

      if (heightOption && isNumber(heightOption)) {
        return Math.max(heightOption, defaultHeight);
      }

      if (isObject(heightOption) && heightOption.lineCount) {
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
      return isObject(option) ? option.lineCount : undefined;
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
      return Math.max.apply(Math, _toConsumableArray(rowHeightValues));
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

export var useRowHeightUtils = function useRowHeightUtils(_ref2) {
  var gridRef = _ref2.gridRef,
      gridStyles = _ref2.gridStyles,
      columns = _ref2.columns;
  var rowHeightUtils = useMemo(function () {
    return new RowHeightUtils();
  }, []); // Update rowHeightUtils with grid ref

  useEffect(function () {
    if (gridRef) rowHeightUtils.setGrid(gridRef);
  }, [gridRef, rowHeightUtils]); // Re-cache styles whenever grid density changes

  useEffect(function () {
    rowHeightUtils.cacheStyles({
      cellPadding: gridStyles.cellPadding
    });
  }, [gridStyles.cellPadding, rowHeightUtils]); // Update row heights map to remove hidden columns whenever orderedVisibleColumns change

  useEffect(function () {
    rowHeightUtils.pruneHiddenColumnHeights(columns);
  }, [rowHeightUtils, columns]);
  return rowHeightUtils;
};
export var useDefaultRowHeight = function useDefaultRowHeight(_ref3) {
  var rowHeightsOptions = _ref3.rowHeightsOptions,
      rowHeightUtils = _ref3.rowHeightUtils;

  var _useContext = useContext(DataGridSortingContext),
      getCorrectRowIndex = _useContext.getCorrectRowIndex; // `minRowHeight` is primarily used by undefined & lineCount heights
  // and ignored by auto & static heights (unless the static height is < the min)


  var _useState = useState(DEFAULT_ROW_HEIGHT),
      _useState2 = _slicedToArray(_useState, 2),
      minRowHeight = _useState2[0],
      setRowHeight = _useState2[1]; // Default/fallback height for all rows


  var defaultRowHeight = useMemo(function () {
    return (rowHeightsOptions === null || rowHeightsOptions === void 0 ? void 0 : rowHeightsOptions.defaultHeight) ? rowHeightUtils.getCalculatedHeight(rowHeightsOptions.defaultHeight, minRowHeight) : minRowHeight;
  }, [rowHeightsOptions, minRowHeight, rowHeightUtils]); // Used by react-window's Grid component to determine actual row heights

  var getRowHeight = useCallback(function (rowIndex) {
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