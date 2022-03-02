function _typeof(obj) { "@babel/helpers - typeof"; if (typeof Symbol === "function" && typeof Symbol.iterator === "symbol") { _typeof = function _typeof(obj) { return typeof obj; }; } else { _typeof = function _typeof(obj) { return obj && typeof Symbol === "function" && obj.constructor === Symbol && obj !== Symbol.prototype ? "symbol" : typeof obj; }; } return _typeof(obj); }

function _extends() { _extends = Object.assign || function (target) { for (var i = 1; i < arguments.length; i++) { var source = arguments[i]; for (var key in source) { if (Object.prototype.hasOwnProperty.call(source, key)) { target[key] = source[key]; } } } return target; }; return _extends.apply(this, arguments); }

function _objectWithoutProperties(source, excluded) { if (source == null) return {}; var target = _objectWithoutPropertiesLoose(source, excluded); var key, i; if (Object.getOwnPropertySymbols) { var sourceSymbolKeys = Object.getOwnPropertySymbols(source); for (i = 0; i < sourceSymbolKeys.length; i++) { key = sourceSymbolKeys[i]; if (excluded.indexOf(key) >= 0) continue; if (!Object.prototype.propertyIsEnumerable.call(source, key)) continue; target[key] = source[key]; } } return target; }

function _objectWithoutPropertiesLoose(source, excluded) { if (source == null) return {}; var target = {}; var sourceKeys = Object.keys(source); var key, i; for (i = 0; i < sourceKeys.length; i++) { key = sourceKeys[i]; if (excluded.indexOf(key) >= 0) continue; target[key] = source[key]; } return target; }

function ownKeys(object, enumerableOnly) { var keys = Object.keys(object); if (Object.getOwnPropertySymbols) { var symbols = Object.getOwnPropertySymbols(object); if (enumerableOnly) symbols = symbols.filter(function (sym) { return Object.getOwnPropertyDescriptor(object, sym).enumerable; }); keys.push.apply(keys, symbols); } return keys; }

function _objectSpread(target) { for (var i = 1; i < arguments.length; i++) { var source = arguments[i] != null ? arguments[i] : {}; if (i % 2) { ownKeys(Object(source), true).forEach(function (key) { _defineProperty(target, key, source[key]); }); } else if (Object.getOwnPropertyDescriptors) { Object.defineProperties(target, Object.getOwnPropertyDescriptors(source)); } else { ownKeys(Object(source)).forEach(function (key) { Object.defineProperty(target, key, Object.getOwnPropertyDescriptor(source, key)); }); } } return target; }

function _classCallCheck(instance, Constructor) { if (!(instance instanceof Constructor)) { throw new TypeError("Cannot call a class as a function"); } }

function _defineProperties(target, props) { for (var i = 0; i < props.length; i++) { var descriptor = props[i]; descriptor.enumerable = descriptor.enumerable || false; descriptor.configurable = true; if ("value" in descriptor) descriptor.writable = true; Object.defineProperty(target, descriptor.key, descriptor); } }

function _createClass(Constructor, protoProps, staticProps) { if (protoProps) _defineProperties(Constructor.prototype, protoProps); if (staticProps) _defineProperties(Constructor, staticProps); return Constructor; }

function _inherits(subClass, superClass) { if (typeof superClass !== "function" && superClass !== null) { throw new TypeError("Super expression must either be null or a function"); } subClass.prototype = Object.create(superClass && superClass.prototype, { constructor: { value: subClass, writable: true, configurable: true } }); if (superClass) _setPrototypeOf(subClass, superClass); }

function _setPrototypeOf(o, p) { _setPrototypeOf = Object.setPrototypeOf || function _setPrototypeOf(o, p) { o.__proto__ = p; return o; }; return _setPrototypeOf(o, p); }

function _createSuper(Derived) { var hasNativeReflectConstruct = _isNativeReflectConstruct(); return function _createSuperInternal() { var Super = _getPrototypeOf(Derived), result; if (hasNativeReflectConstruct) { var NewTarget = _getPrototypeOf(this).constructor; result = Reflect.construct(Super, arguments, NewTarget); } else { result = Super.apply(this, arguments); } return _possibleConstructorReturn(this, result); }; }

function _possibleConstructorReturn(self, call) { if (call && (_typeof(call) === "object" || typeof call === "function")) { return call; } return _assertThisInitialized(self); }

function _assertThisInitialized(self) { if (self === void 0) { throw new ReferenceError("this hasn't been initialised - super() hasn't been called"); } return self; }

function _isNativeReflectConstruct() { if (typeof Reflect === "undefined" || !Reflect.construct) return false; if (Reflect.construct.sham) return false; if (typeof Proxy === "function") return true; try { Date.prototype.toString.call(Reflect.construct(Date, [], function () {})); return true; } catch (e) { return false; } }

function _getPrototypeOf(o) { _getPrototypeOf = Object.setPrototypeOf ? Object.getPrototypeOf : function _getPrototypeOf(o) { return o.__proto__ || Object.getPrototypeOf(o); }; return _getPrototypeOf(o); }

function _defineProperty(obj, key, value) { if (key in obj) { Object.defineProperty(obj, key, { value: value, enumerable: true, configurable: true, writable: true }); } else { obj[key] = value; } return obj; }

/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */
import React, { Component } from 'react';
import PropTypes from "prop-types";
import { EuiBasicTable } from './basic_table';
import { defaults as paginationBarDefaults } from './pagination_bar';
import { isString } from '../../services/predicate';
import { Comparators } from '../../services/sort';
import { EuiSearchBar } from '../search_bar';
import { EuiSpacer } from '../spacer';
import { jsx as ___EmotionJSX } from "@emotion/react";

function isEuiSearchBarProps(x) {
  return typeof x !== 'boolean';
}

var getQueryFromSearch = function getQueryFromSearch(search, defaultQuery) {
  var query;

  if (!search) {
    query = '';
  } else {
    query = (defaultQuery ? search.defaultQuery || search.query || '' : search.query) || '';
  }

  return isString(query) ? EuiSearchBar.Query.parse(query) : query;
};

var getInitialPagination = function getInitialPagination(pagination) {
  if (!pagination) {
    return {
      pageIndex: undefined,
      pageSize: undefined
    };
  }

  var _ref = pagination,
      _ref$pageSizeOptions = _ref.pageSizeOptions,
      pageSizeOptions = _ref$pageSizeOptions === void 0 ? paginationBarDefaults.pageSizeOptions : _ref$pageSizeOptions,
      hidePerPageOptions = _ref.hidePerPageOptions;
  var defaultPageSize = pageSizeOptions ? pageSizeOptions[0] : paginationBarDefaults.pageSizeOptions[0];
  var initialPageIndex = pagination === true ? 0 : pagination.pageIndex || pagination.initialPageIndex || 0;
  var initialPageSize = pagination === true ? defaultPageSize : pagination.pageSize || pagination.initialPageSize || defaultPageSize;

  if (!hidePerPageOptions && initialPageSize && (!pageSizeOptions || !pageSizeOptions.includes(initialPageSize))) {
    throw new Error("EuiInMemoryTable received initialPageSize ".concat(initialPageSize, ", which wasn't provided within pageSizeOptions."));
  }

  return {
    pageIndex: initialPageIndex,
    pageSize: initialPageSize,
    pageSizeOptions: pageSizeOptions,
    hidePerPageOptions: hidePerPageOptions
  };
};

function findColumnByProp(columns, prop, value) {
  for (var i = 0; i < columns.length; i++) {
    var column = columns[i];

    if (column[prop] === value) {
      return column;
    }
  }
}

function getInitialSorting(columns, sorting) {
  if (!sorting || !sorting.sort) {
    return {
      sortName: undefined,
      sortDirection: undefined
    };
  }

  var _sort = sorting.sort,
      sortable = _sort.field,
      sortDirection = _sort.direction; // sortable could be a column's `field` or its `name`
  // for backwards compatibility `field` must be checked first

  var sortColumn = findColumnByProp(columns, 'field', sortable);

  if (sortColumn == null) {
    sortColumn = findColumnByProp(columns, 'name', sortable);
  }

  if (sortColumn == null) {
    return {
      sortName: undefined,
      sortDirection: undefined
    };
  }

  var sortName = sortColumn.name;
  return {
    sortName: sortName,
    sortDirection: sortDirection
  };
}

export var EuiInMemoryTable = /*#__PURE__*/function (_Component) {
  _inherits(EuiInMemoryTable, _Component);

  var _super = _createSuper(EuiInMemoryTable);

  _createClass(EuiInMemoryTable, null, [{
    key: "getDerivedStateFromProps",
    value: function getDerivedStateFromProps(nextProps, prevState) {
      var updatedPrevState = prevState;

      if (nextProps.items !== prevState.prevProps.items) {
        // We have new items because an external search has completed, so reset pagination state.
        var nextPageIndex = 0;

        if (nextProps.pagination != null && typeof nextProps.pagination !== 'boolean') {
          nextPageIndex = nextProps.pagination.pageIndex || 0;
        }

        updatedPrevState = _objectSpread(_objectSpread({}, updatedPrevState), {}, {
          prevProps: _objectSpread(_objectSpread({}, updatedPrevState.prevProps), {}, {
            items: nextProps.items
          }),
          pageIndex: nextPageIndex
        });
      } // apply changes to controlled pagination


      if (nextProps.pagination != null && typeof nextProps.pagination !== 'boolean') {
        if (nextProps.pagination.pageSize != null && nextProps.pagination.pageSize !== updatedPrevState.pageIndex) {
          updatedPrevState = _objectSpread(_objectSpread({}, updatedPrevState), {}, {
            pageSize: nextProps.pagination.pageSize
          });
        }

        if (nextProps.pagination.pageIndex != null && nextProps.pagination.pageIndex !== updatedPrevState.pageIndex) {
          updatedPrevState = _objectSpread(_objectSpread({}, updatedPrevState), {}, {
            pageIndex: nextProps.pagination.pageIndex
          });
        }
      }

      var _getInitialSorting = getInitialSorting(nextProps.columns, nextProps.sorting),
          sortName = _getInitialSorting.sortName,
          sortDirection = _getInitialSorting.sortDirection;

      if (sortName !== prevState.prevProps.sortName || sortDirection !== prevState.prevProps.sortDirection) {
        updatedPrevState = _objectSpread(_objectSpread({}, updatedPrevState), {}, {
          sortName: sortName,
          sortDirection: sortDirection
        });
      }

      var nextQuery = nextProps.search ? nextProps.search.query : '';
      var prevQuery = prevState.prevProps.search ? prevState.prevProps.search.query : '';

      if (nextQuery !== prevQuery) {
        updatedPrevState = _objectSpread(_objectSpread({}, updatedPrevState), {}, {
          prevProps: _objectSpread(_objectSpread({}, updatedPrevState.prevProps), {}, {
            search: nextProps.search
          }),
          query: getQueryFromSearch(nextProps.search, false)
        });
      }

      if (updatedPrevState !== prevState) {
        return updatedPrevState;
      }

      return null;
    }
  }]);

  function EuiInMemoryTable(props) {
    var _this;

    _classCallCheck(this, EuiInMemoryTable);

    _this = _super.call(this, props);

    _defineProperty(_assertThisInitialized(_this), "tableRef", void 0);

    _defineProperty(_assertThisInitialized(_this), "onTableChange", function (_ref2) {
      var page = _ref2.page,
          sort = _ref2.sort;

      var _ref3 = page || {},
          pageIndex = _ref3.index,
          pageSize = _ref3.size; // don't apply pagination changes that are otherwise controlled
      // `page` is left unchanged as it goes to the consumer's `onTableChange` callback, allowing the app to respond


      var pagination = _this.props.pagination;

      if (pagination != null && typeof pagination !== 'boolean') {
        if (pagination.pageSize != null) pageSize = pagination.pageSize;
        if (pagination.pageIndex != null) pageIndex = pagination.pageIndex;
      }

      var _ref4 = sort || {},
          sortName = _ref4.field,
          sortDirection = _ref4.direction; // To keep backwards compatibility reportedSortName needs to be tracked separately
      // from sortName; sortName gets stored internally while reportedSortName is sent to the callback


      var reportedSortName = sortName; // EuiBasicTable returns the column's `field` if it exists instead of `name`,
      // map back to `name` if this is the case

      for (var i = 0; i < _this.props.columns.length; i++) {
        var column = _this.props.columns[i];

        if ('field' in column && column.field === sortName) {
          sortName = column.name;
          break;
        }
      } // Allow going back to 'neutral' sorting


      if (_this.state.allowNeutralSort && _this.state.sortName === sortName && _this.state.sortDirection === 'desc' && sortDirection === 'asc') {
        sortName = '';
        reportedSortName = '';
        sortDirection = 'asc'; // Default sort direction.
      }

      if (_this.props.onTableChange) {
        _this.props.onTableChange({
          // @ts-ignore complex relationship between pagination's existence and criteria, the code logic ensures this is correctly maintained
          page: page,
          sort: {
            field: reportedSortName,
            direction: sortDirection
          }
        });
      }

      _this.setState({
        pageIndex: pageIndex,
        pageSize: pageSize,
        sortName: sortName,
        sortDirection: sortDirection
      });
    });

    _defineProperty(_assertThisInitialized(_this), "onQueryChange", function (_ref5) {
      var query = _ref5.query,
          queryText = _ref5.queryText,
          error = _ref5.error;
      var search = _this.props.search;

      if (isEuiSearchBarProps(search)) {
        if (search.onChange) {
          var shouldQueryInMemory = error == null ? search.onChange({
            query: query,
            queryText: queryText,
            error: null
          }) : search.onChange({
            query: null,
            queryText: queryText,
            error: error
          });

          if (!shouldQueryInMemory) {
            return;
          }
        }
      } // Reset pagination state.


      _this.setState(function (state) {
        return {
          prevProps: _objectSpread(_objectSpread({}, state.prevProps), {}, {
            search: search
          }),
          query: query,
          pageIndex: 0
        };
      });
    });

    var columns = props.columns,
        _search = props.search,
        _pagination = props.pagination,
        sorting = props.sorting,
        allowNeutralSort = props.allowNeutralSort;

    var _getInitialPagination = getInitialPagination(_pagination),
        _pageIndex = _getInitialPagination.pageIndex,
        _pageSize = _getInitialPagination.pageSize,
        pageSizeOptions = _getInitialPagination.pageSizeOptions,
        hidePerPageOptions = _getInitialPagination.hidePerPageOptions;

    var _getInitialSorting2 = getInitialSorting(columns, sorting),
        _sortName = _getInitialSorting2.sortName,
        _sortDirection = _getInitialSorting2.sortDirection;

    _this.state = {
      prevProps: {
        items: props.items,
        sortName: _sortName,
        sortDirection: _sortDirection,
        search: _search
      },
      search: _search,
      query: getQueryFromSearch(_search, true),
      pageIndex: _pageIndex || 0,
      pageSize: _pageSize,
      pageSizeOptions: pageSizeOptions,
      sortName: _sortName,
      sortDirection: _sortDirection,
      allowNeutralSort: allowNeutralSort !== false,
      hidePerPageOptions: hidePerPageOptions
    };
    _this.tableRef = /*#__PURE__*/React.createRef();
    return _this;
  }

  _createClass(EuiInMemoryTable, [{
    key: "setSelection",
    value: function setSelection(newSelection) {
      if (this.tableRef.current) {
        this.tableRef.current.setSelection(newSelection);
      }
    }
  }, {
    key: "renderSearchBar",
    value: function renderSearchBar() {
      var search = this.props.search;

      if (search) {
        var searchBarProps = {};

        if (isEuiSearchBarProps(search)) {
          var onChange = search.onChange,
              _searchBarProps = _objectWithoutProperties(search, ["onChange"]);

          searchBarProps = _searchBarProps;

          if (searchBarProps.box && searchBarProps.box.schema === true) {
            searchBarProps.box = _objectSpread(_objectSpread({}, searchBarProps.box), {}, {
              schema: this.resolveSearchSchema()
            });
          }
        }

        return ___EmotionJSX(React.Fragment, null, ___EmotionJSX(EuiSearchBar, _extends({
          onChange: this.onQueryChange
        }, searchBarProps)), ___EmotionJSX(EuiSpacer, {
          size: "l"
        }));
      }
    }
  }, {
    key: "resolveSearchSchema",
    value: function resolveSearchSchema() {
      var columns = this.props.columns;
      return columns.reduce(function (schema, column) {
        var _ref6 = column,
            field = _ref6.field,
            dataType = _ref6.dataType;

        if (field) {
          var type = dataType || 'string';
          schema.fields[field] = {
            type: type
          };
        }

        return schema;
      }, {
        strict: true,
        fields: {}
      });
    }
  }, {
    key: "getItemSorter",
    value: function getItemSorter() {
      var _this$state = this.state,
          sortName = _this$state.sortName,
          sortDirection = _this$state.sortDirection;
      var columns = this.props.columns;
      var sortColumn = columns.find(function (_ref7) {
        var name = _ref7.name;
        return name === sortName;
      });

      if (sortColumn == null) {
        // can't return a non-function so return a function that says everything is the same
        return function () {
          return 0;
        };
      }

      var sortable = sortColumn.sortable;

      if (typeof sortable === 'function') {
        return Comparators.value(sortable, Comparators.default(sortDirection));
      }

      return Comparators.property(sortColumn.field, Comparators.default(sortDirection));
    }
  }, {
    key: "getItems",
    value: function getItems() {
      var executeQueryOptions = this.props.executeQueryOptions;
      var items = this.state.prevProps.items;

      if (!items.length) {
        return {
          items: [],
          totalItemCount: 0
        };
      }

      var _this$state2 = this.state,
          query = _this$state2.query,
          sortName = _this$state2.sortName,
          pageIndex = _this$state2.pageIndex,
          pageSize = _this$state2.pageSize;
      var matchingItems = query ? EuiSearchBar.Query.execute(query, items, executeQueryOptions) : items;
      var sortedItems = sortName ? matchingItems.slice(0) // avoid mutating the source array
      .sort(this.getItemSorter()) // sort, causes mutation
      : matchingItems;
      var visibleItems = pageSize && this.props.pagination ? function () {
        var startIndex = pageIndex * pageSize;
        return sortedItems.slice(startIndex, Math.min(startIndex + pageSize, sortedItems.length));
      }() : sortedItems;
      return {
        items: visibleItems,
        totalItemCount: matchingItems.length
      };
    }
  }, {
    key: "render",
    value: function render() {
      var _this$props = this.props,
          columns = _this$props.columns,
          loading = _this$props.loading,
          message = _this$props.message,
          error = _this$props.error,
          selection = _this$props.selection,
          isSelectable = _this$props.isSelectable,
          hasActions = _this$props.hasActions,
          compressed = _this$props.compressed,
          hasPagination = _this$props.pagination,
          hasSorting = _this$props.sorting,
          itemIdToExpandedRowMap = _this$props.itemIdToExpandedRowMap,
          itemId = _this$props.itemId,
          rowProps = _this$props.rowProps,
          cellProps = _this$props.cellProps,
          tableLayout = _this$props.tableLayout,
          _unuseditems = _this$props.items,
          search = _this$props.search,
          onTableChange = _this$props.onTableChange,
          executeQueryOptions = _this$props.executeQueryOptions,
          allowNeutralSort = _this$props.allowNeutralSort,
          childrenBetween = _this$props.childrenBetween,
          rest = _objectWithoutProperties(_this$props, ["columns", "loading", "message", "error", "selection", "isSelectable", "hasActions", "compressed", "pagination", "sorting", "itemIdToExpandedRowMap", "itemId", "rowProps", "cellProps", "tableLayout", "items", "search", "onTableChange", "executeQueryOptions", "allowNeutralSort", "childrenBetween"]);

      var _this$state3 = this.state,
          pageIndex = _this$state3.pageIndex,
          pageSize = _this$state3.pageSize,
          pageSizeOptions = _this$state3.pageSizeOptions,
          sortName = _this$state3.sortName,
          sortDirection = _this$state3.sortDirection,
          hidePerPageOptions = _this$state3.hidePerPageOptions;

      var _this$getItems = this.getItems(),
          items = _this$getItems.items,
          totalItemCount = _this$getItems.totalItemCount;

      var pagination = !hasPagination ? undefined : {
        pageIndex: pageIndex,
        pageSize: pageSize || 1,
        pageSizeOptions: pageSizeOptions,
        totalItemCount: totalItemCount,
        hidePerPageOptions: hidePerPageOptions
      }; // Data loaded from a server can have a default sort order which is meaningful to the
      // user, but can't be reproduced with client-side sort logic. So we allow the table to display
      // rows in the order in which they're initially loaded by providing an undefined sorting prop.

      var sorting = !hasSorting ? undefined : {
        sort: !sortName && !sortDirection ? undefined : {
          field: sortName,
          direction: sortDirection
        },
        allowNeutralSort: this.state.allowNeutralSort
      };
      var searchBar = this.renderSearchBar();

      var table = // @ts-ignore complex relationship between pagination's existence and criteria, the code logic ensures this is correctly maintained
      ___EmotionJSX(EuiBasicTable, _extends({
        ref: this.tableRef,
        items: items,
        itemId: itemId,
        rowProps: rowProps,
        cellProps: cellProps,
        columns: columns,
        pagination: pagination,
        sorting: sorting,
        selection: selection,
        isSelectable: isSelectable,
        hasActions: hasActions,
        onChange: this.onTableChange,
        error: error,
        loading: loading,
        noItemsMessage: message,
        tableLayout: tableLayout,
        compressed: compressed,
        itemIdToExpandedRowMap: itemIdToExpandedRowMap
      }, rest));

      if (!searchBar) {
        return table;
      }

      return ___EmotionJSX("div", null, searchBar, childrenBetween, table);
    }
  }]);

  return EuiInMemoryTable;
}(Component);

_defineProperty(EuiInMemoryTable, "defaultProps", {
  responsive: true,
  tableLayout: 'fixed'
});

EuiInMemoryTable.propTypes = {
  className: PropTypes.string,
  "aria-label": PropTypes.string,
  "data-test-subj": PropTypes.string,

  /**
     * Describes how to extract a unique ID from each item, used for selections & expanded rows
     */

  /**
     * Describes how to extract a unique ID from each item, used for selections & expanded rows
     */
  itemId: PropTypes.oneOfType([PropTypes.string.isRequired, PropTypes.number.isRequired, PropTypes.func.isRequired]),

  /**
     * Row expansion uses the itemId prop to identify each row
     */

  /**
     * Row expansion uses the itemId prop to identify each row
     */
  itemIdToExpandedRowMap: PropTypes.shape({}),

  /**
     * A list of objects to who in the table - an item per row
     */

  /**
     * A list of objects to who in the table - an item per row
     */
  items: PropTypes.arrayOf(PropTypes.any.isRequired),

  /**
     * Applied to `EuiTableRowCell`
     */

  /**
     * Applied to `EuiTableRowCell`
     */
  cellProps: PropTypes.oneOfType([PropTypes.any.isRequired, PropTypes.func.isRequired]),

  /**
     * An array of one of the objects: #EuiTableFieldDataColumnType, #EuiTableComputedColumnType or #EuiTableActionsColumnType.
     */

  /**
     * An array of one of the objects: #EuiTableFieldDataColumnType, #EuiTableComputedColumnType or #EuiTableActionsColumnType.
     */
  columns: PropTypes.arrayOf(PropTypes.oneOfType([PropTypes.shape({
    /**
       * A field of the item (may be a nested field)
       */
    // type hack used for better autocomplete support
    // https://github.com/microsoft/TypeScript/issues/29729
    field: PropTypes.oneOfType([PropTypes.any.isRequired, PropTypes.any.isRequired]).isRequired,
    // supports outer.inner key paths

    /**
       * The display name of the column
       */
    name: PropTypes.node.isRequired,

    /**
       * A description of the column (will be presented as a title over the column header)
       */
    description: PropTypes.string,

    /**
       * Describes the data types of the displayed value (serves as a rendering hint for the table)
       */
    dataType: PropTypes.oneOf(["auto", "string", "number", "boolean", "date"]),

    /**
       * A CSS width property. Hints for the required width of the column (e.g. "30%", "100px", etc..)
       */
    width: PropTypes.string,

    /**
       * Defines whether the user can sort on this column. If a function is provided, this function returns the value to sort against
       */
    sortable: PropTypes.oneOfType([PropTypes.bool.isRequired, PropTypes.func.isRequired]),
    isExpander: PropTypes.bool,
    textOnly: PropTypes.bool,

    /**
       * Defines the horizontal alignment of the column
       */
    align: PropTypes.oneOf(["left", "right", "center"]),

    /**
       * Indicates whether this column should truncate its content when it doesn't fit
       */
    truncateText: PropTypes.bool,
    mobileOptions: PropTypes.shape({
      render: PropTypes.func
    }),

    /**
       * Describe a custom renderer function for the content
       */
    render: PropTypes.func,

    /**
       * Content to display in the footer beneath this column
       */
    footer: PropTypes.oneOfType([PropTypes.string.isRequired, PropTypes.element.isRequired, PropTypes.func.isRequired]),

    /**
       * Disables the user's ability to change the sort but still shows the current direction
       */
    readOnly: PropTypes.bool,
    className: PropTypes.string,
    "aria-label": PropTypes.string,
    "data-test-subj": PropTypes.string
  }).isRequired, PropTypes.shape({
    /**
       * A function that computes the value for each item and renders it
       */
    render: PropTypes.func.isRequired,

    /**
       * The display name of the column
       */
    name: PropTypes.node,

    /**
       * A description of the column (will be presented as a title over the column header
       */
    description: PropTypes.string,

    /**
       * If provided, allows this column to be sorted on. Must return the value to sort against.
       */
    sortable: PropTypes.func,

    /**
       * A CSS width property. Hints for the required width of the column
       */
    width: PropTypes.string,

    /**
       * Indicates whether this column should truncate its content when it doesn't fit
       */
    truncateText: PropTypes.bool,
    isExpander: PropTypes.bool,
    align: PropTypes.oneOf(["left", "right", "center"]),

    /**
       * Disables the user's ability to change the sort but still shows the current direction
       */
    readOnly: PropTypes.bool,
    className: PropTypes.string,
    "aria-label": PropTypes.string,
    "data-test-subj": PropTypes.string
  }).isRequired, PropTypes.shape({
    /**
       * An array of one of the objects: #DefaultItemAction or #CustomItemAction
       */
    actions: PropTypes.arrayOf(PropTypes.oneOfType([PropTypes.shape({
      /**
         * The type of action
         */
      type: PropTypes.oneOfType([PropTypes.oneOf(["button"]), PropTypes.oneOf(["icon"]).isRequired]),

      /**
         * Defines the color of the button
         */
      color: PropTypes.oneOfType([PropTypes.oneOfType([PropTypes.oneOf(["primary", "danger", "text", "ghost", "success", "warning"]).isRequired, PropTypes.func.isRequired]), PropTypes.oneOfType([PropTypes.oneOf(["primary", "accent", "success", "warning", "danger", "ghost", "text"]).isRequired, PropTypes.func.isRequired])]),

      /**
         * The display name of the action (will be the button caption)
         */

      /**
         * The display name of the action (will be the button caption)
         */
      name: PropTypes.oneOfType([PropTypes.node.isRequired, PropTypes.func.isRequired]).isRequired,

      /**
         * Describes the action (will be the button title)
         */

      /**
         * Describes the action (will be the button title)
         */
      description: PropTypes.string.isRequired,

      /**
         * A handler function to execute the action
         */

      /**
         * A handler function to execute the action
         */
      onClick: PropTypes.func,
      href: PropTypes.string,
      target: PropTypes.string,

      /**
         * A callback function that determines whether the action is available
         */

      /**
         * A callback function that determines whether the action is available
         */
      available: PropTypes.func,

      /**
         * A callback function that determines whether the action is enabled
         */

      /**
         * A callback function that determines whether the action is enabled
         */
      enabled: PropTypes.func,
      isPrimary: PropTypes.bool,
      "data-test-subj": PropTypes.string,

      /**
         * Associates an icon with the button
         */
      icon: PropTypes.oneOfType([PropTypes.oneOf(["accessibility", "addDataApp", "advancedSettingsApp", "agentApp", "aggregate", "alert", "analyzeEvent", "annotation", "apmApp", "apmTrace", "appSearchApp", "apps", "arrowDown", "arrowLeft", "arrowRight", "arrowUp", "arrowStart", "arrowEnd", "asterisk", "auditbeatApp", "beaker", "bell", "bellSlash", "bolt", "boxesHorizontal", "boxesVertical", "branch", "broom", "brush", "bug", "bullseye", "calendar", "canvasApp", "check", "checkInCircleFilled", "cheer", "classificationJob", "clock", "cloudDrizzle", "cloudStormy", "cloudSunny", "codeApp", "color", "compute", "console", "consoleApp", "continuityAbove", "continuityAboveBelow", "continuityBelow", "continuityWithin", "controlsHorizontal", "controlsVertical", "copy", "copyClipboard", "createAdvancedJob", "createMultiMetricJob", "createPopulationJob", "createSingleMetricJob", "cross", "crossClusterReplicationApp", "crossInACircleFilled", "crosshairs", "currency", "cut", "dashboardApp", "dataVisualizer", "database", "devToolsApp", "discoverApp", "document", "documentEdit", "documentation", "documents", "dot", "doubleArrowLeft", "doubleArrowRight", "download", "editorAlignCenter", "editorAlignLeft", "editorAlignRight", "editorBold", "editorCodeBlock", "editorComment", "editorDistributeHorizontal", "editorDistributeVertical", "editorHeading", "editorItalic", "editorItemAlignBottom", "editorItemAlignCenter", "editorItemAlignLeft", "editorItemAlignMiddle", "editorItemAlignRight", "editorItemAlignTop", "editorLink", "editorOrderedList", "editorPositionBottomLeft", "editorPositionBottomRight", "editorPositionTopLeft", "editorPositionTopRight", "editorRedo", "editorStrike", "editorTable", "editorUnderline", "editorUndo", "editorUnorderedList", "email", "empty", "emsApp", "eql", "eraser", "exit", "expand", "expandMini", "exportAction", "eye", "eyeClosed", "faceHappy", "faceNeutral", "faceSad", "filebeatApp", "filter", "flag", "fleetApp", "fold", "folderCheck", "folderClosed", "folderExclamation", "folderOpen", "frameNext", "framePrevious", "fullScreen", "fullScreenExit", "function", "gear", "gisApp", "glasses", "globe", "grab", "grabHorizontal", "graphApp", "grid", "grokApp", "heart", "heartbeatApp", "heatmap", "help", "home", "iInCircle", "image", "importAction", "indexClose", "indexEdit", "indexFlush", "indexManagementApp", "indexMapping", "indexOpen", "indexPatternApp", "indexRollupApp", "indexRuntime", "indexSettings", "inputOutput", "inspect", "invert", "ip", "keyboardShortcut", "kqlField", "kqlFunction", "kqlOperand", "kqlSelector", "kqlValue", "layers", "lensApp", "lettering", "link", "list", "listAdd", "lock", "lockOpen", "logoAWS", "logoAWSMono", "logoAerospike", "logoApache", "logoAppSearch", "logoAzure", "logoAzureMono", "logoBeats", "logoBusinessAnalytics", "logoCeph", "logoCloud", "logoCloudEnterprise", "logoCode", "logoCodesandbox", "logoCouchbase", "logoDocker", "logoDropwizard", "logoElastic", "logoElasticStack", "logoElasticsearch", "logoEnterpriseSearch", "logoEtcd", "logoGCP", "logoGCPMono", "logoGithub", "logoGmail", "logoGolang", "logoGoogleG", "logoHAproxy", "logoIBM", "logoIBMMono", "logoKafka", "logoKibana", "logoKubernetes", "logoLogging", "logoLogstash", "logoMaps", "logoMemcached", "logoMetrics", "logoMongodb", "logoMySQL", "logoNginx", "logoObservability", "logoOsquery", "logoPhp", "logoPostgres", "logoPrometheus", "logoRabbitmq", "logoRedis", "logoSecurity", "logoSiteSearch", "logoSketch", "logoSlack", "logoUptime", "logoWebhook", "logoWindows", "logoWorkplaceSearch", "logsApp", "logstashFilter", "logstashIf", "logstashInput", "logstashOutput", "logstashQueue", "machineLearningApp", "magnet", "magnifyWithExclamation", "magnifyWithMinus", "magnifyWithPlus", "managementApp", "mapMarker", "memory", "menu", "menuDown", "menuLeft", "menuRight", "menuUp", "merge", "metricbeatApp", "metricsApp", "minimize", "minus", "minusInCircle", "minusInCircleFilled", "mobile", "monitoringApp", "moon", "nested", "node", "notebookApp", "number", "offline", "online", "outlierDetectionJob", "package", "packetbeatApp", "pageSelect", "pagesSelect", "paperClip", "partial", "pause", "payment", "pencil", "percent", "pin", "pinFilled", "pipelineApp", "play", "playFilled", "plus", "plusInCircle", "plusInCircleFilled", "popout", "push", "questionInCircle", "quote", "recentlyViewedApp", "refresh", "regressionJob", "reporter", "reportingApp", "returnKey", "save", "savedObjectsApp", "scale", "search", "searchProfilerApp", "securityAnalyticsApp", "securityApp", "securitySignal", "securitySignalDetected", "securitySignalResolved", "shard", "share", "snowflake", "sortDown", "sortLeft", "sortRight", "sortUp", "sortable", "spacesApp", "sqlApp", "starEmpty", "starEmptySpace", "starFilled", "starFilledSpace", "starMinusEmpty", "starMinusFilled", "starPlusEmpty", "starPlusFilled", "stats", "stop", "stopFilled", "stopSlash", "storage", "string", "submodule", "sun", "swatchInput", "symlink", "tableDensityCompact", "tableDensityExpanded", "tableDensityNormal", "tableOfContents", "tag", "tear", "temperature", "timeline", "timelionApp", "timeRefresh", "timeslider", "training", "trash", "unfold", "unlink", "upgradeAssistantApp", "uptimeApp", "user", "users", "usersRolesApp", "vector", "videoPlayer", "visArea", "visAreaStacked", "visBarHorizontal", "visBarHorizontalStacked", "visBarVertical", "visBarVerticalStacked", "visGauge", "visGoal", "visLine", "visMapCoordinate", "visMapRegion", "visMetric", "visPie", "visTable", "visTagCloud", "visText", "visTimelion", "visVega", "visVisualBuilder", "visualizeApp", "watchesApp", "wordWrap", "wordWrapDisabled", "workplaceSearchApp", "wrench", "tokenClass", "tokenProperty", "tokenEnum", "tokenVariable", "tokenMethod", "tokenAnnotation", "tokenException", "tokenInterface", "tokenParameter", "tokenField", "tokenElement", "tokenFunction", "tokenBoolean", "tokenString", "tokenArray", "tokenNumber", "tokenConstant", "tokenObject", "tokenEvent", "tokenKey", "tokenNull", "tokenStruct", "tokenPackage", "tokenOperator", "tokenEnumMember", "tokenRepo", "tokenSymbol", "tokenFile", "tokenModule", "tokenNamespace", "tokenDate", "tokenIP", "tokenNested", "tokenAlias", "tokenShape", "tokenGeo", "tokenRange", "tokenBinary", "tokenJoin", "tokenPercolator", "tokenFlattened", "tokenRankFeature", "tokenRankFeatures", "tokenKeyword", "tokenTag", "tokenCompletionSuggester", "tokenDenseVector", "tokenText", "tokenTokenCount", "tokenSearchType", "tokenHistogram"]).isRequired, PropTypes.func.isRequired])
    }).isRequired, PropTypes.shape({
      /**
         * The function that renders the action. Note that the returned node is expected to have `onFocus` and `onBlur` functions
         */
      render: PropTypes.func.isRequired,

      /**
         * A callback that defines whether the action is available
         */
      available: PropTypes.func,

      /**
         * A callback that defines whether the action is enabled
         */
      enabled: PropTypes.func,
      isPrimary: PropTypes.bool
    }).isRequired]).isRequired).isRequired,

    /**
       * The display name of the column
       */
    name: PropTypes.node,

    /**
       * A description of the column (will be presented as a title over the column header
       */
    description: PropTypes.string,

    /**
       * A CSS width property. Hints for the required width of the column
       */
    width: PropTypes.string
  }).isRequired]).isRequired),

  /**
     * Error message to display
     */

  /**
     * Error message to display
     */
  error: PropTypes.string,

  /**
     * Describes the content of the table. If not specified, the caption will be "This table contains {itemCount} rows."
     */

  /**
     * Describes the content of the table. If not specified, the caption will be "This table contains {itemCount} rows."
     */
  tableCaption: PropTypes.string,

  /**
     * Indicates which column should be used as the identifying cell in each row. Should match a "field" prop in FieldDataColumn
     */

  /**
     * Indicates which column should be used as the identifying cell in each row. Should match a "field" prop in FieldDataColumn
     */
  rowHeader: PropTypes.string,
  hasActions: PropTypes.bool,
  isExpandable: PropTypes.bool,
  isSelectable: PropTypes.bool,

  /**
     * Provides an infinite loading indicator
     */

  /**
     * Provides an infinite loading indicator
     */
  loading: PropTypes.bool,

  /**
     * Called whenever pagination or sorting changes (this property is required when either pagination or sorting is configured). See #Criteria or #CriteriaWithPagination
     */
  onChange: PropTypes.func,

  /**
     * If true, will convert table to cards in mobile view
     */

  /**
     * If true, will convert table to cards in mobile view
     */
  responsive: PropTypes.bool,

  /**
     * Applied to `EuiTableRow`
     */

  /**
     * Applied to `EuiTableRow`
     */
  rowProps: PropTypes.oneOfType([PropTypes.any.isRequired, PropTypes.func.isRequired]),

  /**
     * Configures #EuiTableSelectionType
     */

  /**
     * Configures #EuiTableSelectionType
     */
  selection: PropTypes.shape({
    /**
       * A callback that will be called whenever the item selection changes
       */
    onSelectionChange: PropTypes.func,

    /**
       * A callback that is called per item to indicate whether it is selectable
       */
    selectable: PropTypes.func,

    /**
       * A callback that is called per item to retrieve a message for its selectable state.We display these messages as a tooltip on an unselectable checkbox
       */
    selectableMessage: PropTypes.func,
    initialSelected: PropTypes.arrayOf(PropTypes.any.isRequired)
  }),

  /**
     * Sets the table-layout CSS property. Note that auto tableLayout prevents truncateText from working properly.
     */

  /**
     * Sets the table-layout CSS property. Note that auto tableLayout prevents truncateText from working properly.
     */
  tableLayout: PropTypes.oneOf(["fixed", "auto"]),

  /**
     * Applied to table cells => Any cell using render function will set this to be false, leading to unnecessary word breaks. Apply textOnly: true in order to ensure it breaks properly
     */

  /**
     * Applied to table cells => Any cell using render function will set this to be false, leading to unnecessary word breaks. Apply textOnly: true in order to ensure it breaks properly
     */
  textOnly: PropTypes.bool,
  message: PropTypes.node,

  /**
     * Configures #Search.
     */

  /**
     * Configures #Search.
     */
  search: PropTypes.oneOfType([PropTypes.bool.isRequired, PropTypes.shape({
    onChange: PropTypes.func,

    /**
       The initial query the bar will hold when first mounted
       */
    defaultQuery: PropTypes.oneOfType([PropTypes.any.isRequired, PropTypes.string.isRequired]),

    /**
       If you wish to use the search bar as a controlled component, continuously pass the query via this prop.
       */
    query: PropTypes.oneOfType([PropTypes.any.isRequired, PropTypes.string.isRequired]),

    /**
       Configures the search box. Set `placeholder` to change the placeholder text in the box and `incremental` to support incremental (as you type) search.
       */
    box: PropTypes.shape({
      name: PropTypes.string,
      id: PropTypes.string,
      placeholder: PropTypes.string,
      value: PropTypes.string,
      isInvalid: PropTypes.bool,
      fullWidth: PropTypes.bool,
      isLoading: PropTypes.bool,

      /**
         * Called when the user presses [Enter] OR on change if the incremental prop is `true`.
         * If you don't need the on[Enter] functionality, prefer using onChange
         */
      onSearch: PropTypes.func,

      /**
         * When `true` the search will be executed (that is, the `onSearch` will be called) as the
         * user types.
         */
      incremental: PropTypes.bool,

      /**
         * when `true` creates a shorter height input
         */
      compressed: PropTypes.bool,
      inputRef: PropTypes.func,

      /**
         * Shows a button that quickly clears any input
         */
      isClearable: PropTypes.bool,

      /**
         * Creates an input group with element(s) coming before input
         * `string` | `ReactElement` or an array of these
         */
      prepend: PropTypes.oneOfType([PropTypes.oneOfType([PropTypes.string.isRequired, PropTypes.element.isRequired]).isRequired, PropTypes.arrayOf(PropTypes.oneOfType([PropTypes.string.isRequired, PropTypes.element.isRequired]).isRequired).isRequired]),

      /**
         * Creates an input group with element(s) coming after input.
         * `string` | `ReactElement` or an array of these
         */
      append: PropTypes.oneOfType([PropTypes.oneOfType([PropTypes.string.isRequired, PropTypes.element.isRequired]).isRequired, PropTypes.arrayOf(PropTypes.oneOfType([PropTypes.string.isRequired, PropTypes.element.isRequired]).isRequired).isRequired]),
      className: PropTypes.string,
      "aria-label": PropTypes.string,
      "data-test-subj": PropTypes.string,
      // Boolean values are not meaningful to this EuiSearchBox, but are allowed so that other
      // components can use e.g. a true value to mean "auto-derive a schema". See EuiInMemoryTable.
      // Admittedly, this is a bit of a hack.
      schema: PropTypes.oneOfType([PropTypes.shape({
        strict: PropTypes.bool,
        fields: PropTypes.any,
        flags: PropTypes.arrayOf(PropTypes.string.isRequired)
      }).isRequired, PropTypes.bool.isRequired])
    }),

    /**
       An array of search filters. See #SearchFilterConfig.
       */
    filters: PropTypes.arrayOf(PropTypes.oneOfType([PropTypes.shape({
      type: PropTypes.oneOf(["is"]).isRequired,
      field: PropTypes.string.isRequired,
      name: PropTypes.string.isRequired,
      negatedName: PropTypes.string,
      available: PropTypes.func
    }).isRequired, PropTypes.shape({
      type: PropTypes.oneOf(["field_value_selection"]).isRequired,
      field: PropTypes.string,
      name: PropTypes.string.isRequired,

      /**
         * See #FieldValueOptionType
         */
      options: PropTypes.oneOfType([PropTypes.arrayOf(PropTypes.shape({
        field: PropTypes.string,
        value: PropTypes.oneOfType([PropTypes.string.isRequired, PropTypes.number.isRequired, PropTypes.bool.isRequired, PropTypes.shape({
          type: PropTypes.oneOf(["date"]).isRequired,
          raw: PropTypes.any.isRequired,
          granularity: PropTypes.oneOfType([PropTypes.shape({
            es: PropTypes.oneOf(["d", "w", "M", "y"]).isRequired,
            js: PropTypes.oneOf(["day", "week", "month", "year"]).isRequired,
            isSame: PropTypes.func.isRequired,
            start: PropTypes.func.isRequired,
            startOfNext: PropTypes.func.isRequired,
            iso8601: PropTypes.func.isRequired
          }).isRequired, PropTypes.oneOf([undefined])]).isRequired,
          text: PropTypes.string.isRequired,
          resolve: PropTypes.func.isRequired
        }).isRequired]).isRequired,
        name: PropTypes.string,
        view: PropTypes.node
      }).isRequired).isRequired, PropTypes.func.isRequired]).isRequired,
      filterWith: PropTypes.oneOfType([PropTypes.oneOf(["prefix", "includes"]), PropTypes.func.isRequired]),
      cache: PropTypes.number,
      multiSelect: PropTypes.oneOfType([PropTypes.bool.isRequired, PropTypes.oneOf(["and", "or"])]),
      loadingMessage: PropTypes.string,
      noOptionsMessage: PropTypes.string,
      searchThreshold: PropTypes.number,
      available: PropTypes.func,
      autoClose: PropTypes.bool,
      operator: PropTypes.oneOf(["eq", "exact", "gt", "gte", "lt", "lte"])
    }).isRequired, PropTypes.shape({
      type: PropTypes.oneOf(["field_value_toggle"]).isRequired,
      field: PropTypes.string.isRequired,
      value: PropTypes.oneOfType([PropTypes.string.isRequired, PropTypes.number.isRequired, PropTypes.bool.isRequired, PropTypes.shape({
        type: PropTypes.oneOf(["date"]).isRequired,
        raw: PropTypes.any.isRequired,
        granularity: PropTypes.oneOfType([PropTypes.shape({
          es: PropTypes.oneOf(["d", "w", "M", "y"]).isRequired,
          js: PropTypes.oneOf(["day", "week", "month", "year"]).isRequired,
          isSame: PropTypes.func.isRequired,
          start: PropTypes.func.isRequired,
          startOfNext: PropTypes.func.isRequired,
          iso8601: PropTypes.func.isRequired
        }).isRequired, PropTypes.oneOf([undefined])]).isRequired,
        text: PropTypes.string.isRequired,
        resolve: PropTypes.func.isRequired
      }).isRequired]).isRequired,
      name: PropTypes.string.isRequired,
      negatedName: PropTypes.string,
      available: PropTypes.func,
      operator: PropTypes.oneOf(["eq", "exact", "gt", "gte", "lt", "lte"])
    }).isRequired, PropTypes.shape({
      type: PropTypes.oneOf(["field_value_toggle_group"]).isRequired,
      field: PropTypes.string.isRequired,

      /**
         * See #FieldValueToggleGroupFilterItemType
         */
      items: PropTypes.arrayOf(PropTypes.shape({
        value: PropTypes.oneOfType([PropTypes.string.isRequired, PropTypes.number.isRequired, PropTypes.bool.isRequired]).isRequired,
        name: PropTypes.string.isRequired,
        negatedName: PropTypes.string,
        operator: PropTypes.oneOf(["eq", "exact", "gt", "gte", "lt", "lte"])
      }).isRequired).isRequired,
      available: PropTypes.func
    }).isRequired]).isRequired),

    /**
       * Tools which go to the left of the search bar.
       */
    toolsLeft: PropTypes.oneOfType([PropTypes.element.isRequired, PropTypes.arrayOf(PropTypes.element.isRequired).isRequired]),

    /**
       * Tools which go to the right of the search bar.
       */
    toolsRight: PropTypes.oneOfType([PropTypes.element.isRequired, PropTypes.arrayOf(PropTypes.element.isRequired).isRequired]),

    /**
       * Date formatter to use when parsing date values
       */
    dateFormat: PropTypes.any,
    className: PropTypes.string,
    "aria-label": PropTypes.string,
    "data-test-subj": PropTypes.string
  }).isRequired]),
  pagination: PropTypes.oneOfType([PropTypes.oneOf([undefined]), PropTypes.oneOfType([PropTypes.bool.isRequired, PropTypes.shape({
    pageSizeOptions: PropTypes.arrayOf(PropTypes.number.isRequired),
    hidePerPageOptions: PropTypes.bool,
    initialPageIndex: PropTypes.number,
    initialPageSize: PropTypes.number,
    pageIndex: PropTypes.number,
    pageSize: PropTypes.number
  }).isRequired])]),
  sorting: PropTypes.oneOfType([PropTypes.bool.isRequired, PropTypes.shape({
    sort: PropTypes.shape({
      field: PropTypes.string.isRequired,
      direction: PropTypes.oneOfType([PropTypes.any.isRequired, PropTypes.any.isRequired]).isRequired
    }).isRequired
  }).isRequired]),

  /**
     * Set `allowNeutralSort` to false to force column sorting. Defaults to true.
     */

  /**
     * Set `allowNeutralSort` to false to force column sorting. Defaults to true.
     */
  allowNeutralSort: PropTypes.bool,

  /**
     * Callback for when table pagination or sorting is changed. This is meant to be informational only, and not used to set any state as the in-memory table already manages this state. See #Criteria or #CriteriaWithPagination.
     */
  onTableChange: PropTypes.func,
  executeQueryOptions: PropTypes.shape({
    defaultFields: PropTypes.arrayOf(PropTypes.string.isRequired),
    isClauseMatcher: PropTypes.func,
    explain: PropTypes.bool
  }),

  /**
     * Insert content between the search bar and table components.
     */

  /**
     * Insert content between the search bar and table components.
     */
  childrenBetween: PropTypes.node
};