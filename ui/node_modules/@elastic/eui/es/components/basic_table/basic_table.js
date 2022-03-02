function _typeof(obj) { "@babel/helpers - typeof"; if (typeof Symbol === "function" && typeof Symbol.iterator === "symbol") { _typeof = function _typeof(obj) { return typeof obj; }; } else { _typeof = function _typeof(obj) { return obj && typeof Symbol === "function" && obj.constructor === Symbol && obj !== Symbol.prototype ? "symbol" : typeof obj; }; } return _typeof(obj); }

function _toConsumableArray(arr) { return _arrayWithoutHoles(arr) || _iterableToArray(arr) || _unsupportedIterableToArray(arr) || _nonIterableSpread(); }

function _nonIterableSpread() { throw new TypeError("Invalid attempt to spread non-iterable instance.\nIn order to be iterable, non-array objects must have a [Symbol.iterator]() method."); }

function _unsupportedIterableToArray(o, minLen) { if (!o) return; if (typeof o === "string") return _arrayLikeToArray(o, minLen); var n = Object.prototype.toString.call(o).slice(8, -1); if (n === "Object" && o.constructor) n = o.constructor.name; if (n === "Map" || n === "Set") return Array.from(o); if (n === "Arguments" || /^(?:Ui|I)nt(?:8|16|32)(?:Clamped)?Array$/.test(n)) return _arrayLikeToArray(o, minLen); }

function _iterableToArray(iter) { if (typeof Symbol !== "undefined" && Symbol.iterator in Object(iter)) return Array.from(iter); }

function _arrayWithoutHoles(arr) { if (Array.isArray(arr)) return _arrayLikeToArray(arr); }

function _arrayLikeToArray(arr, len) { if (len == null || len > arr.length) len = arr.length; for (var i = 0, arr2 = new Array(len); i < len; i++) { arr2[i] = arr[i]; } return arr2; }

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
import React, { Component, Fragment } from 'react';
import PropTypes from "prop-types";
import classNames from 'classnames';
import { formatAuto, formatBoolean, formatDate, formatNumber, formatText, LEFT_ALIGNMENT, RIGHT_ALIGNMENT, SortDirection } from '../../services';
import { isFunction } from '../../services/predicate';
import { get } from '../../services/objects';
import { EuiFlexGroup, EuiFlexItem } from '../flex';
import { EuiCheckbox } from '../form';
import { EuiTable, EuiTableBody, EuiTableFooter, EuiTableFooterCell, EuiTableHeader, EuiTableHeaderCell, EuiTableHeaderCellCheckbox, EuiTableHeaderMobile, EuiTableRow, EuiTableRowCell, EuiTableRowCellCheckbox, EuiTableSortMobile } from '../table';
import { CollapsedItemActions } from './collapsed_item_actions';
import { ExpandedItemActions } from './expanded_item_actions';
import { PaginationBar } from './pagination_bar';
import { EuiIcon } from '../icon';
import { EuiScreenReaderOnly } from '../accessibility';
import { EuiI18n } from '../i18n';
import { EuiDelayRender } from '../delay_render';
import { htmlIdGenerator } from '../../services/accessibility';
import { jsx as ___EmotionJSX } from "@emotion/react";
var dataTypesProfiles = {
  auto: {
    align: LEFT_ALIGNMENT,
    render: function render(value) {
      return formatAuto(value);
    }
  },
  string: {
    align: LEFT_ALIGNMENT,
    render: function render(value) {
      return formatText(value);
    }
  },
  number: {
    align: RIGHT_ALIGNMENT,
    render: function render(value) {
      return formatNumber(value);
    }
  },
  boolean: {
    align: LEFT_ALIGNMENT,
    render: function render(value) {
      return formatBoolean(value);
    }
  },
  date: {
    align: LEFT_ALIGNMENT,
    render: function render(value) {
      return formatDate(value);
    }
  }
};
var DATA_TYPES = Object.keys(dataTypesProfiles);
export function getItemId(item, itemId) {
  if (itemId) {
    if (isFunction(itemId)) {
      return itemId(item);
    } // @ts-ignore never mind about the index signature


    return item[itemId];
  }
}

function getRowProps(item, rowProps) {
  if (rowProps) {
    if (isFunction(rowProps)) {
      return rowProps(item);
    }

    return rowProps;
  }

  return {};
}

function getCellProps(item, column, cellProps) {
  if (cellProps) {
    if (isFunction(cellProps)) {
      return cellProps(item, column);
    }

    return cellProps;
  }

  return {};
}

function getColumnFooter(column, _ref) {
  var items = _ref.items,
      pagination = _ref.pagination;
  var _ref2 = column,
      footer = _ref2.footer;

  if (footer) {
    if (isFunction(footer)) {
      return footer({
        items: items,
        pagination: pagination
      });
    }

    return footer;
  }

  return undefined;
}

function hasPagination(x) {
  return x.hasOwnProperty('pagination') && !!x.pagination;
}

export var EuiBasicTable = /*#__PURE__*/function (_Component) {
  _inherits(EuiBasicTable, _Component);

  var _super = _createSuper(EuiBasicTable);

  _createClass(EuiBasicTable, null, [{
    key: "getDerivedStateFromProps",
    value: function getDerivedStateFromProps(nextProps, prevState) {
      if (!nextProps.selection) {
        // next props doesn't have a selection, reset our state
        return {
          selection: []
        };
      }

      var itemId = nextProps.itemId;
      var selection = prevState.selection.filter(function (selectedItem) {
        return nextProps.items.findIndex(function (item) {
          return getItemId(item, itemId) === getItemId(selectedItem, itemId);
        }) !== -1;
      });

      if (selection.length !== prevState.selection.length) {
        if (nextProps.selection.onSelectionChange) {
          nextProps.selection.onSelectionChange(selection);
        }

        return {
          selection: selection
        };
      }

      return null;
    } // used for moving in & out of `loading` state

  }]);

  function EuiBasicTable(props) {
    var _this;

    _classCallCheck(this, EuiBasicTable);

    _this = _super.call(this, props);

    _defineProperty(_assertThisInitialized(_this), "cleanups", []);

    _defineProperty(_assertThisInitialized(_this), "tbody", null);

    _defineProperty(_assertThisInitialized(_this), "setTbody", function (tbody) {
      // remove listeners from an existing element
      _this.removeLoadingListeners(); // update the ref


      _this.tbody = tbody; // if loading, add listeners

      if (_this.props.loading === true && tbody) {
        _this.addLoadingListeners(tbody);
      }
    });

    _defineProperty(_assertThisInitialized(_this), "addLoadingListeners", function (tbody) {
      var listener = function listener(event) {
        event.stopPropagation();
        event.preventDefault();
      };

      ['mousedown', 'mouseup', 'mouseover', 'mouseout', 'mouseenter', 'mouseleave', 'click', 'dblclick', 'keydown', 'keyup', 'keypress'].forEach(function (event) {
        tbody.addEventListener(event, listener, true);

        _this.cleanups.push(function () {
          tbody.removeEventListener(event, listener, true);
        });
      });
    });

    _defineProperty(_assertThisInitialized(_this), "removeLoadingListeners", function () {
      _this.cleanups.forEach(function (cleanup) {
        return cleanup();
      });

      _this.cleanups.length = 0;
    });

    _defineProperty(_assertThisInitialized(_this), "tableId", htmlIdGenerator('__table')());

    _defineProperty(_assertThisInitialized(_this), "selectAllIdGenerator", htmlIdGenerator('_selection_column-checkbox'));

    _defineProperty(_assertThisInitialized(_this), "renderSelectAll", function (isMobile) {
      var _this$props = _this.props,
          items = _this$props.items,
          selection = _this$props.selection;

      if (!selection) {
        return;
      }

      var selectableItems = items.filter(function (item) {
        return !selection.selectable || selection.selectable(item);
      });
      var checked = _this.state.selection && selectableItems.length > 0 && _this.state.selection.length === selectableItems.length;
      var disabled = selectableItems.length === 0;

      var onChange = function onChange(event) {
        if (event.target.checked) {
          _this.changeSelection(selectableItems);
        } else {
          _this.changeSelection([]);
        }
      };

      return ___EmotionJSX(EuiI18n, {
        token: "euiBasicTable.selectAllRows",
        default: "Select all rows"
      }, function (selectAllRows) {
        return ___EmotionJSX(EuiCheckbox, {
          id: _this.selectAllIdGenerator(isMobile ? 'mobile' : 'desktop'),
          type: isMobile ? undefined : 'inList',
          checked: checked,
          disabled: disabled,
          onChange: onChange // Only add data-test-subj to one of the checkboxes
          ,
          "data-test-subj": isMobile ? undefined : 'checkboxSelectAll',
          "aria-label": selectAllRows,
          label: isMobile ? selectAllRows : null
        });
      });
    });

    _defineProperty(_assertThisInitialized(_this), "resolveColumnSortDirection", function (column) {
      var sorting = _this.props.sorting;
      var _ref3 = column,
          sortable = _ref3.sortable,
          field = _ref3.field,
          name = _ref3.name;

      if (!sorting || !sorting.sort || !sortable) {
        return;
      }

      if (sorting.sort.field === field || sorting.sort.field === name) {
        return sorting.sort.direction;
      }
    });

    _defineProperty(_assertThisInitialized(_this), "resolveColumnOnSort", function (column) {
      var sorting = _this.props.sorting;
      var _ref4 = column,
          sortable = _ref4.sortable,
          name = _ref4.name;

      if (!sorting || !sortable) {
        return;
      }

      if (!_this.props.onChange) {
        throw new Error("BasicTable is configured to be sortable on column [".concat(name, "] but\n          [onChange] is not configured. This callback must be implemented to handle the sort requests"));
      }

      return function () {
        return _this.onColumnSortChange(column);
      };
    });

    _this.state = {
      // used for checking if  initial selection is rendered
      initialSelectionRendered: false,
      selection: []
    };
    return _this;
  }

  _createClass(EuiBasicTable, [{
    key: "componentDidMount",
    value: function componentDidMount() {
      if (this.props.loading && this.tbody) this.addLoadingListeners(this.tbody);
      this.getInitialSelection();
    }
  }, {
    key: "componentDidUpdate",
    value: function componentDidUpdate(prevProps) {
      if (prevProps.loading !== this.props.loading) {
        if (this.props.loading && this.tbody) {
          this.addLoadingListeners(this.tbody);
        } else {
          this.removeLoadingListeners();
        }
      }

      this.getInitialSelection();
    }
  }, {
    key: "componentWillUnmount",
    value: function componentWillUnmount() {
      this.removeLoadingListeners();
    }
  }, {
    key: "getInitialSelection",
    value: function getInitialSelection() {
      if (this.props.selection && this.props.selection.initialSelected && !this.state.initialSelectionRendered && this.props.items.length > 0) {
        this.setState({
          selection: this.props.selection.initialSelected
        });
        this.setState({
          initialSelectionRendered: true
        });
      }
    }
  }, {
    key: "setSelection",
    value: function setSelection(newSelection) {
      this.changeSelection(newSelection);
    }
  }, {
    key: "buildCriteria",
    value: function buildCriteria(props) {
      var criteria = {};

      if (hasPagination(props)) {
        criteria.page = {
          index: props.pagination.pageIndex,
          size: props.pagination.pageSize
        };
      }

      if (props.sorting) {
        criteria.sort = props.sorting.sort;
      }

      return criteria;
    }
  }, {
    key: "changeSelection",
    value: function changeSelection(selection) {
      if (!this.props.selection) {
        return;
      }

      this.setState({
        selection: selection
      });

      if (this.props.selection.onSelectionChange) {
        this.props.selection.onSelectionChange(selection);
      }
    }
  }, {
    key: "clearSelection",
    value: function clearSelection() {
      this.changeSelection([]);
    }
  }, {
    key: "onPageSizeChange",
    value: function onPageSizeChange(size) {
      this.clearSelection();
      var currentCriteria = this.buildCriteria(this.props);

      var criteria = _objectSpread(_objectSpread({}, currentCriteria), {}, {
        page: {
          index: 0,
          // when page size changes, we take the user back to the first page
          size: size
        }
      });

      if (this.props.onChange) {
        this.props.onChange(criteria);
      }
    }
  }, {
    key: "onPageChange",
    value: function onPageChange(index) {
      this.clearSelection();
      var currentCriteria = this.buildCriteria(this.props);

      var criteria = _objectSpread(_objectSpread({}, currentCriteria), {}, {
        page: _objectSpread(_objectSpread({}, currentCriteria.page), {}, {
          index: index
        })
      });

      if (this.props.onChange) {
        this.props.onChange(criteria);
      }
    }
  }, {
    key: "onColumnSortChange",
    value: function onColumnSortChange(column) {
      this.clearSelection();
      var currentCriteria = this.buildCriteria(this.props);
      var direction = SortDirection.ASC;

      if (currentCriteria && currentCriteria.sort && (currentCriteria.sort.field === column.field || currentCriteria.sort.field === column.name)) {
        direction = SortDirection.reverse(currentCriteria.sort.direction);
      }

      var criteria = _objectSpread(_objectSpread({}, currentCriteria), {}, {
        // resetting the page if the criteria has one
        page: !currentCriteria.page ? undefined : {
          index: 0,
          size: currentCriteria.page.size
        },
        sort: {
          field: column.field || column.name,
          direction: direction
        }
      });

      if (this.props.onChange) {
        // @ts-ignore complex relationship between pagination's existence and criteria, the code logic ensures this is correctly maintained
        this.props.onChange(criteria);
      }
    }
  }, {
    key: "render",
    value: function render() {
      var _this$props2 = this.props,
          className = _this$props2.className,
          loading = _this$props2.loading,
          items = _this$props2.items,
          itemId = _this$props2.itemId,
          columns = _this$props2.columns,
          pagination = _this$props2.pagination,
          sorting = _this$props2.sorting,
          selection = _this$props2.selection,
          onChange = _this$props2.onChange,
          error = _this$props2.error,
          noItemsMessage = _this$props2.noItemsMessage,
          compressed = _this$props2.compressed,
          itemIdToExpandedRowMap = _this$props2.itemIdToExpandedRowMap,
          responsive = _this$props2.responsive,
          isSelectable = _this$props2.isSelectable,
          isExpandable = _this$props2.isExpandable,
          hasActions = _this$props2.hasActions,
          rowProps = _this$props2.rowProps,
          cellProps = _this$props2.cellProps,
          tableCaption = _this$props2.tableCaption,
          rowHeader = _this$props2.rowHeader,
          tableLayout = _this$props2.tableLayout,
          rest = _objectWithoutProperties(_this$props2, ["className", "loading", "items", "itemId", "columns", "pagination", "sorting", "selection", "onChange", "error", "noItemsMessage", "compressed", "itemIdToExpandedRowMap", "responsive", "isSelectable", "isExpandable", "hasActions", "rowProps", "cellProps", "tableCaption", "rowHeader", "tableLayout"]);

      var classes = classNames('euiBasicTable', {
        'euiBasicTable-loading': loading
      }, className);
      var table = this.renderTable();
      var paginationBar = this.renderPaginationBar();
      return ___EmotionJSX("div", _extends({
        className: classes
      }, rest), table, paginationBar);
    }
  }, {
    key: "renderTable",
    value: function renderTable() {
      var _this$props3 = this.props,
          compressed = _this$props3.compressed,
          responsive = _this$props3.responsive,
          tableLayout = _this$props3.tableLayout;
      var mobileHeader = responsive ? ___EmotionJSX(EuiTableHeaderMobile, null, ___EmotionJSX(EuiFlexGroup, {
        responsive: false,
        justifyContent: "spaceBetween",
        alignItems: "baseline"
      }, ___EmotionJSX(EuiFlexItem, {
        grow: false
      }, this.renderSelectAll(true)), ___EmotionJSX(EuiFlexItem, {
        grow: false
      }, this.renderTableMobileSort()))) : undefined;
      var caption = this.renderTableCaption();
      var head = this.renderTableHead();
      var body = this.renderTableBody();
      var footer = this.renderTableFooter();
      return ___EmotionJSX("div", null, mobileHeader, ___EmotionJSX(EuiTable, {
        id: this.tableId,
        tableLayout: tableLayout,
        responsive: responsive,
        compressed: compressed
      }, caption, head, body, footer));
    }
  }, {
    key: "renderTableMobileSort",
    value: function renderTableMobileSort() {
      var _this2 = this;

      var _this$props4 = this.props,
          columns = _this$props4.columns,
          sorting = _this$props4.sorting;
      var items = [];

      if (!sorting) {
        return null;
      }

      columns.forEach(function (column, index) {
        var _column, _column$mobileOptions;

        if (column.field && sorting.sort && !!sorting.enableAllColumns && column.sortable == null) {
          column = _objectSpread(_objectSpread({}, column), {}, {
            sortable: true
          });
        }

        if (!column.sortable || ((_column = column) === null || _column === void 0 ? void 0 : (_column$mobileOptions = _column.mobileOptions) === null || _column$mobileOptions === void 0 ? void 0 : _column$mobileOptions.show) === false) {
          return;
        }

        var sortDirection = _this2.resolveColumnSortDirection(column);

        items.push({
          name: column.name,
          key: "_data_s_".concat(column.field, "_").concat(index),
          onSort: _this2.resolveColumnOnSort(column),
          isSorted: !!sortDirection,
          isSortAscending: sortDirection ? SortDirection.isAsc(sortDirection) : undefined
        });
      });
      return items.length ? ___EmotionJSX(EuiTableSortMobile, {
        items: items
      }) : null;
    }
  }, {
    key: "renderTableCaption",
    value: function renderTableCaption() {
      var _this$props5 = this.props,
          items = _this$props5.items,
          pagination = _this$props5.pagination,
          tableCaption = _this$props5.tableCaption;
      var captionElement;

      if (tableCaption) {
        if (pagination) {
          captionElement = ___EmotionJSX(EuiI18n, {
            token: "euiBasicTable.tableCaptionWithPagination",
            default: "{tableCaption}; Page {page} of {pageCount}.",
            values: {
              tableCaption: tableCaption,
              page: pagination.pageIndex + 1,
              pageCount: Math.ceil(pagination.totalItemCount / pagination.pageSize)
            }
          });
        } else {
          captionElement = tableCaption;
        }
      } else {
        if (pagination) {
          if (pagination.totalItemCount > 0) {
            captionElement = ___EmotionJSX(EuiI18n, {
              token: "euiBasicTable.tableAutoCaptionWithPagination",
              default: "This table contains {itemCount} rows out of {totalItemCount} rows; Page {page} of {pageCount}.",
              values: {
                totalItemCount: pagination.totalItemCount,
                itemCount: items.length,
                page: pagination.pageIndex + 1,
                pageCount: Math.ceil(pagination.totalItemCount / pagination.pageSize)
              }
            });
          } else {
            captionElement = ___EmotionJSX(EuiI18n, {
              token: "euiBasicTable.tableSimpleAutoCaptionWithPagination",
              default: "This table contains {itemCount} rows; Page {page} of {pageCount}.",
              values: {
                itemCount: items.length,
                page: pagination.pageIndex + 1,
                pageCount: Math.ceil(pagination.totalItemCount / pagination.pageSize)
              }
            });
          }
        } else {
          captionElement = ___EmotionJSX(EuiI18n, {
            token: "euiBasicTable.tableAutoCaptionWithoutPagination",
            default: "This table contains {itemCount} rows.",
            values: {
              itemCount: items.length
            }
          });
        }
      }

      return ___EmotionJSX(EuiScreenReaderOnly, null, ___EmotionJSX("caption", {
        className: "euiTableCaption"
      }, ___EmotionJSX(EuiDelayRender, null, captionElement)));
    }
  }, {
    key: "renderTableHead",
    value: function renderTableHead() {
      var _this3 = this;

      var _this$props6 = this.props,
          columns = _this$props6.columns,
          selection = _this$props6.selection;
      var headers = [];

      if (selection) {
        headers.push(___EmotionJSX(EuiTableHeaderCellCheckbox, {
          key: "_selection_column_h"
        }, this.renderSelectAll(false)));
      }

      columns.forEach(function (column, index) {
        var _ref5 = column,
            field = _ref5.field,
            width = _ref5.width,
            name = _ref5.name,
            align = _ref5.align,
            dataType = _ref5.dataType,
            sortable = _ref5.sortable,
            mobileOptions = _ref5.mobileOptions,
            readOnly = _ref5.readOnly,
            description = _ref5.description;

        var columnAlign = align || _this3.getAlignForDataType(dataType); // actions column


        if (column.actions) {
          headers.push(___EmotionJSX(EuiTableHeaderCell, {
            key: "_actions_h_".concat(index),
            align: "right",
            width: width,
            description: description,
            mobileOptions: mobileOptions
          }, name));
          return;
        } // computed column


        if (!column.field) {
          var _sorting = {}; // computed columns are only sortable if their `sortable` is a function

          if (_this3.props.sorting && typeof sortable === 'function') {
            var sortDirection = _this3.resolveColumnSortDirection(column);

            _sorting.isSorted = !!sortDirection;
            _sorting.isSortAscending = sortDirection ? SortDirection.isAsc(sortDirection) : undefined;
            _sorting.onSort = _this3.resolveColumnOnSort(column);
            _sorting.readOnly = _this3.props.sorting.readOnly || readOnly;
          }

          headers.push(___EmotionJSX(EuiTableHeaderCell, _extends({
            key: "_computed_column_h_".concat(index),
            align: columnAlign,
            width: width,
            mobileOptions: mobileOptions,
            "data-test-subj": "tableHeaderCell_".concat(typeof name === 'string' ? name : '', "_").concat(index),
            description: description
          }, _sorting), name));
          return;
        } // field data column


        var sorting = {};

        if (_this3.props.sorting) {
          if (_this3.props.sorting.sort && !!_this3.props.sorting.enableAllColumns && column.sortable == null) {
            column = _objectSpread(_objectSpread({}, column), {}, {
              sortable: true
            });
          }

          var _ref6 = column,
              _sortable = _ref6.sortable;

          if (_sortable) {
            var _sortDirection = _this3.resolveColumnSortDirection(column);

            sorting.isSorted = !!_sortDirection;
            sorting.isSortAscending = _sortDirection ? SortDirection.isAsc(_sortDirection) : undefined;
            sorting.onSort = _this3.resolveColumnOnSort(column);
            sorting.readOnly = _this3.props.sorting.readOnly || readOnly;
          }
        }

        headers.push(___EmotionJSX(EuiTableHeaderCell, _extends({
          key: "_data_h_".concat(field, "_").concat(index),
          align: columnAlign,
          width: width,
          mobileOptions: mobileOptions,
          "data-test-subj": "tableHeaderCell_".concat(field, "_").concat(index),
          description: description
        }, sorting), name));
      });
      return ___EmotionJSX(EuiTableHeader, null, headers);
    }
  }, {
    key: "renderTableFooter",
    value: function renderTableFooter() {
      var _this$props7 = this.props,
          items = _this$props7.items,
          columns = _this$props7.columns,
          pagination = _this$props7.pagination,
          selection = _this$props7.selection;
      var footers = [];
      var hasDefinedFooter = false;

      if (selection) {
        // Create an empty cell to compensate for additional selection column
        footers.push(___EmotionJSX(EuiTableFooterCell, {
          key: "_selection_column_f"
        }, undefined));
      }

      columns.forEach(function (column) {
        var footer = getColumnFooter(column, {
          items: items,
          pagination: pagination
        });
        var _ref7 = column,
            mobileOptions = _ref7.mobileOptions,
            field = _ref7.field,
            align = _ref7.align;

        if (mobileOptions === null || mobileOptions === void 0 ? void 0 : mobileOptions.only) {
          return; // exclude columns that only exist for mobile headers
        }

        if (footer) {
          footers.push(___EmotionJSX(EuiTableFooterCell, {
            key: "footer_".concat(field, "_").concat(footers.length - 1),
            align: align
          }, footer));
          hasDefinedFooter = true;
        } else {
          // Footer is undefined, so create an empty cell to preserve layout
          footers.push(___EmotionJSX(EuiTableFooterCell, {
            key: "footer_empty_".concat(footers.length - 1),
            align: align
          }, undefined));
        }
      });
      return footers.length && hasDefinedFooter ? ___EmotionJSX(EuiTableFooter, null, footers) : null;
    }
  }, {
    key: "renderTableBody",
    value: function renderTableBody() {
      var _this4 = this;

      if (this.props.error) {
        return this.renderErrorBody(this.props.error);
      }

      var items = this.props.items;

      if (items.length === 0) {
        return this.renderEmptyBody();
      }

      var rows = items.map(function (item, index) {
        // if there's pagination the item's index must be adjusted to the where it is in the whole dataset
        var tableItemIndex = hasPagination(_this4.props) ? _this4.props.pagination.pageIndex * _this4.props.pagination.pageSize + index : index;
        return _this4.renderItemRow(item, tableItemIndex);
      });
      return ___EmotionJSX(EuiTableBody, {
        bodyRef: this.setTbody
      }, rows);
    }
  }, {
    key: "renderErrorBody",
    value: function renderErrorBody(error) {
      var colSpan = this.props.columns.length + (this.props.selection ? 1 : 0);
      return ___EmotionJSX(EuiTableBody, null, ___EmotionJSX(EuiTableRow, null, ___EmotionJSX(EuiTableRowCell, {
        align: "center",
        colSpan: colSpan,
        mobileOptions: {
          width: '100%'
        }
      }, ___EmotionJSX(EuiIcon, {
        type: "minusInCircle",
        color: "danger"
      }), " ", error)));
    }
  }, {
    key: "renderEmptyBody",
    value: function renderEmptyBody() {
      var _this$props8 = this.props,
          columns = _this$props8.columns,
          selection = _this$props8.selection,
          noItemsMessage = _this$props8.noItemsMessage;
      var colSpan = columns.length + (selection ? 1 : 0);
      return ___EmotionJSX(EuiTableBody, null, ___EmotionJSX(EuiTableRow, null, ___EmotionJSX(EuiTableRowCell, {
        align: "center",
        colSpan: colSpan,
        mobileOptions: {
          width: '100%'
        }
      }, noItemsMessage)));
    }
  }, {
    key: "renderItemRow",
    value: function renderItemRow(item, rowIndex) {
      var _this5 = this;

      var _this$props9 = this.props,
          columns = _this$props9.columns,
          selection = _this$props9.selection,
          isSelectable = _this$props9.isSelectable,
          hasActions = _this$props9.hasActions,
          rowHeader = _this$props9.rowHeader,
          _this$props9$itemIdTo = _this$props9.itemIdToExpandedRowMap,
          itemIdToExpandedRowMap = _this$props9$itemIdTo === void 0 ? {} : _this$props9$itemIdTo,
          isExpandable = _this$props9.isExpandable;
      var cells = [];
      var itemIdCallback = this.props.itemId;
      var itemId = getItemId(item, itemIdCallback) != null ? getItemId(item, itemIdCallback) : rowIndex;
      var selected = !selection ? false : this.state.selection && !!this.state.selection.find(function (selectedItem) {
        return getItemId(selectedItem, itemIdCallback) === itemId;
      });
      var calculatedHasSelection;

      if (selection) {
        cells.push(this.renderItemSelectionCell(itemId, item, selected));
        calculatedHasSelection = true;
      }

      var calculatedHasActions;
      columns.forEach(function (column, columnIndex) {
        if (column.actions) {
          cells.push(_this5.renderItemActionsCell(itemId, item, column, columnIndex));
          calculatedHasActions = true;
        } else if (column.field) {
          var fieldDataColumn = column;
          cells.push(_this5.renderItemFieldDataCell(itemId, item, column, columnIndex, fieldDataColumn.field === rowHeader));
        } else {
          cells.push(_this5.renderItemComputedCell(itemId, item, column, columnIndex));
        }
      }); // Occupy full width of table, taking checkbox & mobile only columns into account.

      var expandedRowColSpan = selection ? columns.length + 1 : columns.length;
      var mobileOnlyCols = columns.reduce(function (num, column) {
        var _mobileOptions;

        return (column === null || column === void 0 ? void 0 : (_mobileOptions = column.mobileOptions) === null || _mobileOptions === void 0 ? void 0 : _mobileOptions.only) ? num + 1 : num + 0; // BWC only
      }, 0);
      expandedRowColSpan = expandedRowColSpan - mobileOnlyCols; // We'll use the ID to associate the expanded row with the original.

      var hasExpandedRow = itemIdToExpandedRowMap.hasOwnProperty(itemId);
      var expandedRowId = hasExpandedRow ? "row_".concat(itemId, "_expansion") : undefined;
      var expandedRow = hasExpandedRow ? ___EmotionJSX(EuiTableRow, {
        id: expandedRowId,
        isExpandedRow: true,
        isSelectable: isSelectable
      }, ___EmotionJSX(EuiTableRowCell, {
        colSpan: expandedRowColSpan,
        textOnly: false
      }, itemIdToExpandedRowMap[itemId])) : undefined;
      var rowPropsCallback = this.props.rowProps;
      var rowProps = getRowProps(item, rowPropsCallback);

      var row = ___EmotionJSX(EuiTableRow, _extends({
        "aria-owns": expandedRowId,
        isSelectable: isSelectable == null ? calculatedHasSelection : isSelectable,
        isSelected: selected,
        hasActions: hasActions == null ? calculatedHasActions : hasActions,
        isExpandable: isExpandable
      }, rowProps), cells);

      return ___EmotionJSX(Fragment, {
        key: "row_".concat(itemId)
      }, row, expandedRow);
    }
  }, {
    key: "renderItemSelectionCell",
    value: function renderItemSelectionCell(itemId, item, selected) {
      var _this6 = this;

      var selection = this.props.selection;
      var key = "_selection_column_".concat(itemId);
      var checked = selected;
      var disabled = selection.selectable && !selection.selectable(item);
      var title = selection.selectableMessage && selection.selectableMessage(!disabled, item);

      var onChange = function onChange(event) {
        if (event.target.checked) {
          _this6.changeSelection([].concat(_toConsumableArray(_this6.state.selection), [item]));
        } else {
          var itemIdCallback = _this6.props.itemId;

          _this6.changeSelection(_this6.state.selection.reduce(function (selection, selectedItem) {
            if (getItemId(selectedItem, itemIdCallback) !== itemId) {
              selection.push(selectedItem);
            }

            return selection;
          }, []));
        }
      };

      return ___EmotionJSX(EuiTableRowCellCheckbox, {
        key: key
      }, ___EmotionJSX(EuiI18n, {
        token: "euiBasicTable.selectThisRow",
        default: "Select this row"
      }, function (selectThisRow) {
        return ___EmotionJSX(EuiCheckbox, {
          id: "".concat(_this6.tableId).concat(key, "-checkbox"),
          type: "inList",
          disabled: disabled,
          checked: checked,
          onChange: onChange,
          title: title || selectThisRow,
          "aria-label": title || selectThisRow,
          "data-test-subj": "checkboxSelectRow-".concat(itemId)
        });
      }));
    }
  }, {
    key: "renderItemActionsCell",
    value: function renderItemActionsCell(itemId, item, column, columnIndex) {
      var _this7 = this;

      var actionEnabled = function actionEnabled(action) {
        return _this7.state.selection.length === 0 && (!action.enabled || action.enabled(item));
      };

      var actualActions = column.actions.filter(function (action) {
        return !action.available || action.available(item);
      });

      if (actualActions.length > 2) {
        // if any of the actions `isPrimary`, add them inline as well, but only the first 2
        var primaryActions = actualActions.filter(function (o) {
          return o.isPrimary;
        });
        actualActions = primaryActions.slice(0, 2); // if we have more than 1 action, we don't show them all in the cell, instead we
        // put them all in a popover tool. This effectively means we can only have a maximum
        // of one tool per row (it's either and normal action, or it's a popover that shows multiple actions)
        //
        // here we create a single custom action that triggers the popover with all the configured actions

        actualActions.push({
          name: 'All actions',
          render: function render(item) {
            return ___EmotionJSX(CollapsedItemActions, {
              actions: column.actions,
              itemId: itemId,
              item: item,
              actionEnabled: actionEnabled
            });
          }
        });
      }

      var tools = ___EmotionJSX(ExpandedItemActions, {
        actions: actualActions,
        itemId: itemId,
        item: item,
        actionEnabled: actionEnabled
      });

      var key = "record_actions_".concat(itemId, "_").concat(columnIndex);
      return ___EmotionJSX(EuiTableRowCell, {
        showOnHover: true,
        key: key,
        align: "right",
        textOnly: false,
        hasActions: true
      }, tools);
    }
  }, {
    key: "renderItemFieldDataCell",
    value: function renderItemFieldDataCell(itemId, item, column, columnIndex, setScopeRow) {
      var field = column.field,
          render = column.render,
          dataType = column.dataType;
      var key = "_data_column_".concat(field, "_").concat(itemId, "_").concat(columnIndex);
      var contentRenderer = render || this.getRendererForDataType(dataType);
      var value = get(item, field);
      var content = contentRenderer(value, item);
      return this.renderItemCell(item, column, key, content, setScopeRow);
    }
  }, {
    key: "renderItemComputedCell",
    value: function renderItemComputedCell(itemId, item, column, columnIndex) {
      var render = column.render;
      var key = "_computed_column_".concat(itemId, "_").concat(columnIndex);
      var contentRenderer = render || this.getRendererForDataType();
      var content = contentRenderer(item);
      return this.renderItemCell(item, column, key, content, false);
    }
  }, {
    key: "renderItemCell",
    value: function renderItemCell(item, column, key, content, setScopeRow) {
      var _ref8 = column,
          align = _ref8.align,
          render = _ref8.render,
          dataType = _ref8.dataType,
          isExpander = _ref8.isExpander,
          textOnly = _ref8.textOnly,
          name = _ref8.name,
          field = _ref8.field,
          description = _ref8.description,
          sortable = _ref8.sortable,
          footer = _ref8.footer,
          mobileOptions = _ref8.mobileOptions,
          rest = _objectWithoutProperties(_ref8, ["align", "render", "dataType", "isExpander", "textOnly", "name", "field", "description", "sortable", "footer", "mobileOptions"]);

      var columnAlign = align || this.getAlignForDataType(dataType);
      var cellPropsCallback = this.props.cellProps;
      var cellProps = getCellProps(item, column, cellPropsCallback);
      return ___EmotionJSX(EuiTableRowCell, _extends({
        key: key,
        align: columnAlign,
        isExpander: isExpander,
        textOnly: textOnly || !render,
        setScopeRow: setScopeRow,
        mobileOptions: _objectSpread(_objectSpread({}, mobileOptions), {}, {
          render: mobileOptions && mobileOptions.render && mobileOptions.render(item),
          header: mobileOptions && mobileOptions.header === false ? false : name
        })
      }, cellProps, rest), content);
    }
  }, {
    key: "getRendererForDataType",
    value: function getRendererForDataType() {
      var dataType = arguments.length > 0 && arguments[0] !== undefined ? arguments[0] : 'auto';
      var profile = dataTypesProfiles[dataType];

      if (!profile) {
        throw new Error("Unknown dataType [".concat(dataType, "]. The supported data types are [").concat(DATA_TYPES.join(', '), "]"));
      }

      return profile.render;
    }
  }, {
    key: "getAlignForDataType",
    value: function getAlignForDataType() {
      var dataType = arguments.length > 0 && arguments[0] !== undefined ? arguments[0] : 'auto';
      var profile = dataTypesProfiles[dataType];

      if (!profile) {
        throw new Error("Unknown dataType [".concat(dataType, "]. The supported data types are [").concat(DATA_TYPES.join(', '), "]"));
      }

      return profile.align;
    }
  }, {
    key: "renderPaginationBar",
    value: function renderPaginationBar() {
      var _this8 = this;

      var _this$props10 = this.props,
          error = _this$props10.error,
          pagination = _this$props10.pagination,
          tableCaption = _this$props10.tableCaption,
          onChange = _this$props10.onChange;

      if (!error && pagination && pagination.totalItemCount > 0) {
        if (!onChange) {
          throw new Error("The Basic Table is configured with pagination but [onChange] is\n        not configured. This callback must be implemented to handle pagination changes");
        }

        return ___EmotionJSX(EuiI18n, {
          token: "euiBasicTable.tablePagination",
          default: "Pagination for table: {tableCaption}",
          values: {
            tableCaption: tableCaption
          }
        }, function (tablePagination) {
          return ___EmotionJSX(PaginationBar, {
            pagination: pagination,
            onPageSizeChange: _this8.onPageSizeChange.bind(_this8),
            onPageChange: _this8.onPageChange.bind(_this8),
            "aria-controls": _this8.tableId,
            "aria-label": tablePagination
          });
        });
      }
    }
  }]);

  return EuiBasicTable;
}(Component);

_defineProperty(EuiBasicTable, "defaultProps", {
  responsive: true,
  tableLayout: 'fixed',
  noItemsMessage: ___EmotionJSX(EuiI18n, {
    token: "euiBasicTable.noItemsMessage",
    default: "No items found"
  })
});

EuiBasicTable.propTypes = {
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
     * Message to display if table is empty
     */

  /**
     * Message to display if table is empty
     */
  noItemsMessage: PropTypes.node,

  /**
     * Called whenever pagination or sorting changes (this property is required when either pagination or sorting is configured). See #Criteria or #CriteriaWithPagination
     */
  onChange: PropTypes.func,

  /**
     * Configures #Pagination
     */
  pagination: PropTypes.oneOfType([PropTypes.oneOf([undefined]), PropTypes.shape({
    /**
       * The current page (zero-based) index
       */
    pageIndex: PropTypes.number.isRequired,

    /**
       * The maximum number of items that can be shown in a single page
       */
    pageSize: PropTypes.number.isRequired,

    /**
       * The total number of items the page is "sliced" of
       */
    totalItemCount: PropTypes.number.isRequired,

    /**
       * Configures the page size dropdown options
       */
    pageSizeOptions: PropTypes.arrayOf(PropTypes.number.isRequired),

    /**
       * Hides the page size dropdown
       */
    hidePerPageOptions: PropTypes.bool
  })]),

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
     * Configures #EuiTableSortingType
     */

  /**
     * Configures #EuiTableSortingType
     */
  sorting: PropTypes.shape({
    /**
       * Indicates the property/field to sort on
       */
    sort: PropTypes.shape({
      field: PropTypes.any.isRequired,
      direction: PropTypes.oneOfType([PropTypes.any.isRequired, PropTypes.any.isRequired]).isRequired
    }),

    /**
       * Enables/disables unsorting of table columns. Supported by EuiInMemoryTable.
       */
    allowNeutralSort: PropTypes.bool,

    /**
       * Enables the default sorting ability for each column.
       */
    enableAllColumns: PropTypes.bool,

    /**
       * Disables the user's ability to change the sort but still shows the current direction
       */
    readOnly: PropTypes.bool
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
  textOnly: PropTypes.bool
};