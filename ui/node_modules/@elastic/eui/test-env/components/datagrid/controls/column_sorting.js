"use strict";

var _interopRequireWildcard = require("@babel/runtime/helpers/interopRequireWildcard");

var _interopRequireDefault = require("@babel/runtime/helpers/interopRequireDefault");

Object.defineProperty(exports, "__esModule", {
  value: true
});
exports.useDataGridColumnSorting = void 0;

var _toConsumableArray2 = _interopRequireDefault(require("@babel/runtime/helpers/toConsumableArray"));

var _slicedToArray2 = _interopRequireDefault(require("@babel/runtime/helpers/slicedToArray"));

var _classnames = _interopRequireDefault(require("classnames"));

var _react = _interopRequireWildcard(require("react"));

var _button = require("../../button");

var _drag_and_drop = require("../../drag_and_drop");

var _flex = require("../../flex");

var _i18n = require("../../i18n");

var _popover = require("../../popover");

var _text = require("../../text");

var _token = require("../../token");

var _column_sorting_draggable = require("./column_sorting_draggable");

var _data_grid_schema = require("../utils/data_grid_schema");

var _react2 = require("@emotion/react");

/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */
var useDataGridColumnSorting = function useDataGridColumnSorting(columns, sorting, schema, schemaDetectors, displayValues) {
  var _useState = (0, _react.useState)(false),
      _useState2 = (0, _slicedToArray2.default)(_useState, 2),
      isOpen = _useState2[0],
      setIsOpen = _useState2[1];

  var _useState3 = (0, _react.useState)(false),
      _useState4 = (0, _slicedToArray2.default)(_useState3, 2),
      availableColumnsIsOpen = _useState4[0],
      setAvailableColumnsIsOpen = _useState4[1]; // prune any non-existent/hidden columns from sorting


  (0, _react.useEffect)(function () {
    if (sorting) {
      var nextSortingColumns = [];
      var availableColumnIds = new Set(columns.map(function (_ref) {
        var id = _ref.id;
        return id;
      }));

      for (var i = 0; i < sorting.columns.length; i++) {
        var column = sorting.columns[i];

        if (availableColumnIds.has(column.id)) {
          nextSortingColumns.push(column);
        }
      } // if the column array lengths differ then the sorting columns have been pruned


      if (nextSortingColumns.length !== sorting.columns.length) {
        sorting.onSort(nextSortingColumns);
      }
    }
  }, [columns, sorting]);
  var sortingButtonText = (0, _i18n.useEuiI18n)('euiColumnSorting.button', 'Sort fields');
  var sortingButtonTextActive = (0, _i18n.useEuiI18n)('euiColumnSorting.buttonActive', function (_ref2) {
    var numberOfSortedFields = _ref2.numberOfSortedFields;
    return "".concat(numberOfSortedFields, " field").concat(numberOfSortedFields === 1 ? '' : 's', " sorted");
  }, {
    numberOfSortedFields: sorting != null ? sorting.columns.length : 0
  });
  if (sorting == null) return null;
  var activeColumnIds = new Set(sorting.columns.map(function (_ref3) {
    var id = _ref3.id;
    return id;
  }));

  var _columns$reduce = columns.reduce(function (acc, column) {
    if (activeColumnIds.has(column.id)) {
      acc.activeColumns.push(column);
    } else {
      acc.inactiveColumns.push(column);
    }

    return acc;
  }, {
    activeColumns: [],
    inactiveColumns: []
  }),
      inactiveColumns = _columns$reduce.inactiveColumns;

  var onDragEnd = function onDragEnd(_ref4) {
    var sourceIndex = _ref4.source.index,
        destination = _ref4.destination;

    if (destination) {
      var destinationIndex = destination.index;
      var nextColumns = (0, _drag_and_drop.euiDragDropReorder)(sorting.columns, sourceIndex, destinationIndex);
      sorting.onSort(nextColumns);
    }
  };

  var controlBtnClasses = (0, _classnames.default)('euiDataGrid__controlBtn', {
    'euiDataGrid__controlBtn--active': sorting.columns.length > 0
  });

  var schemaDetails = function schemaDetails(id) {
    return schema.hasOwnProperty(id) && schema[id].columnType != null ? (0, _data_grid_schema.getDetailsForSchema)(schemaDetectors, schema[id].columnType) : null;
  };

  var inactiveSortableColumns = inactiveColumns.filter(function (_ref5) {
    var id = _ref5.id,
        isSortable = _ref5.isSortable;
    var schemaDetail = schemaDetails(id);
    var sortable = true;

    if (isSortable != null) {
      sortable = isSortable;
    } else if (schemaDetail != null) {
      sortable = schemaDetail.hasOwnProperty('isSortable') ? schemaDetail.isSortable : true;
    }

    return sortable;
  });
  var columnSorting = (0, _react2.jsx)(_popover.EuiPopover, {
    "data-test-subj": "dataGridColumnSortingPopover",
    isOpen: isOpen,
    closePopover: function closePopover() {
      return setIsOpen(false);
    },
    anchorPosition: "downLeft",
    panelPaddingSize: "s",
    panelClassName: "euiDataGrid__controlPopoverWithDragDrop",
    button: (0, _react2.jsx)(_button.EuiButtonEmpty, {
      size: "xs",
      iconType: "sortable",
      color: "text",
      className: controlBtnClasses,
      "data-test-subj": "dataGridColumnSortingButton",
      onClick: function onClick() {
        return setIsOpen(!isOpen);
      }
    }, sorting.columns.length > 0 ? sortingButtonTextActive : sortingButtonText)
  }, sorting.columns.length > 0 ? (0, _react2.jsx)("div", {
    role: "region",
    "aria-live": "assertive",
    className: "euiDataGrid__controlScroll"
  }, (0, _react2.jsx)(_drag_and_drop.EuiDragDropContext, {
    onDragEnd: onDragEnd
  }, (0, _react2.jsx)(_drag_and_drop.EuiDroppable, {
    droppableId: "columnSorting"
  }, (0, _react2.jsx)(_react.Fragment, null, sorting.columns.map(function (_ref6, index) {
    var id = _ref6.id,
        direction = _ref6.direction;
    return (0, _react2.jsx)(_column_sorting_draggable.EuiDataGridColumnSortingDraggable, {
      key: id,
      id: id,
      display: displayValues[id],
      direction: direction,
      index: index,
      sorting: sorting,
      schema: schema,
      schemaDetectors: schemaDetectors
    });
  }))))) : (0, _react2.jsx)(_text.EuiText, {
    size: "s",
    color: "subdued"
  }, (0, _react2.jsx)("p", {
    role: "alert"
  }, (0, _react2.jsx)(_i18n.EuiI18n, {
    token: "euiColumnSorting.emptySorting",
    default: "Currently no fields are sorted"
  }))), (inactiveSortableColumns.length > 0 || sorting.columns.length > 0) && (0, _react2.jsx)(_popover.EuiPopoverFooter, null, (0, _react2.jsx)(_flex.EuiFlexGroup, {
    gutterSize: "m",
    justifyContent: "spaceBetween",
    responsive: false
  }, (0, _react2.jsx)(_flex.EuiFlexItem, {
    grow: false
  }, inactiveSortableColumns.length > 0 && (0, _react2.jsx)(_popover.EuiPopover, {
    "data-test-subj": "dataGridColumnSortingPopoverColumnSelection",
    isOpen: availableColumnsIsOpen,
    closePopover: function closePopover() {
      return setAvailableColumnsIsOpen(false);
    },
    anchorPosition: "downLeft",
    panelPaddingSize: "none",
    button: (0, _react2.jsx)(_button.EuiButtonEmpty, {
      size: "xs",
      flush: "left",
      iconType: "arrowDown",
      iconSide: "right",
      "data-test-subj": "dataGridColumnSortingSelectionButton",
      onClick: function onClick() {
        return setAvailableColumnsIsOpen(!availableColumnsIsOpen);
      }
    }, (0, _react2.jsx)(_i18n.EuiI18n, {
      token: "euiColumnSorting.pickFields",
      default: "Pick fields to sort by"
    }))
  }, (0, _react2.jsx)(_i18n.EuiI18n, {
    token: "euiColumnSorting.sortFieldAriaLabel",
    default: "Sort by: "
  }, function (sortFieldAriaLabel) {
    return (0, _react2.jsx)("div", {
      className: "euiDataGridColumnSorting__fieldList",
      role: "listbox"
    }, inactiveSortableColumns.map(function (_ref7) {
      var id = _ref7.id,
          defaultSortDirection = _ref7.defaultSortDirection;
      return (0, _react2.jsx)("button", {
        key: id,
        className: "euiDataGridColumnSorting__field",
        "aria-label": "".concat(sortFieldAriaLabel, " ").concat(id),
        role: "option",
        "aria-selected": "false",
        "data-test-subj": "dataGridColumnSortingPopoverColumnSelection-".concat(id),
        onClick: function onClick() {
          var _schemaDetails;

          var nextColumns = (0, _toConsumableArray2.default)(sorting.columns);
          nextColumns.push({
            id: id,
            direction: defaultSortDirection || ((_schemaDetails = schemaDetails(id)) === null || _schemaDetails === void 0 ? void 0 : _schemaDetails.defaultSortDirection) || 'asc'
          });
          sorting.onSort(nextColumns);
        }
      }, (0, _react2.jsx)(_flex.EuiFlexGroup, {
        alignItems: "center",
        gutterSize: "s",
        component: "span",
        responsive: false
      }, (0, _react2.jsx)(_flex.EuiFlexItem, {
        grow: false
      }, (0, _react2.jsx)(_token.EuiToken, {
        iconType: schemaDetails(id) != null ? (0, _data_grid_schema.getDetailsForSchema)(schemaDetectors, schema[id].columnType).icon : 'tokenString',
        color: schemaDetails(id) != null ? (0, _data_grid_schema.getDetailsForSchema)(schemaDetectors, schema[id].columnType).color : undefined
      })), (0, _react2.jsx)(_flex.EuiFlexItem, {
        grow: false
      }, (0, _react2.jsx)(_text.EuiText, {
        size: "xs"
      }, displayValues[id]))));
    }));
  }))), sorting.columns.length > 0 ? (0, _react2.jsx)(_flex.EuiFlexItem, {
    grow: false
  }, (0, _react2.jsx)(_button.EuiButtonEmpty, {
    size: "xs",
    flush: "right",
    onClick: function onClick() {
      return sorting.onSort([]);
    },
    "data-test-subj": "dataGridColumnSortingClearButton"
  }, (0, _react2.jsx)(_i18n.EuiI18n, {
    token: "euiColumnSorting.clearAll",
    default: "Clear sorting"
  }))) : null)));
  return columnSorting;
};

exports.useDataGridColumnSorting = useDataGridColumnSorting;