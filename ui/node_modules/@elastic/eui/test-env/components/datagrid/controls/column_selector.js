"use strict";

var _interopRequireWildcard = require("@babel/runtime/helpers/interopRequireWildcard");

var _interopRequireDefault = require("@babel/runtime/helpers/interopRequireDefault");

Object.defineProperty(exports, "__esModule", {
  value: true
});
exports.useDataGridColumnSelector = void 0;

var _toConsumableArray2 = _interopRequireDefault(require("@babel/runtime/helpers/toConsumableArray"));

var _slicedToArray2 = _interopRequireDefault(require("@babel/runtime/helpers/slicedToArray"));

var _react = _interopRequireWildcard(require("react"));

var _classnames = _interopRequireDefault(require("classnames"));

var _popover = require("../../popover");

var _i18n = require("../../i18n");

var _button = require("../../button");

var _flex = require("../../flex");

var _form = require("../../form");

var _drag_and_drop = require("../../drag_and_drop");

var _icon = require("../../icon");

var _services = require("../../../services");

var _data_grid_toolbar = require("./data_grid_toolbar");

var _react2 = require("@emotion/react");

/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */
var useDataGridColumnSelector = function useDataGridColumnSelector(availableColumns, columnVisibility, showColumnSelector, displayValues) {
  var allowColumnHiding = (0, _data_grid_toolbar.getNestedObjectOptions)(showColumnSelector, 'allowHide');
  var allowColumnReorder = (0, _data_grid_toolbar.getNestedObjectOptions)(showColumnSelector, 'allowReorder');

  var _useDependentState = (0, _services.useDependentState)(function () {
    return availableColumns.map(function (_ref) {
      var id = _ref.id;
      return id;
    });
  }, [availableColumns]),
      _useDependentState2 = (0, _slicedToArray2.default)(_useDependentState, 2),
      sortedColumns = _useDependentState2[0],
      setSortedColumns = _useDependentState2[1];

  var visibleColumns = columnVisibility.visibleColumns,
      setVisibleColumns = columnVisibility.setVisibleColumns;
  var visibleColumnIds = (0, _react.useMemo)(function () {
    return new Set(visibleColumns);
  }, [visibleColumns]);

  var _useState = (0, _react.useState)(false),
      _useState2 = (0, _slicedToArray2.default)(_useState, 2),
      isOpen = _useState2[0],
      setIsOpen = _useState2[1];

  var setColumns = (0, _react.useCallback)(function (nextColumns) {
    setSortedColumns(nextColumns);
    var nextVisibleColumns = nextColumns.filter(function (id) {
      return visibleColumnIds.has(id);
    });
    setVisibleColumns(nextVisibleColumns);
  }, [setSortedColumns, setVisibleColumns, visibleColumnIds]);
  var onDragEnd = (0, _react.useCallback)(function (_ref2) {
    var sourceIndex = _ref2.source.index,
        destination = _ref2.destination;

    if (destination) {
      var destinationIndex = destination.index;
      var nextSortedColumns = (0, _drag_and_drop.euiDragDropReorder)(sortedColumns, sourceIndex, destinationIndex);
      setColumns(nextSortedColumns);
    }
  }, [sortedColumns, setColumns]);
  var numberOfHiddenFields = availableColumns.length - visibleColumns.length;

  var _useState3 = (0, _react.useState)(''),
      _useState4 = (0, _slicedToArray2.default)(_useState3, 2),
      columnSearchText = _useState4[0],
      setColumnSearchText = _useState4[1];

  var controlBtnClasses = (0, _classnames.default)('euiDataGrid__controlBtn', {
    'euiDataGrid__controlBtn--active': numberOfHiddenFields > 0
  });
  var filteredColumns = (0, _react.useMemo)(function () {
    return sortedColumns.filter(function (id) {
      return (displayValues[id] || id).toLowerCase().indexOf(columnSearchText.toLowerCase()) !== -1;
    });
  }, [sortedColumns, columnSearchText, displayValues]);
  var isDragEnabled = allowColumnReorder && columnSearchText.length === 0; // only allow drag-and-drop when not filtering columns

  var buttonText = (0, _react2.jsx)(_i18n.EuiI18n, {
    token: "euiColumnSelector.button",
    default: "Columns"
  });

  if (numberOfHiddenFields === 1) {
    buttonText = (0, _react2.jsx)(_i18n.EuiI18n, {
      token: "euiColumnSelector.buttonActiveSingular",
      default: "{numberOfHiddenFields} column hidden",
      values: {
        numberOfHiddenFields: numberOfHiddenFields
      }
    });
  } else if (numberOfHiddenFields > 1) {
    buttonText = (0, _react2.jsx)(_i18n.EuiI18n, {
      token: "euiColumnSelector.buttonActivePlural",
      default: "{numberOfHiddenFields} columns hidden",
      values: {
        numberOfHiddenFields: numberOfHiddenFields
      }
    });
  }

  var columnSelector = allowColumnHiding || allowColumnReorder ? (0, _react2.jsx)(_popover.EuiPopover, {
    "data-test-subj": "dataGridColumnSelectorPopover",
    isOpen: isOpen,
    closePopover: function closePopover() {
      return setIsOpen(false);
    },
    anchorPosition: "downLeft",
    panelPaddingSize: "s",
    panelClassName: "euiDataGrid__controlPopoverWithDragDrop",
    button: (0, _react2.jsx)(_button.EuiButtonEmpty, {
      size: "xs",
      iconType: allowColumnHiding ? 'listAdd' : 'list',
      color: "text",
      className: controlBtnClasses,
      "data-test-subj": "dataGridColumnSelectorButton",
      onClick: function onClick() {
        return setIsOpen(!isOpen);
      }
    }, buttonText)
  }, (0, _react2.jsx)("div", null, allowColumnHiding && (0, _react2.jsx)(_popover.EuiPopoverTitle, null, (0, _react2.jsx)(_i18n.EuiI18n, {
    tokens: ['euiColumnSelector.search', 'euiColumnSelector.searchcolumns'],
    defaults: ['Search', 'Search columns']
  }, function (_ref3) {
    var _ref4 = (0, _slicedToArray2.default)(_ref3, 2),
        search = _ref4[0],
        searchcolumns = _ref4[1];

    return (0, _react2.jsx)(_form.EuiFieldText, {
      compressed: true,
      placeholder: search,
      "aria-label": searchcolumns,
      value: columnSearchText,
      onChange: function onChange(e) {
        return setColumnSearchText(e.currentTarget.value);
      },
      "data-test-subj": "dataGridColumnSelectorSearch"
    });
  })), (0, _react2.jsx)("div", {
    className: "euiDataGrid__controlScroll"
  }, (0, _react2.jsx)(_drag_and_drop.EuiDragDropContext, {
    onDragEnd: onDragEnd
  }, (0, _react2.jsx)(_drag_and_drop.EuiDroppable, {
    droppableId: "columnOrder",
    isDropDisabled: !isDragEnabled
  }, (0, _react2.jsx)(_react.Fragment, null, filteredColumns.map(function (id, index) {
    return (0, _react2.jsx)(_drag_and_drop.EuiDraggable, {
      key: id,
      draggableId: id,
      index: index,
      isDragDisabled: !isDragEnabled
    }, function (provided, state) {
      return (0, _react2.jsx)("div", {
        className: "euiDataGridColumnSelector__item ".concat(state.isDragging && 'euiDataGridColumnSelector__item-isDragging'),
        "data-test-subj": "dataGridColumnSelectorColumnItem-".concat(id)
      }, (0, _react2.jsx)(_flex.EuiFlexGroup, {
        responsive: false,
        gutterSize: "m",
        alignItems: "center"
      }, (0, _react2.jsx)(_flex.EuiFlexItem, null, allowColumnHiding ? (0, _react2.jsx)(_form.EuiSwitch, {
        name: id,
        label: displayValues[id] || id,
        checked: visibleColumnIds.has(id),
        compressed: true,
        className: "euiSwitch--mini",
        onChange: function onChange(event) {
          var checked = event.target.checked;
          var nextVisibleColumns = sortedColumns.filter(function (columnId) {
            return checked ? visibleColumnIds.has(columnId) || id === columnId : visibleColumnIds.has(columnId) && id !== columnId;
          });
          setVisibleColumns(nextVisibleColumns);
        },
        "data-test-subj": "dataGridColumnSelectorToggleColumnVisibility-".concat(id)
      }) : (0, _react2.jsx)("span", {
        className: "euiDataGridColumnSelector__itemLabel"
      }, id)), isDragEnabled && (0, _react2.jsx)(_flex.EuiFlexItem, {
        grow: false
      }, (0, _react2.jsx)(_icon.EuiIcon, {
        type: "grab",
        color: "subdued"
      }))));
    });
  })))))), allowColumnHiding && (0, _react2.jsx)(_popover.EuiPopoverFooter, null, (0, _react2.jsx)(_flex.EuiFlexGroup, {
    gutterSize: "s",
    responsive: false,
    justifyContent: "spaceBetween"
  }, (0, _react2.jsx)(_flex.EuiFlexItem, {
    grow: false
  }, (0, _react2.jsx)(_button.EuiButtonEmpty, {
    size: "xs",
    flush: "left",
    onClick: function onClick() {
      return setVisibleColumns(sortedColumns);
    },
    "data-test-subj": "dataGridColumnSelectorShowAllButton"
  }, (0, _react2.jsx)(_i18n.EuiI18n, {
    token: "euiColumnSelector.selectAll",
    default: "Show all"
  }))), (0, _react2.jsx)(_flex.EuiFlexItem, {
    grow: false
  }, (0, _react2.jsx)(_button.EuiButtonEmpty, {
    size: "xs",
    flush: "right",
    onClick: function onClick() {
      return setVisibleColumns([]);
    },
    "data-test-subj": "dataGridColumnSelectorHideAllButton"
  }, (0, _react2.jsx)(_i18n.EuiI18n, {
    token: "euiColumnSelector.hideAll",
    default: "Hide all"
  })))))) : null;
  var orderedVisibleColumns = (0, _react.useMemo)(function () {
    return visibleColumns.map(function (columnId) {
      return availableColumns.find(function (_ref5) {
        var id = _ref5.id;
        return id === columnId;
      });
    } // cast to avoid `undefined`, it filters those out next
    ).filter(function (column) {
      return column != null;
    });
  }, [availableColumns, visibleColumns]);
  /**
   * Used for moving columns left/right, available in the headers actions menu
   */

  var switchColumnPos = (0, _react.useCallback)(function (fromColId, toColId) {
    var moveFromIdx = sortedColumns.indexOf(fromColId);
    var moveToIdx = sortedColumns.indexOf(toColId);

    if (moveFromIdx === -1 || moveToIdx === -1) {
      return;
    }

    var nextSortedColumns = (0, _toConsumableArray2.default)(sortedColumns);
    nextSortedColumns.splice(moveFromIdx, 1);
    nextSortedColumns.splice(moveToIdx, 0, fromColId);
    setColumns(nextSortedColumns);
  }, [setColumns, sortedColumns]);
  return [columnSelector, orderedVisibleColumns, setVisibleColumns, switchColumnPos];
};

exports.useDataGridColumnSelector = useDataGridColumnSelector;