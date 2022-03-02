function _toConsumableArray(arr) { return _arrayWithoutHoles(arr) || _iterableToArray(arr) || _unsupportedIterableToArray(arr) || _nonIterableSpread(); }

function _nonIterableSpread() { throw new TypeError("Invalid attempt to spread non-iterable instance.\nIn order to be iterable, non-array objects must have a [Symbol.iterator]() method."); }

function _iterableToArray(iter) { if (typeof Symbol !== "undefined" && Symbol.iterator in Object(iter)) return Array.from(iter); }

function _arrayWithoutHoles(arr) { if (Array.isArray(arr)) return _arrayLikeToArray(arr); }

function _slicedToArray(arr, i) { return _arrayWithHoles(arr) || _iterableToArrayLimit(arr, i) || _unsupportedIterableToArray(arr, i) || _nonIterableRest(); }

function _nonIterableRest() { throw new TypeError("Invalid attempt to destructure non-iterable instance.\nIn order to be iterable, non-array objects must have a [Symbol.iterator]() method."); }

function _unsupportedIterableToArray(o, minLen) { if (!o) return; if (typeof o === "string") return _arrayLikeToArray(o, minLen); var n = Object.prototype.toString.call(o).slice(8, -1); if (n === "Object" && o.constructor) n = o.constructor.name; if (n === "Map" || n === "Set") return Array.from(o); if (n === "Arguments" || /^(?:Ui|I)nt(?:8|16|32)(?:Clamped)?Array$/.test(n)) return _arrayLikeToArray(o, minLen); }

function _arrayLikeToArray(arr, len) { if (len == null || len > arr.length) len = arr.length; for (var i = 0, arr2 = new Array(len); i < len; i++) { arr2[i] = arr[i]; } return arr2; }

function _iterableToArrayLimit(arr, i) { if (typeof Symbol === "undefined" || !(Symbol.iterator in Object(arr))) return; var _arr = []; var _n = true; var _d = false; var _e = undefined; try { for (var _i = arr[Symbol.iterator](), _s; !(_n = (_s = _i.next()).done); _n = true) { _arr.push(_s.value); if (i && _arr.length === i) break; } } catch (err) { _d = true; _e = err; } finally { try { if (!_n && _i["return"] != null) _i["return"](); } finally { if (_d) throw _e; } } return _arr; }

function _arrayWithHoles(arr) { if (Array.isArray(arr)) return arr; }

/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */
import classNames from 'classnames';
import React, { Fragment, useEffect, useState } from 'react';
import { EuiButtonEmpty } from '../../button';
import { EuiDragDropContext, euiDragDropReorder, EuiDroppable } from '../../drag_and_drop';
import { EuiFlexGroup, EuiFlexItem } from '../../flex';
import { EuiI18n, useEuiI18n } from '../../i18n';
import { EuiPopover, EuiPopoverFooter } from '../../popover';
import { EuiText } from '../../text';
import { EuiToken } from '../../token';
import { EuiDataGridColumnSortingDraggable } from './column_sorting_draggable';
import { getDetailsForSchema } from '../utils/data_grid_schema';
import { jsx as ___EmotionJSX } from "@emotion/react";
export var useDataGridColumnSorting = function useDataGridColumnSorting(columns, sorting, schema, schemaDetectors, displayValues) {
  var _useState = useState(false),
      _useState2 = _slicedToArray(_useState, 2),
      isOpen = _useState2[0],
      setIsOpen = _useState2[1];

  var _useState3 = useState(false),
      _useState4 = _slicedToArray(_useState3, 2),
      availableColumnsIsOpen = _useState4[0],
      setAvailableColumnsIsOpen = _useState4[1]; // prune any non-existent/hidden columns from sorting


  useEffect(function () {
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
  var sortingButtonText = useEuiI18n('euiColumnSorting.button', 'Sort fields');
  var sortingButtonTextActive = useEuiI18n('euiColumnSorting.buttonActive', function (_ref2) {
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
      var nextColumns = euiDragDropReorder(sorting.columns, sourceIndex, destinationIndex);
      sorting.onSort(nextColumns);
    }
  };

  var controlBtnClasses = classNames('euiDataGrid__controlBtn', {
    'euiDataGrid__controlBtn--active': sorting.columns.length > 0
  });

  var schemaDetails = function schemaDetails(id) {
    return schema.hasOwnProperty(id) && schema[id].columnType != null ? getDetailsForSchema(schemaDetectors, schema[id].columnType) : null;
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

  var columnSorting = ___EmotionJSX(EuiPopover, {
    "data-test-subj": "dataGridColumnSortingPopover",
    isOpen: isOpen,
    closePopover: function closePopover() {
      return setIsOpen(false);
    },
    anchorPosition: "downLeft",
    panelPaddingSize: "s",
    panelClassName: "euiDataGrid__controlPopoverWithDragDrop",
    button: ___EmotionJSX(EuiButtonEmpty, {
      size: "xs",
      iconType: "sortable",
      color: "text",
      className: controlBtnClasses,
      "data-test-subj": "dataGridColumnSortingButton",
      onClick: function onClick() {
        return setIsOpen(!isOpen);
      }
    }, sorting.columns.length > 0 ? sortingButtonTextActive : sortingButtonText)
  }, sorting.columns.length > 0 ? ___EmotionJSX("div", {
    role: "region",
    "aria-live": "assertive",
    className: "euiDataGrid__controlScroll"
  }, ___EmotionJSX(EuiDragDropContext, {
    onDragEnd: onDragEnd
  }, ___EmotionJSX(EuiDroppable, {
    droppableId: "columnSorting"
  }, ___EmotionJSX(Fragment, null, sorting.columns.map(function (_ref6, index) {
    var id = _ref6.id,
        direction = _ref6.direction;
    return ___EmotionJSX(EuiDataGridColumnSortingDraggable, {
      key: id,
      id: id,
      display: displayValues[id],
      direction: direction,
      index: index,
      sorting: sorting,
      schema: schema,
      schemaDetectors: schemaDetectors
    });
  }))))) : ___EmotionJSX(EuiText, {
    size: "s",
    color: "subdued"
  }, ___EmotionJSX("p", {
    role: "alert"
  }, ___EmotionJSX(EuiI18n, {
    token: "euiColumnSorting.emptySorting",
    default: "Currently no fields are sorted"
  }))), (inactiveSortableColumns.length > 0 || sorting.columns.length > 0) && ___EmotionJSX(EuiPopoverFooter, null, ___EmotionJSX(EuiFlexGroup, {
    gutterSize: "m",
    justifyContent: "spaceBetween",
    responsive: false
  }, ___EmotionJSX(EuiFlexItem, {
    grow: false
  }, inactiveSortableColumns.length > 0 && ___EmotionJSX(EuiPopover, {
    "data-test-subj": "dataGridColumnSortingPopoverColumnSelection",
    isOpen: availableColumnsIsOpen,
    closePopover: function closePopover() {
      return setAvailableColumnsIsOpen(false);
    },
    anchorPosition: "downLeft",
    panelPaddingSize: "none",
    button: ___EmotionJSX(EuiButtonEmpty, {
      size: "xs",
      flush: "left",
      iconType: "arrowDown",
      iconSide: "right",
      "data-test-subj": "dataGridColumnSortingSelectionButton",
      onClick: function onClick() {
        return setAvailableColumnsIsOpen(!availableColumnsIsOpen);
      }
    }, ___EmotionJSX(EuiI18n, {
      token: "euiColumnSorting.pickFields",
      default: "Pick fields to sort by"
    }))
  }, ___EmotionJSX(EuiI18n, {
    token: "euiColumnSorting.sortFieldAriaLabel",
    default: "Sort by: "
  }, function (sortFieldAriaLabel) {
    return ___EmotionJSX("div", {
      className: "euiDataGridColumnSorting__fieldList",
      role: "listbox"
    }, inactiveSortableColumns.map(function (_ref7) {
      var id = _ref7.id,
          defaultSortDirection = _ref7.defaultSortDirection;
      return ___EmotionJSX("button", {
        key: id,
        className: "euiDataGridColumnSorting__field",
        "aria-label": "".concat(sortFieldAriaLabel, " ").concat(id),
        role: "option",
        "aria-selected": "false",
        "data-test-subj": "dataGridColumnSortingPopoverColumnSelection-".concat(id),
        onClick: function onClick() {
          var _schemaDetails;

          var nextColumns = _toConsumableArray(sorting.columns);

          nextColumns.push({
            id: id,
            direction: defaultSortDirection || ((_schemaDetails = schemaDetails(id)) === null || _schemaDetails === void 0 ? void 0 : _schemaDetails.defaultSortDirection) || 'asc'
          });
          sorting.onSort(nextColumns);
        }
      }, ___EmotionJSX(EuiFlexGroup, {
        alignItems: "center",
        gutterSize: "s",
        component: "span",
        responsive: false
      }, ___EmotionJSX(EuiFlexItem, {
        grow: false
      }, ___EmotionJSX(EuiToken, {
        iconType: schemaDetails(id) != null ? getDetailsForSchema(schemaDetectors, schema[id].columnType).icon : 'tokenString',
        color: schemaDetails(id) != null ? getDetailsForSchema(schemaDetectors, schema[id].columnType).color : undefined
      })), ___EmotionJSX(EuiFlexItem, {
        grow: false
      }, ___EmotionJSX(EuiText, {
        size: "xs"
      }, displayValues[id]))));
    }));
  }))), sorting.columns.length > 0 ? ___EmotionJSX(EuiFlexItem, {
    grow: false
  }, ___EmotionJSX(EuiButtonEmpty, {
    size: "xs",
    flush: "right",
    onClick: function onClick() {
      return sorting.onSort([]);
    },
    "data-test-subj": "dataGridColumnSortingClearButton"
  }, ___EmotionJSX(EuiI18n, {
    token: "euiColumnSorting.clearAll",
    default: "Clear sorting"
  }))) : null)));

  return columnSorting;
};