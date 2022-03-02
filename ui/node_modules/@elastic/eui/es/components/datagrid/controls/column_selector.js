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
import React, { Fragment, useState, useMemo, useCallback } from 'react';
import classNames from 'classnames';
import { EuiPopover, EuiPopoverFooter, EuiPopoverTitle } from '../../popover';
import { EuiI18n } from '../../i18n';
import { EuiButtonEmpty } from '../../button';
import { EuiFlexGroup, EuiFlexItem } from '../../flex';
import { EuiSwitch, EuiFieldText } from '../../form';
import { EuiDragDropContext, EuiDraggable, EuiDroppable, euiDragDropReorder } from '../../drag_and_drop';
import { EuiIcon } from '../../icon';
import { useDependentState } from '../../../services';
import { getNestedObjectOptions } from './data_grid_toolbar';
import { jsx as ___EmotionJSX } from "@emotion/react";
export var useDataGridColumnSelector = function useDataGridColumnSelector(availableColumns, columnVisibility, showColumnSelector, displayValues) {
  var allowColumnHiding = getNestedObjectOptions(showColumnSelector, 'allowHide');
  var allowColumnReorder = getNestedObjectOptions(showColumnSelector, 'allowReorder');

  var _useDependentState = useDependentState(function () {
    return availableColumns.map(function (_ref) {
      var id = _ref.id;
      return id;
    });
  }, [availableColumns]),
      _useDependentState2 = _slicedToArray(_useDependentState, 2),
      sortedColumns = _useDependentState2[0],
      setSortedColumns = _useDependentState2[1];

  var visibleColumns = columnVisibility.visibleColumns,
      setVisibleColumns = columnVisibility.setVisibleColumns;
  var visibleColumnIds = useMemo(function () {
    return new Set(visibleColumns);
  }, [visibleColumns]);

  var _useState = useState(false),
      _useState2 = _slicedToArray(_useState, 2),
      isOpen = _useState2[0],
      setIsOpen = _useState2[1];

  var setColumns = useCallback(function (nextColumns) {
    setSortedColumns(nextColumns);
    var nextVisibleColumns = nextColumns.filter(function (id) {
      return visibleColumnIds.has(id);
    });
    setVisibleColumns(nextVisibleColumns);
  }, [setSortedColumns, setVisibleColumns, visibleColumnIds]);
  var onDragEnd = useCallback(function (_ref2) {
    var sourceIndex = _ref2.source.index,
        destination = _ref2.destination;

    if (destination) {
      var destinationIndex = destination.index;
      var nextSortedColumns = euiDragDropReorder(sortedColumns, sourceIndex, destinationIndex);
      setColumns(nextSortedColumns);
    }
  }, [sortedColumns, setColumns]);
  var numberOfHiddenFields = availableColumns.length - visibleColumns.length;

  var _useState3 = useState(''),
      _useState4 = _slicedToArray(_useState3, 2),
      columnSearchText = _useState4[0],
      setColumnSearchText = _useState4[1];

  var controlBtnClasses = classNames('euiDataGrid__controlBtn', {
    'euiDataGrid__controlBtn--active': numberOfHiddenFields > 0
  });
  var filteredColumns = useMemo(function () {
    return sortedColumns.filter(function (id) {
      return (displayValues[id] || id).toLowerCase().indexOf(columnSearchText.toLowerCase()) !== -1;
    });
  }, [sortedColumns, columnSearchText, displayValues]);
  var isDragEnabled = allowColumnReorder && columnSearchText.length === 0; // only allow drag-and-drop when not filtering columns

  var buttonText = ___EmotionJSX(EuiI18n, {
    token: "euiColumnSelector.button",
    default: "Columns"
  });

  if (numberOfHiddenFields === 1) {
    buttonText = ___EmotionJSX(EuiI18n, {
      token: "euiColumnSelector.buttonActiveSingular",
      default: "{numberOfHiddenFields} column hidden",
      values: {
        numberOfHiddenFields: numberOfHiddenFields
      }
    });
  } else if (numberOfHiddenFields > 1) {
    buttonText = ___EmotionJSX(EuiI18n, {
      token: "euiColumnSelector.buttonActivePlural",
      default: "{numberOfHiddenFields} columns hidden",
      values: {
        numberOfHiddenFields: numberOfHiddenFields
      }
    });
  }

  var columnSelector = allowColumnHiding || allowColumnReorder ? ___EmotionJSX(EuiPopover, {
    "data-test-subj": "dataGridColumnSelectorPopover",
    isOpen: isOpen,
    closePopover: function closePopover() {
      return setIsOpen(false);
    },
    anchorPosition: "downLeft",
    panelPaddingSize: "s",
    panelClassName: "euiDataGrid__controlPopoverWithDragDrop",
    button: ___EmotionJSX(EuiButtonEmpty, {
      size: "xs",
      iconType: allowColumnHiding ? 'listAdd' : 'list',
      color: "text",
      className: controlBtnClasses,
      "data-test-subj": "dataGridColumnSelectorButton",
      onClick: function onClick() {
        return setIsOpen(!isOpen);
      }
    }, buttonText)
  }, ___EmotionJSX("div", null, allowColumnHiding && ___EmotionJSX(EuiPopoverTitle, null, ___EmotionJSX(EuiI18n, {
    tokens: ['euiColumnSelector.search', 'euiColumnSelector.searchcolumns'],
    defaults: ['Search', 'Search columns']
  }, function (_ref3) {
    var _ref4 = _slicedToArray(_ref3, 2),
        search = _ref4[0],
        searchcolumns = _ref4[1];

    return ___EmotionJSX(EuiFieldText, {
      compressed: true,
      placeholder: search,
      "aria-label": searchcolumns,
      value: columnSearchText,
      onChange: function onChange(e) {
        return setColumnSearchText(e.currentTarget.value);
      },
      "data-test-subj": "dataGridColumnSelectorSearch"
    });
  })), ___EmotionJSX("div", {
    className: "euiDataGrid__controlScroll"
  }, ___EmotionJSX(EuiDragDropContext, {
    onDragEnd: onDragEnd
  }, ___EmotionJSX(EuiDroppable, {
    droppableId: "columnOrder",
    isDropDisabled: !isDragEnabled
  }, ___EmotionJSX(Fragment, null, filteredColumns.map(function (id, index) {
    return ___EmotionJSX(EuiDraggable, {
      key: id,
      draggableId: id,
      index: index,
      isDragDisabled: !isDragEnabled
    }, function (provided, state) {
      return ___EmotionJSX("div", {
        className: "euiDataGridColumnSelector__item ".concat(state.isDragging && 'euiDataGridColumnSelector__item-isDragging'),
        "data-test-subj": "dataGridColumnSelectorColumnItem-".concat(id)
      }, ___EmotionJSX(EuiFlexGroup, {
        responsive: false,
        gutterSize: "m",
        alignItems: "center"
      }, ___EmotionJSX(EuiFlexItem, null, allowColumnHiding ? ___EmotionJSX(EuiSwitch, {
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
      }) : ___EmotionJSX("span", {
        className: "euiDataGridColumnSelector__itemLabel"
      }, id)), isDragEnabled && ___EmotionJSX(EuiFlexItem, {
        grow: false
      }, ___EmotionJSX(EuiIcon, {
        type: "grab",
        color: "subdued"
      }))));
    });
  })))))), allowColumnHiding && ___EmotionJSX(EuiPopoverFooter, null, ___EmotionJSX(EuiFlexGroup, {
    gutterSize: "s",
    responsive: false,
    justifyContent: "spaceBetween"
  }, ___EmotionJSX(EuiFlexItem, {
    grow: false
  }, ___EmotionJSX(EuiButtonEmpty, {
    size: "xs",
    flush: "left",
    onClick: function onClick() {
      return setVisibleColumns(sortedColumns);
    },
    "data-test-subj": "dataGridColumnSelectorShowAllButton"
  }, ___EmotionJSX(EuiI18n, {
    token: "euiColumnSelector.selectAll",
    default: "Show all"
  }))), ___EmotionJSX(EuiFlexItem, {
    grow: false
  }, ___EmotionJSX(EuiButtonEmpty, {
    size: "xs",
    flush: "right",
    onClick: function onClick() {
      return setVisibleColumns([]);
    },
    "data-test-subj": "dataGridColumnSelectorHideAllButton"
  }, ___EmotionJSX(EuiI18n, {
    token: "euiColumnSelector.hideAll",
    default: "Hide all"
  })))))) : null;
  var orderedVisibleColumns = useMemo(function () {
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

  var switchColumnPos = useCallback(function (fromColId, toColId) {
    var moveFromIdx = sortedColumns.indexOf(fromColId);
    var moveToIdx = sortedColumns.indexOf(toColId);

    if (moveFromIdx === -1 || moveToIdx === -1) {
      return;
    }

    var nextSortedColumns = _toConsumableArray(sortedColumns);

    nextSortedColumns.splice(moveFromIdx, 1);
    nextSortedColumns.splice(moveToIdx, 0, fromColId);
    setColumns(nextSortedColumns);
  }, [setColumns, sortedColumns]);
  return [columnSelector, orderedVisibleColumns, setVisibleColumns, switchColumnPos];
};