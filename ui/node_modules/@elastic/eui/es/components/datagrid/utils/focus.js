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
import { createContext, useContext, useCallback, useEffect, useMemo, useRef, useState } from 'react';
import tabbable from 'tabbable';
import { keys } from '../../../services';
export var DataGridFocusContext = /*#__PURE__*/createContext({
  focusedCell: undefined,
  setFocusedCell: function setFocusedCell() {},
  setIsFocusedCellInView: function setIsFocusedCellInView() {},
  onFocusUpdate: function onFocusUpdate() {
    return function () {};
  },
  focusFirstVisibleInteractiveCell: function focusFirstVisibleInteractiveCell() {}
});

/**
 * Main focus context and overarching focus state management
 */
export var useFocus = function useFocus(_ref) {
  var headerIsInteractive = _ref.headerIsInteractive,
      gridItemsRendered = _ref.gridItemsRendered;
  // Maintain a map of focus cell state callbacks
  var cellsUpdateFocus = useRef(new Map());
  var onFocusUpdate = useCallback(function (cell, updateFocus) {
    var key = "".concat(cell[0], "-").concat(cell[1]);
    cellsUpdateFocus.current.set(key, updateFocus);
    return function () {
      cellsUpdateFocus.current.delete(key);
    };
  }, []); // Current focused cell

  var _useState = useState(false),
      _useState2 = _slicedToArray(_useState, 2),
      isFocusedCellInView = _useState2[0],
      setIsFocusedCellInView = _useState2[1];

  var _useState3 = useState(undefined),
      _useState4 = _slicedToArray(_useState3, 2),
      focusedCell = _useState4[0],
      _setFocusedCell = _useState4[1];

  var setFocusedCell = useCallback(function (focusedCell) {
    _setFocusedCell(focusedCell);

    setIsFocusedCellInView(true); // scrolling.ts ensures focused cells are fully in view
  }, []);
  var previousCell = useRef(undefined);
  useEffect(function () {
    if (previousCell.current) {
      notifyCellOfFocusState(cellsUpdateFocus.current, previousCell.current, false);
    }

    previousCell.current = focusedCell;

    if (focusedCell) {
      notifyCellOfFocusState(cellsUpdateFocus.current, focusedCell, true);
    }
  }, [cellsUpdateFocus, focusedCell]);
  var focusFirstVisibleInteractiveCell = useCallback(function () {
    if (headerIsInteractive) {
      // The header (rowIndex -1) is sticky and will always be in view
      setFocusedCell([0, -1]);
    } else if (gridItemsRendered.current) {
      var _gridItemsRendered$cu = gridItemsRendered.current,
          visibleColumnStartIndex = _gridItemsRendered$cu.visibleColumnStartIndex,
          visibleRowStartIndex = _gridItemsRendered$cu.visibleRowStartIndex;
      setFocusedCell([visibleColumnStartIndex, visibleRowStartIndex]);
    } else {// If the header is non-interactive and there are no rendered cells,
      // there's nothing to do - we might as well leave focus on the grid body wrapper
    }
  }, [setFocusedCell, headerIsInteractive, gridItemsRendered]);
  var focusProps = useMemo(function () {
    return isFocusedCellInView ? {
      // FireFox allows tabbing to a div that is scrollable, while Chrome does not
      tabIndex: -1
    } : {
      tabIndex: 0,
      onFocus: function onFocus(e) {
        // if e.target (the source element of the `focus event`
        // matches e.currentTarget (always the div with this onFocus listener)
        // then the user has focused directly on the data grid wrapper (almost definitely by tabbing)
        // so shift focus to the first visible and interactive cell within the grid
        if (e.target === e.currentTarget) {
          focusFirstVisibleInteractiveCell();
        }
      }
    };
  }, [isFocusedCellInView, focusFirstVisibleInteractiveCell]);
  return {
    onFocusUpdate: onFocusUpdate,
    focusedCell: focusedCell,
    setFocusedCell: setFocusedCell,
    setIsFocusedCellInView: setIsFocusedCellInView,
    focusFirstVisibleInteractiveCell: focusFirstVisibleInteractiveCell,
    focusProps: focusProps
  };
};
export var notifyCellOfFocusState = function notifyCellOfFocusState(cellsUpdateFocus, cell, isFocused) {
  var key = "".concat(cell[0], "-").concat(cell[1]);
  var onFocus = cellsUpdateFocus.get(key);

  if (onFocus) {
    onFocus(isFocused);
  }
};
/**
 * Keydown handler for connecting focus state with keyboard navigation
 */

export var createKeyDownHandler = function createKeyDownHandler(_ref2) {
  var gridElement = _ref2.gridElement,
      visibleColCount = _ref2.visibleColCount,
      visibleRowCount = _ref2.visibleRowCount,
      visibleRowStartIndex = _ref2.visibleRowStartIndex,
      rowCount = _ref2.rowCount,
      pagination = _ref2.pagination,
      hasFooter = _ref2.hasFooter,
      headerIsInteractive = _ref2.headerIsInteractive,
      focusContext = _ref2.focusContext;
  return function (event) {
    var focusedCell = focusContext.focusedCell,
        setFocusedCell = focusContext.setFocusedCell;
    if (focusedCell == null) return;

    if (gridElement == null || !gridElement.contains(document.activeElement)) {
      // if the `contentElement` does not contain the focused element, don't handle the event
      // this happens when React bubbles the key event through a portal
      return;
    }

    var _focusedCell = _slicedToArray(focusedCell, 2),
        x = _focusedCell[0],
        y = _focusedCell[1];

    var key = event.key,
        ctrlKey = event.ctrlKey;

    if (key === keys.ARROW_DOWN) {
      event.preventDefault();

      if (hasFooter ? y < visibleRowCount : y < visibleRowCount - 1) {
        if (y === -1) {
          // The header is sticky, so on scrolling virtualized grids, row 0 will not
          // always be rendered to navigate down to. We need to account for this by
          // sending the down arrow to the first visible/virtualized row instead
          setFocusedCell([x, visibleRowStartIndex]);
        } else {
          setFocusedCell([x, y + 1]);
        }
      }
    } else if (key === keys.ARROW_LEFT) {
      event.preventDefault();

      if (x > 0) {
        setFocusedCell([x - 1, y]);
      }
    } else if (key === keys.ARROW_UP) {
      event.preventDefault();
      var minimumIndex = headerIsInteractive ? -1 : 0;

      if (y > minimumIndex) {
        setFocusedCell([x, y - 1]);
      }
    } else if (key === keys.ARROW_RIGHT) {
      event.preventDefault();

      if (x < visibleColCount - 1) {
        setFocusedCell([x + 1, y]);
      }
    } else if (key === keys.PAGE_DOWN) {
      if (pagination) {
        event.preventDefault();
        var pageSize = pagination.pageSize;
        var pageCount = Math.ceil(rowCount / pageSize);
        var pageIndex = pagination.pageIndex;

        if (pageIndex < pageCount - 1) {
          pagination.onChangePage(pageIndex + 1);
        }

        setFocusedCell([focusedCell[0], 0]);
      }
    } else if (key === keys.PAGE_UP) {
      if (pagination) {
        event.preventDefault();
        var _pageIndex = pagination.pageIndex;

        if (_pageIndex > 0) {
          pagination.onChangePage(_pageIndex - 1);
        }

        setFocusedCell([focusedCell[0], pagination.pageSize - 1]);
      }
    } else if (key === (ctrlKey && keys.END)) {
      event.preventDefault();
      setFocusedCell([visibleColCount - 1, visibleRowCount - 1]);
    } else if (key === (ctrlKey && keys.HOME)) {
      event.preventDefault();
      setFocusedCell([0, 0]);
    } else if (key === keys.END) {
      event.preventDefault();
      setFocusedCell([visibleColCount - 1, y]);
    } else if (key === keys.HOME) {
      event.preventDefault();
      setFocusedCell([0, y]);
    }
  };
};
/**
 * Mutation observer for the grid body, which exists to pick up DOM changes
 * in cells and remove interactive elements from the page's tab index, as
 * we want to move between cells via arrow keys instead of tabbing.
 */

export var preventTabbing = function preventTabbing(records) {
  // multiple mutation records can implicate the same cell
  // so be sure to only check each cell once
  var processedCells = new Set();

  for (var i = 0; i < records.length; i++) {
    var record = records[i]; // find the cell content owning this mutation

    var cell = getParentCellContent(record.target);
    if (processedCells.has(cell)) continue;
    processedCells.add(cell);

    if (cell) {
      // if we found it, disable tabbable elements
      var tabbables = tabbable(cell);

      for (var _i2 = 0; _i2 < tabbables.length; _i2++) {
        var element = tabbables[_i2];

        if (element.getAttribute('role') !== 'gridcell' && !element.dataset['euigrid-tab-managed']) {
          element.setAttribute('tabIndex', '-1');
          element.setAttribute('data-datagrid-interactable', 'true');
        }
      }
    }
  }
}; // Starts with a Node or HTMLElement returned by a mutation record
// and search its ancestors for a div[data-datagrid-cellcontent], if any,
// which is a valid target for disabling tabbing within

export var getParentCellContent = function getParentCellContent(_element) {
  var element = _element.nodeType === document.ELEMENT_NODE ? _element : _element.parentElement;

  while (element && // we haven't walked off the document yet
  element.nodeName !== 'div' && // looking for a div
  !element.hasAttribute('data-datagrid-cellcontent') // that has data-datagrid-cellcontent
  ) {
    element = element.parentElement;
  }

  return element;
};
/**
 * Focus fixes & workarounds
 */
// If the focus is on the header, and the header is no longer interactive,
// move the focus down to the first row

export var useHeaderFocusWorkaround = function useHeaderFocusWorkaround(headerIsInteractive) {
  var _useContext = useContext(DataGridFocusContext),
      focusedCell = _useContext.focusedCell,
      setFocusedCell = _useContext.setFocusedCell;

  useEffect(function () {
    if (!headerIsInteractive && focusedCell && focusedCell[1] === -1) {
      setFocusedCell([focusedCell[0], 0]);
    }
  }, [headerIsInteractive, focusedCell, setFocusedCell]);
};