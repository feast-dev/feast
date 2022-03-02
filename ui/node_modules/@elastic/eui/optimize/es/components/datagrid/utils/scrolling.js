import _regeneratorRuntime from "@babel/runtime/regenerator";
import _asyncToGenerator from "@babel/runtime/helpers/asyncToGenerator";

/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */
import { useContext, useEffect, useCallback } from 'react';
import { DataGridFocusContext } from './focus';

/**
 * The primary goal of this scroll logic is to ensure keyboard navigation works accessibly,
 * but there are other scenarios where it applies (e.g. clicking partially-visible cells)
 * or is useful for (e.g. manually scrolling to cell that is currently out of viewport
 * while accounting for headers/footers/scrollbars)
 */
export var useScroll = function useScroll(args) {
  var _useScrollCellIntoVie = useScrollCellIntoView(args),
      scrollCellIntoView = _useScrollCellIntoVie.scrollCellIntoView;

  var _useContext = useContext(DataGridFocusContext),
      focusedCell = _useContext.focusedCell;

  useEffect(function () {
    if (focusedCell) {
      scrollCellIntoView({
        rowIndex: focusedCell[1],
        colIndex: focusedCell[0]
      });
    }
  }, [focusedCell, scrollCellIntoView]);
  return {
    scrollCellIntoView: scrollCellIntoView
  };
};
/**
 * Ensures that the passed cell is always fully in view by using cell position
 * checks and scroll adjustments/workarounds.
 */

export var useScrollCellIntoView = function useScrollCellIntoView(_ref) {
  var gridRef = _ref.gridRef,
      outerGridRef = _ref.outerGridRef,
      innerGridRef = _ref.innerGridRef,
      headerRowHeight = _ref.headerRowHeight,
      footerRowHeight = _ref.footerRowHeight,
      visibleRowCount = _ref.visibleRowCount,
      hasStickyFooter = _ref.hasStickyFooter;
  var scrollCellIntoView = useCallback( /*#__PURE__*/function () {
    var _ref3 = _asyncToGenerator( /*#__PURE__*/_regeneratorRuntime.mark(function _callee(_ref2) {
      var _adjustedScrollLeft;

      var rowIndex, colIndex, gridDoesNotScroll, getCell, cell, cellIsInView, _outerGridRef$current, scrollTop, scrollLeft, adjustedScrollTop, adjustedScrollLeft, cellRightPos, rightScrollBound, rightWidthOutOfView, cellLeftPos, leftScrollBound, leftWidthOutOfView, isStickyHeader, isStickyFooter, _adjustedScrollTop, cellBottomPos, bottomScrollBound, bottomHeightOutOfView, cellTopPos, topScrollBound, topHeightOutOfView, _adjustedScrollLeft2, _adjustedScrollTop2;

      return _regeneratorRuntime.wrap(function _callee$(_context) {
        while (1) {
          switch (_context.prev = _context.next) {
            case 0:
              rowIndex = _ref2.rowIndex, colIndex = _ref2.colIndex;

              if (!(!gridRef.current || !outerGridRef.current || !innerGridRef.current)) {
                _context.next = 3;
                break;
              }

              return _context.abrupt("return");

            case 3:
              gridDoesNotScroll = innerGridRef.current.offsetHeight === outerGridRef.current.offsetHeight && innerGridRef.current.offsetWidth === outerGridRef.current.offsetWidth;

              if (!gridDoesNotScroll) {
                _context.next = 6;
                break;
              }

              return _context.abrupt("return");

            case 6:
              // Obtain the outermost wrapper of the current cell in view in order to
              // get scroll position/height/width calculations and determine what level
              // of scroll adjustment the cell needs
              getCell = function getCell() {
                return outerGridRef.current.querySelector("[data-gridcell-column-index=\"".concat(colIndex, "\"][data-gridcell-visible-row-index=\"").concat(rowIndex, "\"]"));
              };

              cell = getCell(); // If the cell is completely out of view, we need to use react-window's
              // scrollToItem API to get it virtualized and rendered.

              cellIsInView = !!getCell();

              if (cellIsInView) {
                _context.next = 14;
                break;
              }

              gridRef.current.scrollToItem({
                rowIndex: rowIndex,
                columnIndex: colIndex
              });
              _context.next = 13;
              return new Promise(requestAnimationFrame);

            case 13:
              // The cell does not immediately render - we need to wait an async tick
              cell = getCell();

            case 14:
              if (cell) {
                _context.next = 16;
                break;
              }

              return _context.abrupt("return");

            case 16:
              // If for some reason we can't find a valid cell, short-circuit
              // We now manually adjust scroll positioning around the cell to ensure it's
              // fully in view on all sides. A couple of notes on this:
              // 1. We're avoiding relying on react-window's scrollToItem for this because it also
              //    does not account for sticky items (see https://github.com/bvaughn/react-window/issues/586)
              // 2. The current scroll position we're using as a base comes from either by
              //    `scrollToItem` or native .focus()'s automatic scroll behavior. This gets us
              //    halfway there, but doesn't guarantee the *full* cell in view, or account for
              //    sticky positioned rows or OS scrollbars, hence these workarounds
              _outerGridRef$current = outerGridRef.current, scrollTop = _outerGridRef$current.scrollTop, scrollLeft = _outerGridRef$current.scrollLeft;
              // Check if the cell's right side is outside the current scrolling bounds
              cellRightPos = cell.offsetLeft + cell.offsetWidth;
              rightScrollBound = scrollLeft + outerGridRef.current.clientWidth; // Note: We specifically want clientWidth and not offsetWidth here to account for scrollbars

              rightWidthOutOfView = cellRightPos - rightScrollBound;

              if (rightWidthOutOfView > 0) {
                adjustedScrollLeft = scrollLeft + rightWidthOutOfView;
              } // Check if the cell's left side is outside the current scrolling bounds


              cellLeftPos = cell.offsetLeft;
              leftScrollBound = (_adjustedScrollLeft = adjustedScrollLeft) !== null && _adjustedScrollLeft !== void 0 ? _adjustedScrollLeft : scrollLeft;
              leftWidthOutOfView = leftScrollBound - cellLeftPos;

              if (leftWidthOutOfView > 0) {
                // Note: This overrides the right side being out of bounds, as we want to prefer
                // showing the top-left corner of items if a cell is larger than the grid container
                adjustedScrollLeft = cellLeftPos;
              } // Skip top/bottom scroll adjustments for sticky headers & footers
              // since they should always be in view vertically


              isStickyHeader = rowIndex === -1;
              isStickyFooter = hasStickyFooter && rowIndex === visibleRowCount;

              if (!isStickyHeader && !isStickyFooter) {
                // Check if the cell's bottom side is outside the current scrolling bounds
                cellBottomPos = cell.offsetTop + cell.offsetHeight;
                bottomScrollBound = scrollTop + outerGridRef.current.clientHeight; // Note: We specifically want clientHeight and not offsetHeight here to account for scrollbars

                if (hasStickyFooter) bottomScrollBound -= footerRowHeight; // Sticky footer is not always present

                bottomHeightOutOfView = cellBottomPos - bottomScrollBound;

                if (bottomHeightOutOfView > 0) {
                  adjustedScrollTop = scrollTop + bottomHeightOutOfView;
                } // Check if the cell's top side is outside the current scrolling bounds


                cellTopPos = cell.offsetTop;
                topScrollBound = (_adjustedScrollTop = adjustedScrollTop) !== null && _adjustedScrollTop !== void 0 ? _adjustedScrollTop : scrollTop + headerRowHeight; // Sticky header is always present

                topHeightOutOfView = topScrollBound - cellTopPos;

                if (topHeightOutOfView > 0) {
                  // Note: This overrides the bottom side being out of bounds, as we want to prefer
                  // showing the top-left corner of items if a cell is larger than the grid container
                  adjustedScrollTop = cellTopPos - headerRowHeight;
                }
              } // Check for undefined specifically (because 0 is a valid scroll position)
              // to avoid unnecessarily calling scrollTo or hijacking scroll


              if (adjustedScrollTop !== undefined || adjustedScrollLeft !== undefined) {
                gridRef.current.scrollTo({
                  scrollLeft: (_adjustedScrollLeft2 = adjustedScrollLeft) !== null && _adjustedScrollLeft2 !== void 0 ? _adjustedScrollLeft2 : scrollLeft,
                  scrollTop: (_adjustedScrollTop2 = adjustedScrollTop) !== null && _adjustedScrollTop2 !== void 0 ? _adjustedScrollTop2 : scrollTop
                });
              }

            case 29:
            case "end":
              return _context.stop();
          }
        }
      }, _callee);
    }));

    return function (_x) {
      return _ref3.apply(this, arguments);
    };
  }(), [gridRef, outerGridRef, innerGridRef, headerRowHeight, footerRowHeight, visibleRowCount, hasStickyFooter]);
  return {
    scrollCellIntoView: scrollCellIntoView
  };
};