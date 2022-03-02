import _extends from "@babel/runtime/helpers/extends";
import _slicedToArray from "@babel/runtime/helpers/slicedToArray";
import _objectWithoutProperties from "@babel/runtime/helpers/objectWithoutProperties";

/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */
import classnames from 'classnames';
import React, { useContext, useEffect, useRef, useState } from 'react';
import tabbable from 'tabbable';
import { keys } from '../../../../services';
import { DataGridFocusContext } from '../../utils/focus';
import { jsx as ___EmotionJSX } from "@emotion/react";

/**
 * This is a wrapper that handles repeated concerns between control &
 * standard header cells. Most of its shared logic is around focus state/UX,
 * but it also DRY's out certain class/data-test-subj/style attributes
 */
export var EuiDataGridHeaderCellWrapper = function EuiDataGridHeaderCellWrapper(_ref) {
  var id = _ref.id,
      index = _ref.index,
      headerIsInteractive = _ref.headerIsInteractive,
      width = _ref.width,
      className = _ref.className,
      children = _ref.children,
      rest = _objectWithoutProperties(_ref, ["id", "index", "headerIsInteractive", "width", "className", "children"]);

  var classes = classnames('euiDataGridHeaderCell', className);

  var _useContext = useContext(DataGridFocusContext),
      setFocusedCell = _useContext.setFocusedCell,
      onFocusUpdate = _useContext.onFocusUpdate;

  var _useState = useState(false),
      _useState2 = _slicedToArray(_useState, 2),
      isFocused = _useState2[0],
      setIsFocused = _useState2[1];

  useEffect(function () {
    onFocusUpdate([index, -1], function (isFocused) {
      setIsFocused(isFocused);
    });
  }, [index, onFocusUpdate]);
  var headerRef = useRef(null);

  var _useState3 = useState(false),
      _useState4 = _slicedToArray(_useState3, 2),
      isCellEntered = _useState4[0],
      setIsCellEntered = _useState4[1];

  useEffect(function () {
    var headerNode = headerRef.current;

    if (isCellEntered) {
      enableAndFocusInteractives(headerNode);
    } else {
      disableInteractives(headerNode);
    }
  }, [isCellEntered]);
  useEffect(function () {
    var headerNode = headerRef.current;

    if (isFocused) {
      var interactives = headerNode.querySelectorAll('[data-euigrid-tab-managed]');

      if (interactives.length === 1) {
        setIsCellEntered(true);
      } else {
        headerNode.focus();
      }
    } else {
      setIsCellEntered(false);
    } // focusin bubbles while focus does not, and this needs to react to children gaining focus


    var onFocusIn = function onFocusIn(e) {
      if (!headerIsInteractive) {
        // header is not interactive, avoid focusing
        requestAnimationFrame(function () {
          return headerNode.blur();
        });
        e.preventDefault();
        return false;
      } else {
        // take the focus
        if (isFocused === false) {
          setFocusedCell([index, -1]);
        } else {
          // this cell already had the grid's focus, so re-enable and focus interactives
          setIsCellEntered(true);
        }
      }
    }; // focusout bubbles while blur does not, and this needs to react to the children losing focus


    var onFocusOut = function onFocusOut() {
      // wait for the next element to receive focus, then update interactives' state
      requestAnimationFrame(function () {
        if (!headerNode.contains(document.activeElement)) {
          setIsCellEntered(false);
        }
      });
    };

    var onKeyUp = function onKeyUp(event) {
      switch (event.key) {
        case keys.ENTER:
          {
            event.preventDefault();
            setIsCellEntered(true);
            break;
          }

        case keys.ESCAPE:
          {
            event.preventDefault(); // move focus to cell

            setIsCellEntered(false);
            headerNode.focus();
            break;
          }
      }
    };

    headerNode.addEventListener('focusin', onFocusIn);
    headerNode.addEventListener('focusout', onFocusOut);
    headerNode.addEventListener('keyup', onKeyUp);
    return function () {
      headerNode.removeEventListener('focusin', onFocusIn);
      headerNode.removeEventListener('focusout', onFocusOut);
      headerNode.removeEventListener('keyup', onKeyUp);
    };
  }, [headerIsInteractive, isFocused, index, setFocusedCell]);
  return ___EmotionJSX("div", _extends({
    role: "columnheader",
    ref: headerRef,
    tabIndex: isFocused && !isCellEntered ? 0 : -1,
    className: classes,
    "data-test-subj": "dataGridHeaderCell-".concat(id),
    "data-gridcell-column-id": id,
    "data-gridcell-column-index": index,
    "data-gridcell-row-index": "-1",
    "data-gridcell-visible-row-index": "-1",
    style: width != null ? {
      width: "".concat(width, "px")
    } : {}
  }, rest), children);
};
/**
 * Utility fns for managing child interactive tabIndex state
 */

var disableInteractives = function disableInteractives(headerNode) {
  var tabbables = tabbable(headerNode);

  if (tabbables.length > 1) {
    console.warn("EuiDataGridHeaderCell expects at most 1 tabbable element, ".concat(tabbables.length, " found instead"));
  }

  tabbables.forEach(function (element) {
    element.setAttribute('data-euigrid-tab-managed', 'true');
    element.setAttribute('tabIndex', '-1');
  });
};

var enableAndFocusInteractives = function enableAndFocusInteractives(headerNode) {
  var interactiveElements = headerNode.querySelectorAll('[data-euigrid-tab-managed]');
  interactiveElements.forEach(function (element, i) {
    element.setAttribute('tabIndex', '0');

    if (i === 0) {
      element.focus();
    }
  });
};