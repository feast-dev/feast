function _extends() { _extends = Object.assign || function (target) { for (var i = 1; i < arguments.length; i++) { var source = arguments[i]; for (var key in source) { if (Object.prototype.hasOwnProperty.call(source, key)) { target[key] = source[key]; } } } return target; }; return _extends.apply(this, arguments); }

function _slicedToArray(arr, i) { return _arrayWithHoles(arr) || _iterableToArrayLimit(arr, i) || _unsupportedIterableToArray(arr, i) || _nonIterableRest(); }

function _nonIterableRest() { throw new TypeError("Invalid attempt to destructure non-iterable instance.\nIn order to be iterable, non-array objects must have a [Symbol.iterator]() method."); }

function _unsupportedIterableToArray(o, minLen) { if (!o) return; if (typeof o === "string") return _arrayLikeToArray(o, minLen); var n = Object.prototype.toString.call(o).slice(8, -1); if (n === "Object" && o.constructor) n = o.constructor.name; if (n === "Map" || n === "Set") return Array.from(o); if (n === "Arguments" || /^(?:Ui|I)nt(?:8|16|32)(?:Clamped)?Array$/.test(n)) return _arrayLikeToArray(o, minLen); }

function _arrayLikeToArray(arr, len) { if (len == null || len > arr.length) len = arr.length; for (var i = 0, arr2 = new Array(len); i < len; i++) { arr2[i] = arr[i]; } return arr2; }

function _iterableToArrayLimit(arr, i) { if (typeof Symbol === "undefined" || !(Symbol.iterator in Object(arr))) return; var _arr = []; var _n = true; var _d = false; var _e = undefined; try { for (var _i = arr[Symbol.iterator](), _s; !(_n = (_s = _i.next()).done); _n = true) { _arr.push(_s.value); if (i && _arr.length === i) break; } } catch (err) { _d = true; _e = err; } finally { try { if (!_n && _i["return"] != null) _i["return"](); } finally { if (_d) throw _e; } } return _arr; }

function _arrayWithHoles(arr) { if (Array.isArray(arr)) return arr; }

function _objectWithoutProperties(source, excluded) { if (source == null) return {}; var target = _objectWithoutPropertiesLoose(source, excluded); var key, i; if (Object.getOwnPropertySymbols) { var sourceSymbolKeys = Object.getOwnPropertySymbols(source); for (i = 0; i < sourceSymbolKeys.length; i++) { key = sourceSymbolKeys[i]; if (excluded.indexOf(key) >= 0) continue; if (!Object.prototype.propertyIsEnumerable.call(source, key)) continue; target[key] = source[key]; } } return target; }

function _objectWithoutPropertiesLoose(source, excluded) { if (source == null) return {}; var target = {}; var sourceKeys = Object.keys(source); var key, i; for (i = 0; i < sourceKeys.length; i++) { key = sourceKeys[i]; if (excluded.indexOf(key) >= 0) continue; target[key] = source[key]; } return target; }

/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */
import classnames from 'classnames';
import PropTypes from "prop-types";
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

EuiDataGridHeaderCellWrapper.propTypes = {
  id: PropTypes.string.isRequired,
  index: PropTypes.number.isRequired,
  headerIsInteractive: PropTypes.bool.isRequired,
  width: PropTypes.oneOfType([PropTypes.number.isRequired, PropTypes.oneOf([null])]),
  className: PropTypes.string
};

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