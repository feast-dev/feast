function _toConsumableArray(arr) { return _arrayWithoutHoles(arr) || _iterableToArray(arr) || _unsupportedIterableToArray(arr) || _nonIterableSpread(); }

function _nonIterableSpread() { throw new TypeError("Invalid attempt to spread non-iterable instance.\nIn order to be iterable, non-array objects must have a [Symbol.iterator]() method."); }

function _unsupportedIterableToArray(o, minLen) { if (!o) return; if (typeof o === "string") return _arrayLikeToArray(o, minLen); var n = Object.prototype.toString.call(o).slice(8, -1); if (n === "Object" && o.constructor) n = o.constructor.name; if (n === "Map" || n === "Set") return Array.from(o); if (n === "Arguments" || /^(?:Ui|I)nt(?:8|16|32)(?:Clamped)?Array$/.test(n)) return _arrayLikeToArray(o, minLen); }

function _iterableToArray(iter) { if (typeof Symbol !== "undefined" && Symbol.iterator in Object(iter)) return Array.from(iter); }

function _arrayWithoutHoles(arr) { if (Array.isArray(arr)) return _arrayLikeToArray(arr); }

function _arrayLikeToArray(arr, len) { if (len == null || len > arr.length) len = arr.length; for (var i = 0, arr2 = new Array(len); i < len; i++) { arr2[i] = arr[i]; } return arr2; }

function _extends() { _extends = Object.assign || function (target) { for (var i = 1; i < arguments.length; i++) { var source = arguments[i]; for (var key in source) { if (Object.prototype.hasOwnProperty.call(source, key)) { target[key] = source[key]; } } } return target; }; return _extends.apply(this, arguments); }

/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */
import React, { useMemo } from 'react';
import classNames from 'classnames';
import { EuiI18n } from '../../i18n';
import { EuiButtonIcon } from '../../button/button_icon';
import { jsx as ___EmotionJSX } from "@emotion/react";
export var EuiDataGridCellButtons = function EuiDataGridCellButtons(_ref) {
  var popoverIsOpen = _ref.popoverIsOpen,
      closePopover = _ref.closePopover,
      onExpandClick = _ref.onExpandClick,
      column = _ref.column,
      rowIndex = _ref.rowIndex;
  var buttonIconClasses = classNames('euiDataGridRowCell__expandButtonIcon', {
    'euiDataGridRowCell__expandButtonIcon-isActive': popoverIsOpen
  });
  var buttonClasses = classNames('euiDataGridRowCell__expandButton', {
    'euiDataGridRowCell__expandButton-isActive': popoverIsOpen
  });

  var expandButton = ___EmotionJSX(EuiI18n, {
    key: 'expand',
    token: "euiDataGridCellButtons.expandButtonTitle",
    default: "Click or hit enter to interact with cell content"
  }, function (expandButtonTitle) {
    return ___EmotionJSX(EuiButtonIcon, {
      display: "fill",
      className: buttonIconClasses,
      color: "primary",
      iconSize: "s",
      iconType: "expandMini",
      "aria-hidden": true,
      onClick: onExpandClick,
      title: expandButtonTitle
    });
  });

  var additionalButtons = useMemo(function () {
    var ButtonComponent = function ButtonComponent(props) {
      return ___EmotionJSX(EuiButtonIcon, _extends({}, props, {
        "aria-hidden": true,
        className: "euiDataGridRowCell__actionButtonIcon",
        iconSize: "s"
      }));
    };

    return column && Array.isArray(column.cellActions) ? column.cellActions.map(function (Action, idx) {
      // React is more permissible than the TS types indicate
      var CellButtonElement = Action;
      return ___EmotionJSX(CellButtonElement, {
        key: idx,
        rowIndex: rowIndex,
        columnId: column.id,
        Component: ButtonComponent,
        isExpanded: false,
        closePopover: closePopover
      });
    }) : [];
  }, [column, rowIndex, closePopover]);
  return ___EmotionJSX("div", {
    className: buttonClasses
  }, [].concat(_toConsumableArray(additionalButtons), [expandButton]));
};