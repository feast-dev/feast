"use strict";

var _interopRequireWildcard = require("@babel/runtime/helpers/interopRequireWildcard");

var _interopRequireDefault = require("@babel/runtime/helpers/interopRequireDefault");

Object.defineProperty(exports, "__esModule", {
  value: true
});
exports.EuiDataGridCellButtons = void 0;

var _toConsumableArray2 = _interopRequireDefault(require("@babel/runtime/helpers/toConsumableArray"));

var _extends2 = _interopRequireDefault(require("@babel/runtime/helpers/extends"));

var _react = _interopRequireWildcard(require("react"));

var _classnames = _interopRequireDefault(require("classnames"));

var _i18n = require("../../i18n");

var _button_icon = require("../../button/button_icon");

var _react2 = require("@emotion/react");

/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */
var EuiDataGridCellButtons = function EuiDataGridCellButtons(_ref) {
  var popoverIsOpen = _ref.popoverIsOpen,
      closePopover = _ref.closePopover,
      onExpandClick = _ref.onExpandClick,
      column = _ref.column,
      rowIndex = _ref.rowIndex;
  var buttonIconClasses = (0, _classnames.default)('euiDataGridRowCell__expandButtonIcon', {
    'euiDataGridRowCell__expandButtonIcon-isActive': popoverIsOpen
  });
  var buttonClasses = (0, _classnames.default)('euiDataGridRowCell__expandButton', {
    'euiDataGridRowCell__expandButton-isActive': popoverIsOpen
  });
  var expandButton = (0, _react2.jsx)(_i18n.EuiI18n, {
    key: 'expand',
    token: "euiDataGridCellButtons.expandButtonTitle",
    default: "Click or hit enter to interact with cell content"
  }, function (expandButtonTitle) {
    return (0, _react2.jsx)(_button_icon.EuiButtonIcon, {
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
  var additionalButtons = (0, _react.useMemo)(function () {
    var ButtonComponent = function ButtonComponent(props) {
      return (0, _react2.jsx)(_button_icon.EuiButtonIcon, (0, _extends2.default)({}, props, {
        "aria-hidden": true,
        className: "euiDataGridRowCell__actionButtonIcon",
        iconSize: "s"
      }));
    };

    return column && Array.isArray(column.cellActions) ? column.cellActions.map(function (Action, idx) {
      // React is more permissible than the TS types indicate
      var CellButtonElement = Action;
      return (0, _react2.jsx)(CellButtonElement, {
        key: idx,
        rowIndex: rowIndex,
        columnId: column.id,
        Component: ButtonComponent,
        isExpanded: false,
        closePopover: closePopover
      });
    }) : [];
  }, [column, rowIndex, closePopover]);
  return (0, _react2.jsx)("div", {
    className: buttonClasses
  }, [].concat((0, _toConsumableArray2.default)(additionalButtons), [expandButton]));
};

exports.EuiDataGridCellButtons = EuiDataGridCellButtons;