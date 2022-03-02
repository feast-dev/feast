import _toConsumableArray from "@babel/runtime/helpers/toConsumableArray";
import _extends from "@babel/runtime/helpers/extends";

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