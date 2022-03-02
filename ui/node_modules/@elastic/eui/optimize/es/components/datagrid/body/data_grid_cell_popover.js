import _extends from "@babel/runtime/helpers/extends";

/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */
import React from 'react';
import { keys } from '../../../services';
import { EuiButtonEmpty } from '../../button/button_empty';
import { EuiFlexGroup, EuiFlexItem } from '../../flex';
import { EuiPopover, EuiPopoverFooter } from '../../popover';
import { jsx as ___EmotionJSX } from "@emotion/react";
export function EuiDataGridCellPopover(_ref) {
  var anchorContent = _ref.anchorContent,
      cellContentProps = _ref.cellContentProps,
      cellContentsRef = _ref.cellContentsRef,
      closePopover = _ref.closePopover,
      column = _ref.column,
      panelRefFn = _ref.panelRefFn,
      PopoverContent = _ref.popoverContent,
      popoverIsOpen = _ref.popoverIsOpen,
      renderCellValue = _ref.renderCellValue,
      rowIndex = _ref.rowIndex;
  var CellElement = renderCellValue;
  return ___EmotionJSX(EuiPopover, {
    hasArrow: false,
    anchorClassName: "euiDataGridRowCell__expand",
    button: anchorContent,
    isOpen: popoverIsOpen,
    panelRef: panelRefFn,
    panelClassName: "euiDataGridRowCell__popover",
    panelPaddingSize: "s",
    display: "block",
    closePopover: closePopover,
    panelProps: {
      'data-test-subj': 'euiDataGridExpansionPopover'
    },
    onKeyDown: function onKeyDown(event) {
      if (event.key === keys.F2 || event.key === keys.ESCAPE) {
        event.preventDefault();
        event.stopPropagation();
        closePopover();
      }
    }
  }, popoverIsOpen ? ___EmotionJSX(React.Fragment, null, ___EmotionJSX(PopoverContent, {
    cellContentsElement: cellContentsRef
  }, ___EmotionJSX(CellElement, _extends({}, cellContentProps, {
    isDetails: true
  }))), column && column.cellActions && column.cellActions.length ? ___EmotionJSX(EuiPopoverFooter, null, ___EmotionJSX(EuiFlexGroup, {
    gutterSize: "s"
  }, column.cellActions.map(function (Action, idx) {
    var CellButtonElement = Action;
    return ___EmotionJSX(EuiFlexItem, {
      key: idx
    }, ___EmotionJSX(CellButtonElement, {
      rowIndex: rowIndex,
      columnId: column.id,
      Component: function Component(props) {
        return ___EmotionJSX(EuiButtonEmpty, _extends({}, props, {
          size: "s"
        }));
      },
      isExpanded: true,
      closePopover: closePopover
    }));
  }))) : null) : null);
}