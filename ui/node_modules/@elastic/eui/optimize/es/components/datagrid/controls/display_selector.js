import _objectWithoutProperties from "@babel/runtime/helpers/objectWithoutProperties";
import _defineProperty from "@babel/runtime/helpers/defineProperty";
import _slicedToArray from "@babel/runtime/helpers/slicedToArray";
import _typeof from "@babel/runtime/helpers/typeof";

function ownKeys(object, enumerableOnly) { var keys = Object.keys(object); if (Object.getOwnPropertySymbols) { var symbols = Object.getOwnPropertySymbols(object); if (enumerableOnly) symbols = symbols.filter(function (sym) { return Object.getOwnPropertyDescriptor(object, sym).enumerable; }); keys.push.apply(keys, symbols); } return keys; }

function _objectSpread(target) { for (var i = 1; i < arguments.length; i++) { var source = arguments[i] != null ? arguments[i] : {}; if (i % 2) { ownKeys(Object(source), true).forEach(function (key) { _defineProperty(target, key, source[key]); }); } else if (Object.getOwnPropertyDescriptors) { Object.defineProperties(target, Object.getOwnPropertyDescriptors(source)); } else { ownKeys(Object(source)).forEach(function (key) { Object.defineProperty(target, key, Object.getOwnPropertyDescriptor(source, key)); }); } } return target; }

/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */
import React, { useState, useMemo, useCallback, useEffect } from 'react';
import { useUpdateEffect } from '../../../services';
import { EuiI18n, useEuiI18n } from '../../i18n';
import { EuiPopover, EuiPopoverFooter } from '../../popover';
import { EuiButtonIcon, EuiButtonGroup, EuiButtonEmpty } from '../../button';
import { EuiFormRow, EuiRange } from '../../form';
import { EuiFlexGroup, EuiFlexItem } from '../../flex';
import { EuiToolTip } from '../../tool_tip';
import { getNestedObjectOptions } from './data_grid_toolbar';
import { jsx as ___EmotionJSX } from "@emotion/react";
export var startingStyles = {
  cellPadding: 'm',
  fontSize: 'm',
  border: 'all',
  stripes: false,
  rowHover: 'highlight',
  header: 'shade',
  footer: 'overline',
  stickyFooter: true
}; // These are the available options. They power the gridDensity hook and also the options in the render

var densityOptions = ['compact', 'normal', 'expanded'];
var densityStyles = {
  expanded: {
    fontSize: 'l',
    cellPadding: 'l'
  },
  normal: {
    fontSize: 'm',
    cellPadding: 'm'
  },
  compact: {
    fontSize: 's',
    cellPadding: 's'
  }
};

var convertGridStylesToSelection = function convertGridStylesToSelection(gridStyles) {
  if (gridStyles.fontSize === 's' && gridStyles.cellPadding === 's') return 'compact';
  if (gridStyles.fontSize === 'm' && gridStyles.cellPadding === 'm') return 'normal';
  if (gridStyles.fontSize === 'l' && gridStyles.cellPadding === 'l') return 'expanded';
  return '';
}; // Used to correctly format the icon name for the grid density icon


var capitalizeDensityString = function capitalizeDensityString(s) {
  return s[0].toUpperCase() + s.slice(1);
}; // Row height options and utilities


var rowHeightButtonOptions = ['undefined', 'auto', 'lineCount'];

var convertRowHeightsOptionsToSelection = function convertRowHeightsOptionsToSelection(rowHeightsOptions) {
  var defaultHeight = rowHeightsOptions.defaultHeight;

  if (defaultHeight === 'auto') {
    return rowHeightButtonOptions[1];
  }

  if (_typeof(defaultHeight) === 'object' && (defaultHeight === null || defaultHeight === void 0 ? void 0 : defaultHeight.lineCount)) {
    return rowHeightButtonOptions[2];
  }

  if (typeof defaultHeight === 'number' || _typeof(defaultHeight) === 'object' && defaultHeight.height) {
    return '';
  }

  return rowHeightButtonOptions[0];
};

export var useDataGridDisplaySelector = function useDataGridDisplaySelector(showDisplaySelector, initialStyles, initialRowHeightsOptions) {
  var _rowHeightsOptions$de2;

  var _useState = useState(false),
      _useState2 = _slicedToArray(_useState, 2),
      isOpen = _useState2[0],
      setIsOpen = _useState2[1];

  var showDensityControls = getNestedObjectOptions(showDisplaySelector, 'allowDensity');
  var showRowHeightControls = getNestedObjectOptions(showDisplaySelector, 'allowRowHeight'); // Track styles specified by the user at run time

  var _useState3 = useState({}),
      _useState4 = _slicedToArray(_useState3, 2),
      userGridStyles = _useState4[0],
      setUserGridStyles = _useState4[1];

  var _useState5 = useState({}),
      _useState6 = _slicedToArray(_useState5, 2),
      userRowHeightsOptions = _useState6[0],
      setUserRowHeightsOptions = _useState6[1]; // Density logic


  var setGridStyles = useCallback(function (density) {
    setUserGridStyles(densityStyles[density]);
  }, []); // Row height logic

  var _useState7 = useState(2),
      _useState8 = _slicedToArray(_useState7, 2),
      lineCount = _useState8[0],
      setLineCount = _useState8[1];

  var setRowHeight = useCallback(function (option) {
    var rowHeightsOptions = {
      rowHeights: {} // Unset all row-specific heights

    };

    if (option === 'auto') {
      rowHeightsOptions.defaultHeight = 'auto';
    } else if (option === 'lineCount') {
      rowHeightsOptions.defaultHeight = {
        lineCount: lineCount
      };
    } else {
      rowHeightsOptions.defaultHeight = undefined;
    }

    setUserRowHeightsOptions(rowHeightsOptions);
  }, [lineCount]);
  var setLineCountHeight = useCallback(function (event) {
    var newLineCount = Number(event.target.value);
    if (newLineCount < 1) return; // Don't let users set a 0 or negative line count

    setLineCount(newLineCount);
    setUserRowHeightsOptions({
      rowHeights: {},
      // Unset all row-specific line counts
      defaultHeight: {
        lineCount: newLineCount
      }
    });
  }, []); // Merge the developer-specified configurations with user overrides

  var gridStyles = useMemo(function () {
    return _objectSpread(_objectSpread({}, initialStyles), userGridStyles);
  }, [initialStyles, userGridStyles]);
  var rowHeightsOptions = useMemo(function () {
    return _objectSpread(_objectSpread({}, initialRowHeightsOptions), userRowHeightsOptions);
  }, [initialRowHeightsOptions, userRowHeightsOptions]); // Set UI controls based on current configurations, on init & when either developer or user settings change

  var gridDensity = useMemo(function () {
    return convertGridStylesToSelection(gridStyles);
  }, [gridStyles]);
  var rowHeightSelection = useMemo(function () {
    return convertRowHeightsOptionsToSelection(rowHeightsOptions);
  }, [rowHeightsOptions]);
  useEffect(function () {
    var _rowHeightsOptions$de;

    // @ts-ignore - optional chaining operator handles types & cases that aren't lineCount
    setLineCount((rowHeightsOptions === null || rowHeightsOptions === void 0 ? void 0 : (_rowHeightsOptions$de = rowHeightsOptions.defaultHeight) === null || _rowHeightsOptions$de === void 0 ? void 0 : _rowHeightsOptions$de.lineCount) || 2); // @ts-ignore - same as above
  }, [rowHeightsOptions === null || rowHeightsOptions === void 0 ? void 0 : (_rowHeightsOptions$de2 = rowHeightsOptions.defaultHeight) === null || _rowHeightsOptions$de2 === void 0 ? void 0 : _rowHeightsOptions$de2.lineCount]); // Show a reset button whenever users manually change settings, and
  // invoke onChange callbacks (removing the callback value itself, so that only configuration values are returned)

  var _useState9 = useState(false),
      _useState10 = _slicedToArray(_useState9, 2),
      showResetButton = _useState10[0],
      setShowResetButton = _useState10[1];

  useUpdateEffect(function () {
    var _initialStyles$onChan;

    var hasUserChanges = Object.keys(userGridStyles).length > 0;
    if (hasUserChanges) setShowResetButton(true);

    var onChange = gridStyles.onChange,
        currentGridStyles = _objectWithoutProperties(gridStyles, ["onChange"]);

    initialStyles === null || initialStyles === void 0 ? void 0 : (_initialStyles$onChan = initialStyles.onChange) === null || _initialStyles$onChan === void 0 ? void 0 : _initialStyles$onChan.call(initialStyles, currentGridStyles);
  }, [userGridStyles]);
  useUpdateEffect(function () {
    var _initialRowHeightsOpt;

    var hasUserChanges = Object.keys(userRowHeightsOptions).length > 0;
    if (hasUserChanges) setShowResetButton(true);

    var onChange = rowHeightsOptions.onChange,
        currentRowHeightsOptions = _objectWithoutProperties(rowHeightsOptions, ["onChange"]);

    initialRowHeightsOptions === null || initialRowHeightsOptions === void 0 ? void 0 : (_initialRowHeightsOpt = initialRowHeightsOptions.onChange) === null || _initialRowHeightsOpt === void 0 ? void 0 : _initialRowHeightsOpt.call(initialRowHeightsOptions, currentRowHeightsOptions);
  }, [userRowHeightsOptions]); // Allow resetting to initial developer-specified configurations

  var resetToInitialState = useCallback(function () {
    setUserGridStyles({});
    setUserRowHeightsOptions({});
    setShowResetButton(false);
  }, []);
  var buttonLabel = useEuiI18n('euiDisplaySelector.buttonText', 'Display options');
  var resetButtonLabel = useEuiI18n('euiDisplaySelector.resetButtonText', 'Reset to default');
  var displaySelector = showDensityControls || showRowHeightControls ? ___EmotionJSX(EuiPopover, {
    "data-test-subj": "dataGridDisplaySelectorPopover",
    isOpen: isOpen,
    closePopover: function closePopover() {
      return setIsOpen(false);
    },
    anchorPosition: "downRight",
    panelPaddingSize: "s",
    panelClassName: "euiDataGrid__displayPopoverPanel",
    button: ___EmotionJSX(EuiToolTip, {
      content: buttonLabel,
      delay: "long"
    }, ___EmotionJSX(EuiButtonIcon, {
      size: "xs",
      iconType: gridDensity ? "tableDensity".concat(capitalizeDensityString(gridDensity)) : 'tableDensityNormal',
      className: "euiDataGrid__controlBtn",
      color: "text",
      "data-test-subj": "dataGridDisplaySelectorButton",
      onClick: function onClick() {
        return setIsOpen(!isOpen);
      },
      "aria-label": buttonLabel
    }))
  }, showDensityControls && ___EmotionJSX(EuiI18n, {
    tokens: ['euiDisplaySelector.densityLabel', 'euiDisplaySelector.labelCompact', 'euiDisplaySelector.labelNormal', 'euiDisplaySelector.labelExpanded'],
    defaults: ['Density', 'Compact', 'Normal', 'Expanded']
  }, function (_ref) {
    var _ref2 = _slicedToArray(_ref, 4),
        densityLabel = _ref2[0],
        labelCompact = _ref2[1],
        labelNormal = _ref2[2],
        labelExpanded = _ref2[3];

    return ___EmotionJSX(EuiFormRow, {
      label: densityLabel,
      display: "columnCompressed"
    }, ___EmotionJSX(EuiButtonGroup, {
      legend: densityLabel,
      buttonSize: "compressed",
      isFullWidth: true,
      options: [{
        id: densityOptions[0],
        label: labelCompact
      }, {
        id: densityOptions[1],
        label: labelNormal
      }, {
        id: densityOptions[2],
        label: labelExpanded
      }],
      onChange: setGridStyles,
      idSelected: gridDensity,
      "data-test-subj": "densityButtonGroup"
    }));
  }), showRowHeightControls && ___EmotionJSX(EuiI18n, {
    tokens: ['euiDisplaySelector.rowHeightLabel', 'euiDisplaySelector.labelSingle', 'euiDisplaySelector.labelAuto', 'euiDisplaySelector.labelCustom', 'euiDisplaySelector.lineCountLabel'],
    defaults: ['Row height', 'Single', 'Auto fit', 'Custom', 'Lines per row']
  }, function (_ref3) {
    var _ref4 = _slicedToArray(_ref3, 5),
        rowHeightLabel = _ref4[0],
        labelSingle = _ref4[1],
        labelAuto = _ref4[2],
        labelCustom = _ref4[3],
        lineCountLabel = _ref4[4];

    return ___EmotionJSX(React.Fragment, null, ___EmotionJSX(EuiFormRow, {
      label: rowHeightLabel,
      display: "columnCompressed"
    }, ___EmotionJSX(EuiButtonGroup, {
      legend: rowHeightLabel,
      buttonSize: "compressed",
      isFullWidth: true,
      options: [{
        id: rowHeightButtonOptions[0],
        label: labelSingle
      }, {
        id: rowHeightButtonOptions[1],
        label: labelAuto
      }, {
        id: rowHeightButtonOptions[2],
        label: labelCustom
      }],
      onChange: setRowHeight,
      idSelected: rowHeightSelection,
      "data-test-subj": "rowHeightButtonGroup"
    })), rowHeightSelection === rowHeightButtonOptions[2] && ___EmotionJSX(EuiFormRow, {
      label: lineCountLabel,
      display: "columnCompressed"
    }, ___EmotionJSX(EuiRange, {
      compressed: true,
      fullWidth: true,
      showInput: true,
      min: 1,
      max: 20,
      step: 1,
      value: lineCount,
      onChange: setLineCountHeight,
      "data-test-subj": "lineCountNumber"
    })));
  }), showResetButton && ___EmotionJSX(EuiPopoverFooter, null, ___EmotionJSX(EuiFlexGroup, {
    justifyContent: "flexEnd",
    responsive: false
  }, ___EmotionJSX(EuiFlexItem, {
    grow: false
  }, ___EmotionJSX("div", null, ___EmotionJSX(EuiButtonEmpty, {
    flush: "both",
    size: "xs",
    onClick: resetToInitialState,
    "data-test-subj": "resetDisplaySelector"
  }, resetButtonLabel)))))) : null;
  return [displaySelector, gridStyles, rowHeightsOptions];
};