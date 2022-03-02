"use strict";

var _interopRequireWildcard = require("@babel/runtime/helpers/interopRequireWildcard");

var _interopRequireDefault = require("@babel/runtime/helpers/interopRequireDefault");

Object.defineProperty(exports, "__esModule", {
  value: true
});
exports.useDataGridDisplaySelector = exports.startingStyles = void 0;

var _objectWithoutProperties2 = _interopRequireDefault(require("@babel/runtime/helpers/objectWithoutProperties"));

var _defineProperty2 = _interopRequireDefault(require("@babel/runtime/helpers/defineProperty"));

var _slicedToArray2 = _interopRequireDefault(require("@babel/runtime/helpers/slicedToArray"));

var _typeof2 = _interopRequireDefault(require("@babel/runtime/helpers/typeof"));

var _react = _interopRequireWildcard(require("react"));

var _services = require("../../../services");

var _i18n = require("../../i18n");

var _popover = require("../../popover");

var _button = require("../../button");

var _form = require("../../form");

var _flex = require("../../flex");

var _tool_tip = require("../../tool_tip");

var _data_grid_toolbar = require("./data_grid_toolbar");

var _react2 = require("@emotion/react");

function ownKeys(object, enumerableOnly) { var keys = Object.keys(object); if (Object.getOwnPropertySymbols) { var symbols = Object.getOwnPropertySymbols(object); if (enumerableOnly) symbols = symbols.filter(function (sym) { return Object.getOwnPropertyDescriptor(object, sym).enumerable; }); keys.push.apply(keys, symbols); } return keys; }

function _objectSpread(target) { for (var i = 1; i < arguments.length; i++) { var source = arguments[i] != null ? arguments[i] : {}; if (i % 2) { ownKeys(Object(source), true).forEach(function (key) { (0, _defineProperty2.default)(target, key, source[key]); }); } else if (Object.getOwnPropertyDescriptors) { Object.defineProperties(target, Object.getOwnPropertyDescriptors(source)); } else { ownKeys(Object(source)).forEach(function (key) { Object.defineProperty(target, key, Object.getOwnPropertyDescriptor(source, key)); }); } } return target; }

var startingStyles = {
  cellPadding: 'm',
  fontSize: 'm',
  border: 'all',
  stripes: false,
  rowHover: 'highlight',
  header: 'shade',
  footer: 'overline',
  stickyFooter: true
}; // These are the available options. They power the gridDensity hook and also the options in the render

exports.startingStyles = startingStyles;
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

  if ((0, _typeof2.default)(defaultHeight) === 'object' && (defaultHeight === null || defaultHeight === void 0 ? void 0 : defaultHeight.lineCount)) {
    return rowHeightButtonOptions[2];
  }

  if (typeof defaultHeight === 'number' || (0, _typeof2.default)(defaultHeight) === 'object' && defaultHeight.height) {
    return '';
  }

  return rowHeightButtonOptions[0];
};

var useDataGridDisplaySelector = function useDataGridDisplaySelector(showDisplaySelector, initialStyles, initialRowHeightsOptions) {
  var _rowHeightsOptions$de2;

  var _useState = (0, _react.useState)(false),
      _useState2 = (0, _slicedToArray2.default)(_useState, 2),
      isOpen = _useState2[0],
      setIsOpen = _useState2[1];

  var showDensityControls = (0, _data_grid_toolbar.getNestedObjectOptions)(showDisplaySelector, 'allowDensity');
  var showRowHeightControls = (0, _data_grid_toolbar.getNestedObjectOptions)(showDisplaySelector, 'allowRowHeight'); // Track styles specified by the user at run time

  var _useState3 = (0, _react.useState)({}),
      _useState4 = (0, _slicedToArray2.default)(_useState3, 2),
      userGridStyles = _useState4[0],
      setUserGridStyles = _useState4[1];

  var _useState5 = (0, _react.useState)({}),
      _useState6 = (0, _slicedToArray2.default)(_useState5, 2),
      userRowHeightsOptions = _useState6[0],
      setUserRowHeightsOptions = _useState6[1]; // Density logic


  var setGridStyles = (0, _react.useCallback)(function (density) {
    setUserGridStyles(densityStyles[density]);
  }, []); // Row height logic

  var _useState7 = (0, _react.useState)(2),
      _useState8 = (0, _slicedToArray2.default)(_useState7, 2),
      lineCount = _useState8[0],
      setLineCount = _useState8[1];

  var setRowHeight = (0, _react.useCallback)(function (option) {
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
  var setLineCountHeight = (0, _react.useCallback)(function (event) {
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

  var gridStyles = (0, _react.useMemo)(function () {
    return _objectSpread(_objectSpread({}, initialStyles), userGridStyles);
  }, [initialStyles, userGridStyles]);
  var rowHeightsOptions = (0, _react.useMemo)(function () {
    return _objectSpread(_objectSpread({}, initialRowHeightsOptions), userRowHeightsOptions);
  }, [initialRowHeightsOptions, userRowHeightsOptions]); // Set UI controls based on current configurations, on init & when either developer or user settings change

  var gridDensity = (0, _react.useMemo)(function () {
    return convertGridStylesToSelection(gridStyles);
  }, [gridStyles]);
  var rowHeightSelection = (0, _react.useMemo)(function () {
    return convertRowHeightsOptionsToSelection(rowHeightsOptions);
  }, [rowHeightsOptions]);
  (0, _react.useEffect)(function () {
    var _rowHeightsOptions$de;

    // @ts-ignore - optional chaining operator handles types & cases that aren't lineCount
    setLineCount((rowHeightsOptions === null || rowHeightsOptions === void 0 ? void 0 : (_rowHeightsOptions$de = rowHeightsOptions.defaultHeight) === null || _rowHeightsOptions$de === void 0 ? void 0 : _rowHeightsOptions$de.lineCount) || 2); // @ts-ignore - same as above
  }, [rowHeightsOptions === null || rowHeightsOptions === void 0 ? void 0 : (_rowHeightsOptions$de2 = rowHeightsOptions.defaultHeight) === null || _rowHeightsOptions$de2 === void 0 ? void 0 : _rowHeightsOptions$de2.lineCount]); // Show a reset button whenever users manually change settings, and
  // invoke onChange callbacks (removing the callback value itself, so that only configuration values are returned)

  var _useState9 = (0, _react.useState)(false),
      _useState10 = (0, _slicedToArray2.default)(_useState9, 2),
      showResetButton = _useState10[0],
      setShowResetButton = _useState10[1];

  (0, _services.useUpdateEffect)(function () {
    var _initialStyles$onChan;

    var hasUserChanges = Object.keys(userGridStyles).length > 0;
    if (hasUserChanges) setShowResetButton(true);
    var onChange = gridStyles.onChange,
        currentGridStyles = (0, _objectWithoutProperties2.default)(gridStyles, ["onChange"]);
    initialStyles === null || initialStyles === void 0 ? void 0 : (_initialStyles$onChan = initialStyles.onChange) === null || _initialStyles$onChan === void 0 ? void 0 : _initialStyles$onChan.call(initialStyles, currentGridStyles);
  }, [userGridStyles]);
  (0, _services.useUpdateEffect)(function () {
    var _initialRowHeightsOpt;

    var hasUserChanges = Object.keys(userRowHeightsOptions).length > 0;
    if (hasUserChanges) setShowResetButton(true);
    var onChange = rowHeightsOptions.onChange,
        currentRowHeightsOptions = (0, _objectWithoutProperties2.default)(rowHeightsOptions, ["onChange"]);
    initialRowHeightsOptions === null || initialRowHeightsOptions === void 0 ? void 0 : (_initialRowHeightsOpt = initialRowHeightsOptions.onChange) === null || _initialRowHeightsOpt === void 0 ? void 0 : _initialRowHeightsOpt.call(initialRowHeightsOptions, currentRowHeightsOptions);
  }, [userRowHeightsOptions]); // Allow resetting to initial developer-specified configurations

  var resetToInitialState = (0, _react.useCallback)(function () {
    setUserGridStyles({});
    setUserRowHeightsOptions({});
    setShowResetButton(false);
  }, []);
  var buttonLabel = (0, _i18n.useEuiI18n)('euiDisplaySelector.buttonText', 'Display options');
  var resetButtonLabel = (0, _i18n.useEuiI18n)('euiDisplaySelector.resetButtonText', 'Reset to default');
  var displaySelector = showDensityControls || showRowHeightControls ? (0, _react2.jsx)(_popover.EuiPopover, {
    "data-test-subj": "dataGridDisplaySelectorPopover",
    isOpen: isOpen,
    closePopover: function closePopover() {
      return setIsOpen(false);
    },
    anchorPosition: "downRight",
    panelPaddingSize: "s",
    panelClassName: "euiDataGrid__displayPopoverPanel",
    button: (0, _react2.jsx)(_tool_tip.EuiToolTip, {
      content: buttonLabel,
      delay: "long"
    }, (0, _react2.jsx)(_button.EuiButtonIcon, {
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
  }, showDensityControls && (0, _react2.jsx)(_i18n.EuiI18n, {
    tokens: ['euiDisplaySelector.densityLabel', 'euiDisplaySelector.labelCompact', 'euiDisplaySelector.labelNormal', 'euiDisplaySelector.labelExpanded'],
    defaults: ['Density', 'Compact', 'Normal', 'Expanded']
  }, function (_ref) {
    var _ref2 = (0, _slicedToArray2.default)(_ref, 4),
        densityLabel = _ref2[0],
        labelCompact = _ref2[1],
        labelNormal = _ref2[2],
        labelExpanded = _ref2[3];

    return (0, _react2.jsx)(_form.EuiFormRow, {
      label: densityLabel,
      display: "columnCompressed"
    }, (0, _react2.jsx)(_button.EuiButtonGroup, {
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
  }), showRowHeightControls && (0, _react2.jsx)(_i18n.EuiI18n, {
    tokens: ['euiDisplaySelector.rowHeightLabel', 'euiDisplaySelector.labelSingle', 'euiDisplaySelector.labelAuto', 'euiDisplaySelector.labelCustom', 'euiDisplaySelector.lineCountLabel'],
    defaults: ['Row height', 'Single', 'Auto fit', 'Custom', 'Lines per row']
  }, function (_ref3) {
    var _ref4 = (0, _slicedToArray2.default)(_ref3, 5),
        rowHeightLabel = _ref4[0],
        labelSingle = _ref4[1],
        labelAuto = _ref4[2],
        labelCustom = _ref4[3],
        lineCountLabel = _ref4[4];

    return (0, _react2.jsx)(_react.default.Fragment, null, (0, _react2.jsx)(_form.EuiFormRow, {
      label: rowHeightLabel,
      display: "columnCompressed"
    }, (0, _react2.jsx)(_button.EuiButtonGroup, {
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
    })), rowHeightSelection === rowHeightButtonOptions[2] && (0, _react2.jsx)(_form.EuiFormRow, {
      label: lineCountLabel,
      display: "columnCompressed"
    }, (0, _react2.jsx)(_form.EuiRange, {
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
  }), showResetButton && (0, _react2.jsx)(_popover.EuiPopoverFooter, null, (0, _react2.jsx)(_flex.EuiFlexGroup, {
    justifyContent: "flexEnd",
    responsive: false
  }, (0, _react2.jsx)(_flex.EuiFlexItem, {
    grow: false
  }, (0, _react2.jsx)("div", null, (0, _react2.jsx)(_button.EuiButtonEmpty, {
    flush: "both",
    size: "xs",
    onClick: resetToInitialState,
    "data-test-subj": "resetDisplaySelector"
  }, resetButtonLabel)))))) : null;
  return [displaySelector, gridStyles, rowHeightsOptions];
};

exports.useDataGridDisplaySelector = useDataGridDisplaySelector;