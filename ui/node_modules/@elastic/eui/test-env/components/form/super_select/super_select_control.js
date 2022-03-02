"use strict";

var _interopRequireWildcard = require("@babel/runtime/helpers/interopRequireWildcard");

var _interopRequireDefault = require("@babel/runtime/helpers/interopRequireDefault");

Object.defineProperty(exports, "__esModule", {
  value: true
});
exports.EuiSuperSelectControl = void 0;

var _extends2 = _interopRequireDefault(require("@babel/runtime/helpers/extends"));

var _objectWithoutProperties2 = _interopRequireDefault(require("@babel/runtime/helpers/objectWithoutProperties"));

var _react = _interopRequireWildcard(require("react"));

var _classnames = _interopRequireDefault(require("classnames"));

var _accessibility = require("../../accessibility");

var _form_control_layout = require("../form_control_layout");

var _i18n = require("../../i18n");

var _react2 = require("@emotion/react");

/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */
var EuiSuperSelectControl = function EuiSuperSelectControl(_ref) {
  var className = _ref.className,
      _ref$options = _ref.options,
      options = _ref$options === void 0 ? [] : _ref$options,
      id = _ref.id,
      name = _ref.name,
      _ref$fullWidth = _ref.fullWidth,
      fullWidth = _ref$fullWidth === void 0 ? false : _ref$fullWidth,
      _ref$isLoading = _ref.isLoading,
      isLoading = _ref$isLoading === void 0 ? false : _ref$isLoading,
      _ref$isInvalid = _ref.isInvalid,
      isInvalid = _ref$isInvalid === void 0 ? false : _ref$isInvalid,
      defaultValue = _ref.defaultValue,
      _ref$compressed = _ref.compressed,
      compressed = _ref$compressed === void 0 ? false : _ref$compressed,
      value = _ref.value,
      prepend = _ref.prepend,
      append = _ref.append,
      screenReaderId = _ref.screenReaderId,
      rest = (0, _objectWithoutProperties2.default)(_ref, ["className", "options", "id", "name", "fullWidth", "isLoading", "isInvalid", "defaultValue", "compressed", "value", "prepend", "append", "screenReaderId"]);
  var classes = (0, _classnames.default)('euiSuperSelectControl', {
    'euiSuperSelectControl--fullWidth': fullWidth,
    'euiSuperSelectControl--compressed': compressed,
    'euiSuperSelectControl--inGroup': prepend || append,
    'euiSuperSelectControl-isLoading': isLoading,
    'euiSuperSelectControl-isInvalid': isInvalid
  }, className); // React HTML input can not have both value and defaultValue properties.
  // https://reactjs.org/docs/uncontrolled-components.html#default-values

  var selectDefaultValue;

  if (value == null) {
    selectDefaultValue = defaultValue || '';
  }

  var selectedValue;

  if (value) {
    var selectedOption = options.find(function (option) {
      return option.value === value;
    });
    selectedValue = selectedOption ? selectedOption.inputDisplay : selectedValue;
  }

  var icon = {
    type: 'arrowDown',
    side: 'right'
  };
  return (0, _react2.jsx)(_react.Fragment, null, (0, _react2.jsx)("input", {
    type: "hidden",
    id: id,
    name: name,
    defaultValue: selectDefaultValue,
    value: value
  }), (0, _react2.jsx)(_form_control_layout.EuiFormControlLayout, {
    icon: icon,
    fullWidth: fullWidth,
    isLoading: isLoading,
    compressed: compressed,
    prepend: prepend,
    append: append
  }, (0, _react2.jsx)(_accessibility.EuiScreenReaderOnly, null, (0, _react2.jsx)("span", {
    id: screenReaderId
  }, (0, _react2.jsx)(_i18n.EuiI18n, {
    token: "euiSuperSelectControl.selectAnOption",
    default: "Select an option: {selectedValue}, is selected",
    values: {
      selectedValue: selectedValue
    }
  }))), (0, _react2.jsx)("button", (0, _extends2.default)({
    type: "button",
    className: classes,
    "aria-haspopup": "listbox"
  }, rest), selectedValue)));
};

exports.EuiSuperSelectControl = EuiSuperSelectControl;