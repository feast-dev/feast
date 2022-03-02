import _extends from "@babel/runtime/helpers/extends";
import _objectWithoutProperties from "@babel/runtime/helpers/objectWithoutProperties";
import _classCallCheck from "@babel/runtime/helpers/classCallCheck";
import _createClass from "@babel/runtime/helpers/createClass";
import _inherits from "@babel/runtime/helpers/inherits";
import _possibleConstructorReturn from "@babel/runtime/helpers/possibleConstructorReturn";
import _getPrototypeOf from "@babel/runtime/helpers/getPrototypeOf";
import _defineProperty from "@babel/runtime/helpers/defineProperty";

function _createSuper(Derived) { var hasNativeReflectConstruct = _isNativeReflectConstruct(); return function _createSuperInternal() { var Super = _getPrototypeOf(Derived), result; if (hasNativeReflectConstruct) { var NewTarget = _getPrototypeOf(this).constructor; result = Reflect.construct(Super, arguments, NewTarget); } else { result = Super.apply(this, arguments); } return _possibleConstructorReturn(this, result); }; }

function _isNativeReflectConstruct() { if (typeof Reflect === "undefined" || !Reflect.construct) return false; if (Reflect.construct.sham) return false; if (typeof Proxy === "function") return true; try { Date.prototype.toString.call(Reflect.construct(Date, [], function () {})); return true; } catch (e) { return false; } }

/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */
import React, { Component } from 'react';
import classNames from 'classnames';
// eslint-disable-line import/named
import { EuiFormControlLayout, EuiValidatableControl } from '../form';
import { EuiI18nConsumer } from '../context';
import { ReactDatePicker } from './react-datepicker';
import { jsx as ___EmotionJSX } from "@emotion/react";
export var euiDatePickerDefaultDateFormat = 'MM/DD/YYYY';
export var euiDatePickerDefaultTimeFormat = 'hh:mm A';
var mapAnchorPositions = {
  'bottom-start': 'downLeft',
  bottom: 'downCenter',
  'bottom-end': 'downRight',
  'left-start': 'leftUp',
  left: 'leftCenter',
  'left-end': 'leftDown',
  'right-start': 'rightUp',
  right: 'rightCenter',
  'right-end': 'rightDown',
  'top-start': 'upLeft',
  top: 'upCenter',
  'top-end': 'upRight'
};

function isPopperPlacement(position) {
  return position != null && Object.keys(mapAnchorPositions).includes(position);
} // EuiDatePicker only supports a subset of props from react-datepicker.


var unsupportedProps = [// We don't want to show multiple months next to each other
'monthsShown', // There is no need to show week numbers
'showWeekNumbers', // Our css adapts to height, no need to fix it
'fixedHeight', // We force the month / year selection UI. No need to configure it
'dropdownMode', // Short month is unnecessary. Our UI has plenty of room for full months
'useShortMonthInDropdown', // The today button is not needed. This should always be external to the calendar
'todayButton', // We hide the time caption, so there is no need to overwrite its text
'timeCaption', // We always want keyboard accessibility on
'disabledKeyboardNavigation', // This is easy enough to do. It can conflict with isLoading state
'isClearable', // There is no reason to launch the datepicker in its own modal. Can always build these ourselves
'withPortal', // Causes Error: Cannot read property 'clone' of undefined
'showMonthYearDropdown', // We overridde this with `popoverPlacement`
'popperPlacement'];
export var EuiDatePicker = /*#__PURE__*/function (_Component) {
  _inherits(EuiDatePicker, _Component);

  var _super = _createSuper(EuiDatePicker);

  function EuiDatePicker() {
    _classCallCheck(this, EuiDatePicker);

    return _super.apply(this, arguments);
  }

  _createClass(EuiDatePicker, [{
    key: "render",
    value: function render() {
      var _this$props = this.props,
          adjustDateOnChange = _this$props.adjustDateOnChange,
          calendarClassName = _this$props.calendarClassName,
          className = _this$props.className,
          customInput = _this$props.customInput,
          dateFormat = _this$props.dateFormat,
          dayClassName = _this$props.dayClassName,
          disabled = _this$props.disabled,
          excludeDates = _this$props.excludeDates,
          filterDate = _this$props.filterDate,
          fullWidth = _this$props.fullWidth,
          iconType = _this$props.iconType,
          injectTimes = _this$props.injectTimes,
          inline = _this$props.inline,
          inputRef = _this$props.inputRef,
          isInvalid = _this$props.isInvalid,
          isLoading = _this$props.isLoading,
          locale = _this$props.locale,
          maxDate = _this$props.maxDate,
          maxTime = _this$props.maxTime,
          minDate = _this$props.minDate,
          minTime = _this$props.minTime,
          onChange = _this$props.onChange,
          onClear = _this$props.onClear,
          openToDate = _this$props.openToDate,
          placeholder = _this$props.placeholder,
          popperClassName = _this$props.popperClassName,
          _popoverPlacement = _this$props.popoverPlacement,
          selected = _this$props.selected,
          shadow = _this$props.shadow,
          shouldCloseOnSelect = _this$props.shouldCloseOnSelect,
          showIcon = _this$props.showIcon,
          showTimeSelect = _this$props.showTimeSelect,
          showTimeSelectOnly = _this$props.showTimeSelectOnly,
          timeFormat = _this$props.timeFormat,
          utcOffset = _this$props.utcOffset,
          rest = _objectWithoutProperties(_this$props, ["adjustDateOnChange", "calendarClassName", "className", "customInput", "dateFormat", "dayClassName", "disabled", "excludeDates", "filterDate", "fullWidth", "iconType", "injectTimes", "inline", "inputRef", "isInvalid", "isLoading", "locale", "maxDate", "maxTime", "minDate", "minTime", "onChange", "onClear", "openToDate", "placeholder", "popperClassName", "popoverPlacement", "selected", "shadow", "shouldCloseOnSelect", "showIcon", "showTimeSelect", "showTimeSelectOnly", "timeFormat", "utcOffset"]);

      var classes = classNames('euiDatePicker', {
        'euiDatePicker--shadow': shadow,
        'euiDatePicker--inline': inline
      });
      var datePickerClasses = classNames('euiDatePicker', 'euiFieldText', {
        'euiFieldText--fullWidth': fullWidth,
        'euiFieldText-isLoading': isLoading,
        'euiFieldText--withIcon': !inline && showIcon,
        'euiFieldText--isClearable': !inline && selected && onClear,
        'euiFieldText-isInvalid': isInvalid
      }, className);
      var optionalIcon;

      if (inline || customInput || !showIcon) {
        optionalIcon = undefined;
      } else if (iconType) {
        optionalIcon = iconType;
      } else if (showTimeSelectOnly) {
        optionalIcon = 'clock';
      } else {
        optionalIcon = 'calendar';
      } // In case the consumer did not alter the default date format but wants
      // to add the time select, we append the default time format


      var fullDateFormat = dateFormat;

      if (showTimeSelect && dateFormat === euiDatePickerDefaultDateFormat) {
        fullDateFormat = "".concat(dateFormat, " ").concat(timeFormat);
      }

      var popoverPlacement;

      if (isPopperPlacement(_popoverPlacement)) {
        popoverPlacement = mapAnchorPositions[_popoverPlacement];
      } else {
        popoverPlacement = _popoverPlacement;
      }

      return ___EmotionJSX("span", {
        className: classes
      }, ___EmotionJSX(EuiFormControlLayout, {
        icon: optionalIcon,
        fullWidth: fullWidth,
        clear: selected && onClear ? {
          onClick: onClear
        } : undefined,
        isLoading: isLoading
      }, ___EmotionJSX(EuiValidatableControl, {
        isInvalid: isInvalid
      }, ___EmotionJSX(EuiI18nConsumer, null, function (_ref) {
        var contextLocale = _ref.locale;
        return ___EmotionJSX(ReactDatePicker, _extends({
          adjustDateOnChange: adjustDateOnChange,
          calendarClassName: calendarClassName,
          className: datePickerClasses,
          customInput: customInput,
          dateFormat: fullDateFormat,
          dayClassName: dayClassName,
          disabled: disabled,
          excludeDates: excludeDates,
          filterDate: filterDate,
          injectTimes: injectTimes,
          inline: inline,
          locale: locale || contextLocale,
          maxDate: maxDate,
          maxTime: maxTime,
          minDate: minDate,
          minTime: minTime,
          onChange: onChange,
          openToDate: openToDate,
          placeholderText: placeholder,
          popperClassName: popperClassName,
          ref: inputRef,
          selected: selected,
          shouldCloseOnSelect: shouldCloseOnSelect,
          showMonthDropdown: true,
          showTimeSelect: showTimeSelectOnly ? true : showTimeSelect,
          showTimeSelectOnly: showTimeSelectOnly,
          showYearDropdown: true,
          timeFormat: timeFormat,
          utcOffset: utcOffset,
          yearDropdownItemNumber: 7,
          accessibleMode: true,
          popperPlacement: popoverPlacement
        }, rest));
      }))));
    }
  }]);

  return EuiDatePicker;
}(Component);

_defineProperty(EuiDatePicker, "defaultProps", {
  adjustDateOnChange: true,
  dateFormat: euiDatePickerDefaultDateFormat,
  fullWidth: false,
  inputRef: function inputRef() {},
  isLoading: false,
  shadow: true,
  shouldCloseOnSelect: true,
  showIcon: true,
  showTimeSelect: false,
  timeFormat: euiDatePickerDefaultTimeFormat,
  popoverPlacement: 'downLeft'
});