import _extends from "@babel/runtime/helpers/extends";
import _classCallCheck from "@babel/runtime/helpers/classCallCheck";
import _assertThisInitialized from "@babel/runtime/helpers/assertThisInitialized";
import _createClass from "@babel/runtime/helpers/createClass";
import _inherits from "@babel/runtime/helpers/inherits";
import _possibleConstructorReturn from "@babel/runtime/helpers/possibleConstructorReturn";
import _getPrototypeOf from "@babel/runtime/helpers/getPrototypeOf";
import _defineProperty from "@babel/runtime/helpers/defineProperty";

function _createSuper(Derived) { var hasNativeReflectConstruct = _isNativeReflectConstruct(); return function _createSuperInternal() { var Super = _getPrototypeOf(Derived), result; if (hasNativeReflectConstruct) { var NewTarget = _getPrototypeOf(this).constructor; result = Reflect.construct(Super, arguments, NewTarget); } else { result = Super.apply(this, arguments); } return _possibleConstructorReturn(this, result); }; }

function _isNativeReflectConstruct() { if (typeof Reflect === "undefined" || !Reflect.construct) return false; if (Reflect.construct.sham) return false; if (typeof Proxy === "function") return true; try { Date.prototype.toString.call(Reflect.construct(Date, [], function () {})); return true; } catch (e) { return false; } }

/*
 * The MIT License (MIT)
 * 
 * Copyright (c) 2018 HackerOne Inc and individual contributors
 * 
 * Permission is hereby granted, free of charge, to any person obtaining a copy
 * of this software and associated documentation files (the "Software"), to deal
 * in the Software without restriction, including without limitation the rights
 * to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
 * copies of the Software, and to permit persons to whom the Software is
 * furnished to do so, subject to the following conditions:
 * 
 * The above copyright notice and this permission notice shall be included in all
 * copies or substantial portions of the Software.
 * 
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
 * IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
 * FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
 * AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
 * LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
 * OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
 * SOFTWARE.
 * 
 */
import Calendar from "./calendar";
import React from "react";
import PropTypes from "prop-types";
import classnames from "classnames";
import { newDate, now, cloneDate, isMoment, isDate, isBefore, isAfter, equals, setTime, getMillisecond, getSecond, getMinute, getHour, addDays, addMonths, addWeeks, addYears, subtractDays, subtractMonths, subtractWeeks, subtractYears, isSameTime, isDayDisabled, isOutOfBounds, isDayInRange, getEffectiveMinDate, getEffectiveMaxDate, parseDate, safeDateFormat, getHightLightDaysMap, getYear, getMonth } from "./date_utils";
import { EuiPopover, popoverAnchorPosition } from '../../../popover/popover';
import { jsx as ___EmotionJSX } from "@emotion/react";
export { default as CalendarContainer } from "./calendar_container";
var outsideClickIgnoreClass = "react-datepicker-ignore-onclickoutside"; // Compares dates year+month combinations

function hasPreSelectionChanged(date1, date2) {
  if (date1 && date2) {
    return getMonth(date1) !== getMonth(date2) || getYear(date1) !== getYear(date2);
  }

  return date1 !== date2;
}

function hasSelectionChanged(date1, date2) {
  if (date1 && date2) {
    return !equals(date1, date2);
  }

  return false;
}
/**
 * General datepicker component.
 */


var INPUT_ERR_1 = "Date input not valid.";

var DatePicker = /*#__PURE__*/function (_React$Component) {
  _inherits(DatePicker, _React$Component);

  var _super = _createSuper(DatePicker);

  _createClass(DatePicker, null, [{
    key: "defaultProps",
    get: function get() {
      return {
        allowSameDay: false,
        dateFormat: "L",
        dateFormatCalendar: "MMMM YYYY",
        onChange: function onChange() {},
        disabled: false,
        disabledKeyboardNavigation: false,
        dropdownMode: "scroll",
        onFocus: function onFocus() {},
        onBlur: function onBlur() {},
        onKeyDown: function onKeyDown() {},
        onInputClick: function onInputClick() {},
        onSelect: function onSelect() {},
        onClickOutside: function onClickOutside() {},
        onMonthChange: function onMonthChange() {},
        preventOpenOnFocus: false,
        onYearChange: function onYearChange() {},
        onInputError: function onInputError() {},
        monthsShown: 1,
        readOnly: false,
        withPortal: false,
        shouldCloseOnSelect: true,
        showTimeSelect: false,
        timeIntervals: 30,
        timeCaption: "Time",
        previousMonthButtonLabel: "Previous Month",
        nextMonthButtonLabel: "Next month",
        renderDayContents: function renderDayContents(date) {
          return date;
        },
        strictParsing: false
      };
    }
  }]);

  function DatePicker(props) {
    var _this;

    _classCallCheck(this, DatePicker);

    _this = _super.call(this, props);

    _defineProperty(_assertThisInitialized(_this), "getPreSelection", function () {
      return _this.props.openToDate ? newDate(_this.props.openToDate) : _this.props.selectsEnd && _this.props.startDate ? newDate(_this.props.startDate) : _this.props.selectsStart && _this.props.endDate ? newDate(_this.props.endDate) : now(_this.props.utcOffset);
    });

    _defineProperty(_assertThisInitialized(_this), "calcInitialState", function () {
      var defaultPreSelection = _this.getPreSelection();

      var minDate = getEffectiveMinDate(_this.props);
      var maxDate = getEffectiveMaxDate(_this.props);
      var boundedPreSelection = minDate && isBefore(defaultPreSelection, minDate) ? minDate : maxDate && isAfter(defaultPreSelection, maxDate) ? maxDate : defaultPreSelection;
      return {
        open: _this.props.startOpen || false,
        preventFocus: false,
        preSelection: _this.props.selected ? newDate(_this.props.selected) : boundedPreSelection,
        // transforming highlighted days (perhaps nested array)
        // to flat Map for faster access in day.jsx
        highlightDates: getHightLightDaysMap(_this.props.highlightDates),
        focused: false,
        // We attempt to handle focus trap activation manually,
        // but that is not possible with custom inputs like buttons.
        // Err on the side of a11y and trap focus when we can't be certain
        // that the trigger comoponent will work with our keyDown logic.
        enableFocusTrap: _this.props.customInput && _this.props.customInput.type !== 'input' ? true : false
      };
    });

    _defineProperty(_assertThisInitialized(_this), "clearPreventFocusTimeout", function () {
      if (_this.preventFocusTimeout) {
        clearTimeout(_this.preventFocusTimeout);
      }
    });

    _defineProperty(_assertThisInitialized(_this), "setFocus", function () {
      if (_this.input && _this.input.focus) {
        _this.input.focus();
      }
    });

    _defineProperty(_assertThisInitialized(_this), "setBlur", function () {
      if (_this.input && _this.input.blur) {
        _this.input.blur();
      }

      if (_this.props.onBlur) {
        _this.props.onBlur();
      }

      _this.cancelFocusInput();
    });

    _defineProperty(_assertThisInitialized(_this), "setOpen", function (open) {
      var skipSetBlur = arguments.length > 1 && arguments[1] !== undefined ? arguments[1] : false;

      _this.setState({
        open: open,
        preSelection: open && _this.state.open ? _this.state.preSelection : _this.calcInitialState().preSelection,
        lastPreSelectChange: PRESELECT_CHANGE_VIA_NAVIGATE
      }, function () {
        if (!open) {
          _this.setState(function (prev) {
            return {
              focused: skipSetBlur ? prev.focused : false,
              enableFocusTrap: skipSetBlur ? false : prev.enableFocusTrap
            };
          }, function () {
            // Skip `onBlur` if 
            // 1) we are possibly manually moving focus between the input and popover (skipSetBlur) and
            // 2) the blur event keeps focus on the input 
            // Focus is also guaranteed to not be inside the popover at this point
            if (!skipSetBlur || document != null && document.activeElement !== _this.input) {
              _this.setBlur();
            }

            _this.setState({
              inputValue: null
            });
          });
        }
      });
    });

    _defineProperty(_assertThisInitialized(_this), "inputOk", function () {
      return isMoment(_this.state.preSelection) || isDate(_this.state.preSelection);
    });

    _defineProperty(_assertThisInitialized(_this), "isCalendarOpen", function () {
      return _this.props.open === undefined ? _this.state.open && !_this.props.disabled && !_this.props.readOnly : _this.props.open;
    });

    _defineProperty(_assertThisInitialized(_this), "handleFocus", function (event) {
      if (!_this.state.preventFocus) {
        _this.props.onFocus(event);

        if (!_this.props.preventOpenOnFocus && !_this.props.readOnly) {
          _this.setOpen(true);
        }
      }

      _this.setState({
        focused: true
      });
    });

    _defineProperty(_assertThisInitialized(_this), "cancelFocusInput", function () {
      clearTimeout(_this.inputFocusTimeout);
      _this.inputFocusTimeout = null;
    });

    _defineProperty(_assertThisInitialized(_this), "deferFocusInput", function () {
      _this.cancelFocusInput();

      _this.inputFocusTimeout = setTimeout(function () {
        return _this.setFocus();
      }, 1);
    });

    _defineProperty(_assertThisInitialized(_this), "handleDropdownFocus", function () {
      _this.cancelFocusInput();
    });

    _defineProperty(_assertThisInitialized(_this), "handleBlur", function (event) {
      if (_this.props.accessibleMode === true) {
        // Fire the `onBlur` callback if
        // 1) the popover is closed or
        // 2) the blur event places focus outside the popover
        // Focus is also guaranteed to not on be on input at this point
        if (!_this.state.open || _this.popover && !_this.popover.contains(event.relatedTarget)) {
          _this.props.onBlur && _this.props.onBlur(event);
        }
      } else {
        if (_this.state.open && !_this.props.withPortal) {
          _this.deferFocusInput();
        } else {
          _this.props.onBlur && _this.props.onBlur(event);
        }

        _this.setState({
          focused: false
        });
      }
    });

    _defineProperty(_assertThisInitialized(_this), "handleCalendarClickOutside", function (event) {
      if (!_this.props.inline) {
        _this.setOpen(false, true);
      }

      _this.props.onClickOutside(event);

      if (_this.props.withPortal) {
        event.preventDefault();
      }
    });

    _defineProperty(_assertThisInitialized(_this), "handleChange", function () {
      for (var _len = arguments.length, allArgs = new Array(_len), _key = 0; _key < _len; _key++) {
        allArgs[_key] = arguments[_key];
      }

      var event = allArgs[0];

      if (_this.props.onChangeRaw) {
        _this.props.onChangeRaw.apply(_assertThisInitialized(_this), allArgs);

        if (typeof event.isDefaultPrevented !== "function" || event.isDefaultPrevented()) {
          return;
        }
      }

      _this.setState({
        inputValue: event.target.value,
        lastPreSelectChange: PRESELECT_CHANGE_VIA_INPUT
      });

      var date = parseDate(event.target.value, _this.props);

      if (date || !event.target.value) {
        _this.setSelected(date, event, true);
      }
    });

    _defineProperty(_assertThisInitialized(_this), "handleSelect", function (date, event) {
      // Preventing onFocus event to fix issue
      // https://github.com/Hacker0x01/react-datepicker/issues/628
      _this.setState({
        preventFocus: true
      }, function () {
        _this.preventFocusTimeout = setTimeout(function () {
          return _this.setState({
            preventFocus: false
          });
        }, 50);
        return _this.preventFocusTimeout;
      });

      _this.setSelected(date, event);

      if (!_this.props.shouldCloseOnSelect || _this.props.showTimeSelect) {
        _this.setPreSelection(date);
      } else if (!_this.props.inline) {
        _this.setOpen(false, true);
      }
    });

    _defineProperty(_assertThisInitialized(_this), "updateSelection", function (newSelection) {
      if (_this.props.adjustDateOnChange) {
        _this.setSelected(newSelection);
      }

      _this.setPreSelection(newSelection);
    });

    _defineProperty(_assertThisInitialized(_this), "setSelected", function (date, event, keepInput) {
      var changedDate = date;

      if (changedDate !== null && isDayDisabled(changedDate, _this.props)) {
        if (isOutOfBounds(changedDate, _this.props)) {
          _this.props.onChange(date, event);

          _this.props.onSelect(changedDate, event);
        }

        return;
      }

      if (changedDate !== null && _this.props.selected) {
        var selected = _this.props.selected;
        if (keepInput) selected = newDate(changedDate);
        changedDate = setTime(newDate(changedDate), {
          hour: getHour(selected),
          minute: getMinute(selected),
          second: getSecond(selected),
          millisecond: getMillisecond(selected)
        });
      }

      if (!isSameTime(_this.props.selected, changedDate) || _this.props.allowSameDay) {
        if (changedDate !== null) {
          if (!_this.props.inline) {
            _this.setState({
              preSelection: changedDate
            });
          }
        }

        _this.props.onChange(changedDate, event);
      }

      _this.props.onSelect(changedDate, event);

      if (!keepInput) {
        _this.setState({
          inputValue: null
        });
      }
    });

    _defineProperty(_assertThisInitialized(_this), "setPreSelection", function (date) {
      var isDateRangePresent = typeof _this.props.minDate !== "undefined" && typeof _this.props.maxDate !== "undefined";
      var isValidDateSelection = isDateRangePresent && date ? isDayInRange(date, _this.props.minDate, _this.props.maxDate) : true;

      if (isValidDateSelection) {
        _this.setState({
          preSelection: date
        });
      }
    });

    _defineProperty(_assertThisInitialized(_this), "handleTimeChange", function (time) {
      var selected = _this.props.selected ? _this.props.selected : _this.getPreSelection();
      var changedDate = setTime(cloneDate(selected), {
        hour: getHour(time),
        minute: getMinute(time),
        second: 0,
        millisecond: 0
      });

      if (!isSameTime(selected, changedDate)) {
        _this.setState({
          preSelection: changedDate
        });

        _this.props.onChange(changedDate);
      }

      _this.props.onSelect(changedDate);

      if (_this.props.shouldCloseOnSelect) {
        _this.setOpen(false, true);
      }

      _this.setState({
        inputValue: null
      });
    });

    _defineProperty(_assertThisInitialized(_this), "onInputClick", function () {
      if (!_this.props.disabled && !_this.props.readOnly) {
        _this.setOpen(true);
      }

      _this.props.onInputClick();
    });

    _defineProperty(_assertThisInitialized(_this), "onAccessibleModeButtonKeyDown", function (event) {
      var eventKey = event.key;

      if (eventKey === " " || eventKey === "Enter") {
        event.preventDefault();

        _this.onInputClick();
      }
    });

    _defineProperty(_assertThisInitialized(_this), "onInputKeyDown", function (event) {
      _this.props.onKeyDown(event);

      var eventKey = event.key;

      if (!_this.state.open && !_this.props.inline && !_this.props.preventOpenOnFocus) {
        if (eventKey === "ArrowDown" || eventKey === "ArrowUp") {
          event.preventDefault();

          _this.setState({
            enableFocusTrap: true
          }, function () {
            _this.onInputClick();
          });
        }

        return;
      }

      if (_this.state.open && !_this.state.enableFocusTrap) {
        if (eventKey === "ArrowDown" || eventKey === "Tab") {
          event.preventDefault();

          _this.setState({
            enableFocusTrap: true
          }, function () {
            _this.onInputClick();
          });
        } else if (eventKey === "Escape" || eventKey === "Enter") {
          event.preventDefault();

          _this.setOpen(false, true);
        }

        return;
      }

      var copy = newDate(_this.state.preSelection);

      if (eventKey === "Enter") {
        event.preventDefault();

        if (_this.inputOk() && _this.state.lastPreSelectChange === PRESELECT_CHANGE_VIA_NAVIGATE) {
          _this.handleSelect(copy, event);

          !_this.props.shouldCloseOnSelect && _this.setPreSelection(copy);
        } else {
          _this.setOpen(false, true);
        }
      } else if (eventKey === "Escape") {
        event.preventDefault();

        _this.setOpen(false, true);

        if (!_this.inputOk()) {
          _this.props.onInputError({
            code: 1,
            msg: INPUT_ERR_1
          });
        }
      } else if (eventKey === "Tab") {
        _this.setOpen(false, true);
      } else if (!_this.props.disabledKeyboardNavigation) {
        var newSelection;

        switch (eventKey) {
          case "ArrowLeft":
            newSelection = subtractDays(copy, 1);
            break;

          case "ArrowRight":
            newSelection = addDays(copy, 1);
            break;

          case "ArrowUp":
            newSelection = subtractWeeks(copy, 1);
            break;

          case "ArrowDown":
            newSelection = addWeeks(copy, 1);
            break;

          case "PageUp":
            newSelection = subtractMonths(copy, 1);
            break;

          case "PageDown":
            newSelection = addMonths(copy, 1);
            break;

          case "Home":
            newSelection = subtractYears(copy, 1);
            break;

          case "End":
            newSelection = addYears(copy, 1);
            break;
        }

        if (!newSelection) {
          if (_this.props.onInputError) {
            _this.props.onInputError({
              code: 1,
              msg: INPUT_ERR_1
            });
          }

          return; // Let the input component handle this keydown
        }

        event.preventDefault();

        _this.setState({
          lastPreSelectChange: PRESELECT_CHANGE_VIA_NAVIGATE
        });

        _this.updateSelection(newSelection);
      }
    });

    _defineProperty(_assertThisInitialized(_this), "onClearClick", function (event) {
      if (event) {
        if (event.preventDefault) {
          event.preventDefault();
        }
      }

      _this.props.onChange(null, event);

      _this.setState({
        inputValue: null
      });
    });

    _defineProperty(_assertThisInitialized(_this), "clear", function () {
      _this.onClearClick();
    });

    _defineProperty(_assertThisInitialized(_this), "renderCalendar", function () {
      if (!_this.props.inline && !_this.isCalendarOpen()) {
        return null;
      }

      var calendar = ___EmotionJSX(Calendar, {
        ref: function ref(elem) {
          _this.calendar = elem;
        },
        locale: _this.props.locale,
        adjustDateOnChange: _this.props.adjustDateOnChange,
        setOpen: _this.setOpen,
        shouldCloseOnSelect: _this.props.shouldCloseOnSelect,
        dateFormat: _this.props.dateFormatCalendar,
        useWeekdaysShort: _this.props.useWeekdaysShort,
        formatWeekDay: _this.props.formatWeekDay,
        dropdownMode: _this.props.dropdownMode,
        selected: _this.props.selected,
        preSelection: _this.state.preSelection,
        onSelect: _this.handleSelect,
        onWeekSelect: _this.props.onWeekSelect,
        openToDate: _this.props.openToDate,
        minDate: _this.props.minDate,
        maxDate: _this.props.maxDate,
        selectsStart: _this.props.selectsStart,
        selectsEnd: _this.props.selectsEnd,
        startDate: _this.props.startDate,
        endDate: _this.props.endDate,
        excludeDates: _this.props.excludeDates,
        filterDate: _this.props.filterDate,
        onClickOutside: _this.handleCalendarClickOutside,
        formatWeekNumber: _this.props.formatWeekNumber,
        highlightDates: _this.state.highlightDates,
        includeDates: _this.props.includeDates,
        includeTimes: _this.props.includeTimes,
        injectTimes: _this.props.injectTimes,
        inline: _this.props.inline,
        peekNextMonth: _this.props.peekNextMonth,
        showMonthDropdown: _this.props.showMonthDropdown,
        useShortMonthInDropdown: _this.props.useShortMonthInDropdown,
        showMonthYearDropdown: _this.props.showMonthYearDropdown,
        showWeekNumbers: _this.props.showWeekNumbers,
        showYearDropdown: _this.props.showYearDropdown,
        withPortal: _this.props.withPortal,
        forceShowMonthNavigation: _this.props.forceShowMonthNavigation,
        showDisabledMonthNavigation: _this.props.showDisabledMonthNavigation,
        scrollableYearDropdown: _this.props.scrollableYearDropdown,
        scrollableMonthYearDropdown: _this.props.scrollableMonthYearDropdown,
        todayButton: _this.props.todayButton,
        weekLabel: _this.props.weekLabel,
        utcOffset: _this.props.utcOffset,
        outsideClickIgnoreClass: outsideClickIgnoreClass,
        fixedHeight: _this.props.fixedHeight,
        monthsShown: _this.props.monthsShown,
        onDropdownFocus: _this.handleDropdownFocus,
        onMonthChange: _this.props.onMonthChange,
        onYearChange: _this.props.onYearChange,
        dayClassName: _this.props.dayClassName,
        showTimeSelect: _this.props.showTimeSelect,
        showTimeSelectOnly: _this.props.showTimeSelectOnly,
        onTimeChange: _this.handleTimeChange,
        timeFormat: _this.props.timeFormat,
        timeIntervals: _this.props.timeIntervals,
        minTime: _this.props.minTime,
        maxTime: _this.props.maxTime,
        excludeTimes: _this.props.excludeTimes,
        timeCaption: _this.props.timeCaption,
        className: _this.props.calendarClassName,
        container: _this.props.calendarContainer,
        yearDropdownItemNumber: _this.props.yearDropdownItemNumber,
        previousMonthButtonLabel: _this.props.previousMonthButtonLabel,
        nextMonthButtonLabel: _this.props.nextMonthButtonLabel,
        disabledKeyboardNavigation: _this.props.disabledKeyboardNavigation,
        renderCustomHeader: _this.props.renderCustomHeader,
        popperProps: _this.props.popperProps,
        renderDayContents: _this.props.renderDayContents,
        updateSelection: _this.updateSelection,
        accessibleMode: _this.props.accessibleMode,
        enableFocusTrap: _this.state.enableFocusTrap
      }, _this.props.children);

      return calendar;
    });

    _defineProperty(_assertThisInitialized(_this), "renderDateInput", function () {
      var _React$cloneElement;

      var className = classnames(_this.props.className, _defineProperty({}, outsideClickIgnoreClass, _this.state.open));

      var customInput = _this.props.customInput || ___EmotionJSX("input", {
        type: "text"
      });

      var customInputRef = _this.props.customInputRef || "ref";
      var inputValue = typeof _this.props.value === "string" ? _this.props.value : typeof _this.state.inputValue === "string" ? _this.state.inputValue : safeDateFormat(_this.props.selected, _this.props);
      return /*#__PURE__*/React.cloneElement(customInput, (_React$cloneElement = {}, _defineProperty(_React$cloneElement, customInputRef, function (input) {
        _this.input = input;
      }), _defineProperty(_React$cloneElement, "value", inputValue), _defineProperty(_React$cloneElement, "onBlur", _this.handleBlur), _defineProperty(_React$cloneElement, "onChange", _this.handleChange), _defineProperty(_React$cloneElement, "onClick", _this.onInputClick), _defineProperty(_React$cloneElement, "onFocus", _this.handleFocus), _defineProperty(_React$cloneElement, "onKeyDown", _this.onInputKeyDown), _defineProperty(_React$cloneElement, "id", _this.props.id), _defineProperty(_React$cloneElement, "name", _this.props.name), _defineProperty(_React$cloneElement, "autoFocus", _this.props.autoFocus), _defineProperty(_React$cloneElement, "placeholder", _this.props.placeholderText), _defineProperty(_React$cloneElement, "disabled", _this.props.disabled), _defineProperty(_React$cloneElement, "autoComplete", _this.props.autoComplete), _defineProperty(_React$cloneElement, "className", className), _defineProperty(_React$cloneElement, "title", _this.props.title), _defineProperty(_React$cloneElement, "readOnly", _this.props.readOnly), _defineProperty(_React$cloneElement, "required", _this.props.required), _defineProperty(_React$cloneElement, "tabIndex", _this.props.tabIndex), _defineProperty(_React$cloneElement, "aria-label", _this.state.open ? 'Press the down key to enter a popover containing a calendar. Press the escape key to close the popover.' : 'Press the down key to open a popover containing a calendar.'), _React$cloneElement));
    });

    _defineProperty(_assertThisInitialized(_this), "renderClearButton", function () {
      if (_this.props.isClearable && _this.props.selected != null) {
        return ___EmotionJSX("button", {
          type: "button",
          className: "react-datepicker__close-icon",
          onClick: _this.onClearClick,
          title: _this.props.clearButtonTitle,
          tabIndex: -1
        });
      } else {
        return null;
      }
    });

    _defineProperty(_assertThisInitialized(_this), "renderAccessibleButton", function () {
      if (_this.props.accessibleModeButton != null) {
        return /*#__PURE__*/React.cloneElement(_this.props.accessibleModeButton, {
          onClick: _this.onInputClick,
          onKeyDown: _this.onAccessibleModeButtonKeyDown,
          className: classnames(_this.props.accessibleModeButton.props.className, "react-datepicker__accessible-icon"),
          tabIndex: 0
        });
      } else {
        return null;
      }
    });

    _this.state = _this.calcInitialState(); // Refs

    _this.input;
    _this.calendar;
    _this.popover;
    return _this;
  }

  _createClass(DatePicker, [{
    key: "componentDidUpdate",
    value: function componentDidUpdate(prevProps, prevState) {
      if (prevProps.inline && hasPreSelectionChanged(prevProps.selected, this.props.selected)) {
        this.setPreSelection(this.props.selected);
      }

      if (prevProps.highlightDates !== this.props.highlightDates) {
        this.setState({
          highlightDates: getHightLightDaysMap(this.props.highlightDates)
        });
      }

      if (!prevState.focused && hasSelectionChanged(prevProps.selected, this.props.selected)) {
        this.setState({
          inputValue: null
        });
      }
    }
  }, {
    key: "componentWillUnmount",
    value: function componentWillUnmount() {
      this.clearPreventFocusTimeout();
    }
  }, {
    key: "render",
    value: function render() {
      var _this2 = this;

      var calendar = this.renderCalendar();

      if (this.props.inline && !this.props.withPortal) {
        return calendar;
      }

      if (this.props.withPortal) {
        return ___EmotionJSX("div", null, !this.props.inline ? ___EmotionJSX("div", {
          className: "react-datepicker__input-container"
        }, this.renderDateInput(), this.renderClearButton(), this.renderAccessibleButton()) : null, this.state.open || this.props.inline ? ___EmotionJSX("div", {
          className: "react-datepicker__portal"
        }, calendar) : null);
      }

      return ___EmotionJSX(EuiPopover, _extends({
        ownFocus: false,
        className: this.props.popperClassName,
        isOpen: this.isCalendarOpen(),
        closePopover: function closePopover() {
          return _this2.setOpen(false, true);
        },
        hasArrow: false,
        buffer: 0,
        display: "block",
        panelPaddingSize: "none",
        anchorPosition: this.props.popperPlacement,
        container: this.props.popperContainer,
        panelRef: function panelRef(elem) {
          _this2.popover = elem;
        }
      }, this.props.popperProps, {
        button: ___EmotionJSX("div", {
          className: "react-datepicker__input-container"
        }, this.renderDateInput(), this.renderClearButton(), this.renderAccessibleButton())
      }), calendar);
    }
  }]);

  return DatePicker;
}(React.Component);

_defineProperty(DatePicker, "propTypes", {
  adjustDateOnChange: PropTypes.bool,
  allowSameDay: PropTypes.bool,
  autoComplete: PropTypes.string,
  autoFocus: PropTypes.bool,
  calendarClassName: PropTypes.string,
  calendarContainer: PropTypes.func,
  children: PropTypes.node,
  className: PropTypes.string,
  customInput: PropTypes.element,
  customInputRef: PropTypes.string,
  // eslint-disable-next-line react/no-unused-prop-types
  dateFormat: PropTypes.oneOfType([PropTypes.string, PropTypes.array]),
  dateFormatCalendar: PropTypes.string,
  dayClassName: PropTypes.func,
  disabled: PropTypes.bool,
  disabledKeyboardNavigation: PropTypes.bool,
  dropdownMode: PropTypes.oneOf(["scroll", "select"]).isRequired,
  endDate: PropTypes.object,
  excludeDates: PropTypes.array,
  filterDate: PropTypes.func,
  fixedHeight: PropTypes.bool,
  formatWeekNumber: PropTypes.func,
  highlightDates: PropTypes.array,
  id: PropTypes.string,
  includeDates: PropTypes.array,
  includeTimes: PropTypes.array,
  injectTimes: PropTypes.array,
  inline: PropTypes.bool,
  isClearable: PropTypes.bool,
  locale: PropTypes.string,
  maxDate: PropTypes.object,
  minDate: PropTypes.object,
  monthsShown: PropTypes.number,
  name: PropTypes.string,
  onBlur: PropTypes.func,
  onChange: PropTypes.func.isRequired,
  onSelect: PropTypes.func,
  onWeekSelect: PropTypes.func,
  onClickOutside: PropTypes.func,
  onChangeRaw: PropTypes.func,
  onFocus: PropTypes.func,
  onInputClick: PropTypes.func,
  onKeyDown: PropTypes.func,
  onMonthChange: PropTypes.func,
  onYearChange: PropTypes.func,
  onInputError: PropTypes.func,
  open: PropTypes.bool,
  openToDate: PropTypes.object,
  peekNextMonth: PropTypes.bool,
  placeholderText: PropTypes.string,
  popperContainer: PropTypes.func,
  popperClassName: PropTypes.string,
  // <PopperComponent/> props
  popperModifiers: PropTypes.object,
  // <PopperComponent/> props
  popperPlacement: PropTypes.oneOf(popoverAnchorPosition),
  popperProps: PropTypes.object,
  preventOpenOnFocus: PropTypes.bool,
  readOnly: PropTypes.bool,
  required: PropTypes.bool,
  scrollableYearDropdown: PropTypes.bool,
  scrollableMonthYearDropdown: PropTypes.bool,
  selected: PropTypes.object,
  selectsEnd: PropTypes.bool,
  selectsStart: PropTypes.bool,
  showMonthDropdown: PropTypes.bool,
  showMonthYearDropdown: PropTypes.bool,
  showWeekNumbers: PropTypes.bool,
  showYearDropdown: PropTypes.bool,
  forceShowMonthNavigation: PropTypes.bool,
  showDisabledMonthNavigation: PropTypes.bool,
  startDate: PropTypes.object,
  startOpen: PropTypes.bool,
  tabIndex: PropTypes.number,
  timeCaption: PropTypes.string,
  title: PropTypes.string,
  todayButton: PropTypes.node,
  useWeekdaysShort: PropTypes.bool,
  formatWeekDay: PropTypes.func,
  utcOffset: PropTypes.oneOfType([PropTypes.number, PropTypes.string]),
  value: PropTypes.string,
  weekLabel: PropTypes.string,
  withPortal: PropTypes.bool,
  yearDropdownItemNumber: PropTypes.number,
  shouldCloseOnSelect: PropTypes.bool,
  showTimeSelect: PropTypes.bool,
  showTimeSelectOnly: PropTypes.bool,
  timeFormat: PropTypes.string,
  timeIntervals: PropTypes.number,
  minTime: PropTypes.object,
  maxTime: PropTypes.object,
  excludeTimes: PropTypes.array,
  useShortMonthInDropdown: PropTypes.bool,
  clearButtonTitle: PropTypes.string,
  previousMonthButtonLabel: PropTypes.string,
  nextMonthButtonLabel: PropTypes.string,
  renderCustomHeader: PropTypes.func,
  renderDayContents: PropTypes.func,
  accessibleMode: PropTypes.bool,
  accessibleModeButton: PropTypes.element,
  strictParsing: PropTypes.bool // eslint-disable-line react/no-unused-prop-types

});

export { DatePicker as default };
var PRESELECT_CHANGE_VIA_INPUT = "input";
var PRESELECT_CHANGE_VIA_NAVIGATE = "navigate";