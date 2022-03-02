function _typeof(obj) { "@babel/helpers - typeof"; if (typeof Symbol === "function" && typeof Symbol.iterator === "symbol") { _typeof = function _typeof(obj) { return typeof obj; }; } else { _typeof = function _typeof(obj) { return obj && typeof Symbol === "function" && obj.constructor === Symbol && obj !== Symbol.prototype ? "symbol" : typeof obj; }; } return _typeof(obj); }

function ownKeys(object, enumerableOnly) { var keys = Object.keys(object); if (Object.getOwnPropertySymbols) { var symbols = Object.getOwnPropertySymbols(object); if (enumerableOnly) symbols = symbols.filter(function (sym) { return Object.getOwnPropertyDescriptor(object, sym).enumerable; }); keys.push.apply(keys, symbols); } return keys; }

function _objectSpread(target) { for (var i = 1; i < arguments.length; i++) { var source = arguments[i] != null ? arguments[i] : {}; if (i % 2) { ownKeys(Object(source), true).forEach(function (key) { _defineProperty(target, key, source[key]); }); } else if (Object.getOwnPropertyDescriptors) { Object.defineProperties(target, Object.getOwnPropertyDescriptors(source)); } else { ownKeys(Object(source)).forEach(function (key) { Object.defineProperty(target, key, Object.getOwnPropertyDescriptor(source, key)); }); } } return target; }

function _classCallCheck(instance, Constructor) { if (!(instance instanceof Constructor)) { throw new TypeError("Cannot call a class as a function"); } }

function _defineProperties(target, props) { for (var i = 0; i < props.length; i++) { var descriptor = props[i]; descriptor.enumerable = descriptor.enumerable || false; descriptor.configurable = true; if ("value" in descriptor) descriptor.writable = true; Object.defineProperty(target, descriptor.key, descriptor); } }

function _createClass(Constructor, protoProps, staticProps) { if (protoProps) _defineProperties(Constructor.prototype, protoProps); if (staticProps) _defineProperties(Constructor, staticProps); return Constructor; }

function _inherits(subClass, superClass) { if (typeof superClass !== "function" && superClass !== null) { throw new TypeError("Super expression must either be null or a function"); } subClass.prototype = Object.create(superClass && superClass.prototype, { constructor: { value: subClass, writable: true, configurable: true } }); if (superClass) _setPrototypeOf(subClass, superClass); }

function _setPrototypeOf(o, p) { _setPrototypeOf = Object.setPrototypeOf || function _setPrototypeOf(o, p) { o.__proto__ = p; return o; }; return _setPrototypeOf(o, p); }

function _createSuper(Derived) { var hasNativeReflectConstruct = _isNativeReflectConstruct(); return function _createSuperInternal() { var Super = _getPrototypeOf(Derived), result; if (hasNativeReflectConstruct) { var NewTarget = _getPrototypeOf(this).constructor; result = Reflect.construct(Super, arguments, NewTarget); } else { result = Super.apply(this, arguments); } return _possibleConstructorReturn(this, result); }; }

function _possibleConstructorReturn(self, call) { if (call && (_typeof(call) === "object" || typeof call === "function")) { return call; } return _assertThisInitialized(self); }

function _assertThisInitialized(self) { if (self === void 0) { throw new ReferenceError("this hasn't been initialised - super() hasn't been called"); } return self; }

function _isNativeReflectConstruct() { if (typeof Reflect === "undefined" || !Reflect.construct) return false; if (Reflect.construct.sham) return false; if (typeof Proxy === "function") return true; try { Date.prototype.toString.call(Reflect.construct(Date, [], function () {})); return true; } catch (e) { return false; } }

function _getPrototypeOf(o) { _getPrototypeOf = Object.setPrototypeOf ? Object.getPrototypeOf : function _getPrototypeOf(o) { return o.__proto__ || Object.getPrototypeOf(o); }; return _getPrototypeOf(o); }

function _defineProperty(obj, key, value) { if (key in obj) { Object.defineProperty(obj, key, { value: value, enumerable: true, configurable: true, writable: true }); } else { obj[key] = value; } return obj; }

function _extends() { _extends = Object.assign || function (target) { for (var i = 1; i < arguments.length; i++) { var source = arguments[i]; for (var key in source) { if (Object.prototype.hasOwnProperty.call(source, key)) { target[key] = source[key]; } } } return target; }; return _extends.apply(this, arguments); }

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
import YearDropdown from "./year_dropdown";
import MonthDropdown from "./month_dropdown";
import MonthYearDropdown from "./month_year_dropdown";
import Month from "./month";
import Time from "./time";
import React from "react";
import PropTypes from "prop-types";
import classnames from "classnames";
import CalendarContainer from "./calendar_container";
import { now, setMonth, getMonth, addMonths, subtractMonths, getStartOfWeek, getStartOfDate, getStartOfMonth, addDays, cloneDate, formatDate, localizeDate, setYear, getYear, isBefore, isAfter, getLocaleData, getFormattedWeekdayInLocale, getWeekdayShortInLocale, getWeekdayMinInLocale, isSameDay, allDaysDisabledBefore, allDaysDisabledAfter, getEffectiveMinDate, getEffectiveMaxDate } from "./date_utils";
import { EuiFocusTrap } from '../../../focus_trap';
import { jsx as ___EmotionJSX } from "@emotion/react";
var FocusTrapContainer = /*#__PURE__*/React.forwardRef(function (props, ref) {
  return ___EmotionJSX("div", _extends({
    ref: ref,
    className: "react-datepicker__focusTrap"
  }, props));
});
var DROPDOWN_FOCUS_CLASSNAMES = ["react-datepicker__year-select", "react-datepicker__month-select", "react-datepicker__month-year-select"];

var isDropdownSelect = function isDropdownSelect() {
  var element = arguments.length > 0 && arguments[0] !== undefined ? arguments[0] : {};
  var classNames = (element.className || "").split(/\s+/);
  return DROPDOWN_FOCUS_CLASSNAMES.some(function (testClassname) {
    return classNames.indexOf(testClassname) >= 0;
  });
};

var Calendar = /*#__PURE__*/function (_React$Component) {
  _inherits(Calendar, _React$Component);

  var _super = _createSuper(Calendar);

  _createClass(Calendar, null, [{
    key: "defaultProps",
    get: function get() {
      return {
        onDropdownFocus: function onDropdownFocus() {},
        monthsShown: 1,
        forceShowMonthNavigation: false,
        timeCaption: "Time",
        previousMonthButtonLabel: "Previous Month",
        nextMonthButtonLabel: "Next Month",
        enableFocusTrap: true
      };
    }
  }]);

  function Calendar(props) {
    var _this;

    _classCallCheck(this, Calendar);

    _this = _super.call(this, props);

    _defineProperty(_assertThisInitialized(_this), "setMonthRef", function (node) {
      _this.monthRef = node;
    });

    _defineProperty(_assertThisInitialized(_this), "setYearRef", function (node) {
      _this.yearRef = node;
    });

    _defineProperty(_assertThisInitialized(_this), "handleOnDropdownToggle", function (isOpen, dropdown) {
      _this.setState({
        pauseFocusTrap: isOpen
      });

      if (!isOpen) {
        var element = dropdown === 'month' ? _this.monthRef : _this.yearRef;

        if (element) {
          // The focus trap has been unpaused and will reinitialize focus
          // but does so on the wrong element (calendar)
          // This refocuses the previous element (dropdown button).
          // Duration arrived at by trial-and-error.
          setTimeout(function () {
            return element.focus();
          }, 25);
        }
      }
    });

    _defineProperty(_assertThisInitialized(_this), "handleClickOutside", function (event) {
      _this.props.onClickOutside(event);
    });

    _defineProperty(_assertThisInitialized(_this), "handleDropdownFocus", function (event) {
      if (isDropdownSelect(event.target)) {
        _this.props.onDropdownFocus();
      }
    });

    _defineProperty(_assertThisInitialized(_this), "getDateInView", function () {
      var _this$props = _this.props,
          preSelection = _this$props.preSelection,
          selected = _this$props.selected,
          openToDate = _this$props.openToDate,
          utcOffset = _this$props.utcOffset;
      var minDate = getEffectiveMinDate(_this.props);
      var maxDate = getEffectiveMaxDate(_this.props);
      var current = now(utcOffset);
      var initialDate = openToDate || selected || preSelection;

      if (initialDate) {
        return initialDate;
      } else {
        if (minDate && isBefore(current, minDate)) {
          return minDate;
        } else if (maxDate && isAfter(current, maxDate)) {
          return maxDate;
        }
      }

      return current;
    });

    _defineProperty(_assertThisInitialized(_this), "localizeDate", function (date) {
      return localizeDate(date, _this.props.locale);
    });

    _defineProperty(_assertThisInitialized(_this), "increaseMonth", function () {
      _this.setState({
        date: addMonths(cloneDate(_this.state.date), 1)
      }, function () {
        return _this.handleMonthChange(_this.state.date);
      });
    });

    _defineProperty(_assertThisInitialized(_this), "decreaseMonth", function () {
      _this.setState({
        date: subtractMonths(cloneDate(_this.state.date), 1)
      }, function () {
        return _this.handleMonthChange(_this.state.date);
      });
    });

    _defineProperty(_assertThisInitialized(_this), "handleDayClick", function (day, event) {
      return _this.props.onSelect(day, event);
    });

    _defineProperty(_assertThisInitialized(_this), "handleDayMouseEnter", function (day) {
      return _this.setState({
        selectingDate: day
      });
    });

    _defineProperty(_assertThisInitialized(_this), "handleMonthMouseLeave", function () {
      return _this.setState({
        selectingDate: null
      });
    });

    _defineProperty(_assertThisInitialized(_this), "handleYearChange", function (date) {
      if (_this.props.onYearChange) {
        _this.props.onYearChange(date);
      }

      if (_this.props.accessibleMode) {
        _this.handleSelectionChange(date);
      }
    });

    _defineProperty(_assertThisInitialized(_this), "handleMonthChange", function (date) {
      if (_this.props.onMonthChange) {
        _this.props.onMonthChange(date);
      }

      if (_this.props.accessibleMode) {
        _this.handleSelectionChange(date);
      }
    });

    _defineProperty(_assertThisInitialized(_this), "handleSelectionChange", function (date) {
      if (_this.props.adjustDateOnChange) {
        _this.props.updateSelection(date);
      } else {
        _this.props.updateSelection(getStartOfMonth(cloneDate(date)));
      }
    });

    _defineProperty(_assertThisInitialized(_this), "handleMonthYearChange", function (date) {
      _this.handleYearChange(date);

      _this.handleMonthChange(date);
    });

    _defineProperty(_assertThisInitialized(_this), "changeYear", function (year) {
      _this.setState({
        date: setYear(cloneDate(_this.state.date), year)
      }, function () {
        return _this.handleYearChange(_this.state.date);
      });
    });

    _defineProperty(_assertThisInitialized(_this), "changeMonth", function (month) {
      _this.setState({
        date: setMonth(cloneDate(_this.state.date), month)
      }, function () {
        return _this.handleMonthChange(_this.state.date);
      });
    });

    _defineProperty(_assertThisInitialized(_this), "changeMonthYear", function (monthYear) {
      _this.setState({
        date: setYear(setMonth(cloneDate(_this.state.date), getMonth(monthYear)), getYear(monthYear))
      }, function () {
        return _this.handleMonthYearChange(_this.state.date);
      });
    });

    _defineProperty(_assertThisInitialized(_this), "header", function () {
      var date = arguments.length > 0 && arguments[0] !== undefined ? arguments[0] : _this.state.date;
      var startOfWeek = getStartOfWeek(cloneDate(date));
      var dayNames = [];

      if (_this.props.showWeekNumbers) {
        dayNames.push(___EmotionJSX("div", {
          key: "W",
          className: "react-datepicker__day-name"
        }, _this.props.weekLabel || "#"));
      }

      return dayNames.concat([0, 1, 2, 3, 4, 5, 6].map(function (offset) {
        var day = addDays(cloneDate(startOfWeek), offset);
        var localeData = getLocaleData(day);

        var weekDayName = _this.formatWeekday(localeData, day);

        return ___EmotionJSX("div", {
          key: offset,
          className: "react-datepicker__day-name"
        }, weekDayName);
      }));
    });

    _defineProperty(_assertThisInitialized(_this), "formatWeekday", function (localeData, day) {
      if (_this.props.formatWeekDay) {
        return getFormattedWeekdayInLocale(localeData, day, _this.props.formatWeekDay);
      }

      return _this.props.useWeekdaysShort ? getWeekdayShortInLocale(localeData, day) : getWeekdayMinInLocale(localeData, day);
    });

    _defineProperty(_assertThisInitialized(_this), "renderPreviousMonthButton", function () {
      if (_this.props.renderCustomHeader) {
        return;
      }

      var allPrevDaysDisabled = allDaysDisabledBefore(_this.state.date, "month", _this.props);

      if (!_this.props.forceShowMonthNavigation && !_this.props.showDisabledMonthNavigation && allPrevDaysDisabled || _this.props.showTimeSelectOnly) {
        return;
      }

      var classes = ["react-datepicker__navigation", "react-datepicker__navigation--previous"];
      var clickHandler = _this.decreaseMonth;

      if (allPrevDaysDisabled && _this.props.showDisabledMonthNavigation) {
        classes.push("react-datepicker__navigation--previous--disabled");
        clickHandler = null;
      }

      return ___EmotionJSX("button", {
        type: "button",
        className: classes.join(" "),
        onClick: clickHandler
      }, _this.props.previousMonthButtonLabel);
    });

    _defineProperty(_assertThisInitialized(_this), "renderNextMonthButton", function () {
      if (_this.props.renderCustomHeader) {
        return;
      }

      var allNextDaysDisabled = allDaysDisabledAfter(_this.state.date, "month", _this.props);

      if (!_this.props.forceShowMonthNavigation && !_this.props.showDisabledMonthNavigation && allNextDaysDisabled || _this.props.showTimeSelectOnly) {
        return;
      }

      var classes = ["react-datepicker__navigation", "react-datepicker__navigation--next"];

      if (_this.props.showTimeSelect) {
        classes.push("react-datepicker__navigation--next--with-time");
      }

      if (_this.props.todayButton) {
        classes.push("react-datepicker__navigation--next--with-today-button");
      }

      var clickHandler = _this.increaseMonth;

      if (allNextDaysDisabled && _this.props.showDisabledMonthNavigation) {
        classes.push("react-datepicker__navigation--next--disabled");
        clickHandler = null;
      }

      return ___EmotionJSX("button", {
        type: "button",
        className: classes.join(" "),
        onClick: clickHandler
      }, _this.props.nextMonthButtonLabel);
    });

    _defineProperty(_assertThisInitialized(_this), "renderCurrentMonth", function () {
      var date = arguments.length > 0 && arguments[0] !== undefined ? arguments[0] : _this.state.date;
      var classes = ["react-datepicker__current-month"];

      if (_this.props.showYearDropdown) {
        classes.push("react-datepicker__current-month--hasYearDropdown");
      }

      if (_this.props.showMonthDropdown) {
        classes.push("react-datepicker__current-month--hasMonthDropdown");
      }

      if (_this.props.showMonthYearDropdown) {
        classes.push("react-datepicker__current-month--hasMonthYearDropdown");
      }

      return ___EmotionJSX("div", {
        className: classes.join(" ")
      }, formatDate(date, _this.props.dateFormat));
    });

    _defineProperty(_assertThisInitialized(_this), "renderYearDropdown", function () {
      var overrideHide = arguments.length > 0 && arguments[0] !== undefined ? arguments[0] : false;

      if (!_this.props.showYearDropdown || overrideHide) {
        return;
      }

      return ___EmotionJSX(YearDropdown, {
        adjustDateOnChange: _this.props.adjustDateOnChange,
        date: _this.state.date,
        onSelect: _this.props.onSelect,
        setOpen: _this.props.setOpen,
        dropdownMode: _this.props.dropdownMode,
        onChange: _this.changeYear,
        minDate: _this.props.minDate,
        maxDate: _this.props.maxDate,
        year: getYear(_this.state.date),
        scrollableYearDropdown: _this.props.scrollableYearDropdown,
        yearDropdownItemNumber: _this.props.yearDropdownItemNumber,
        accessibleMode: _this.props.accessibleMode,
        onDropdownToggle: _this.handleOnDropdownToggle,
        buttonRef: _this.setYearRef
      });
    });

    _defineProperty(_assertThisInitialized(_this), "renderMonthDropdown", function () {
      var overrideHide = arguments.length > 0 && arguments[0] !== undefined ? arguments[0] : false;

      if (!_this.props.showMonthDropdown || overrideHide) {
        return;
      }

      return ___EmotionJSX(MonthDropdown, {
        dropdownMode: _this.props.dropdownMode,
        locale: _this.props.locale,
        dateFormat: _this.props.dateFormat,
        onChange: _this.changeMonth,
        month: getMonth(_this.state.date),
        useShortMonthInDropdown: _this.props.useShortMonthInDropdown,
        accessibleMode: _this.props.accessibleMode,
        onDropdownToggle: _this.handleOnDropdownToggle,
        buttonRef: _this.setMonthRef
      });
    });

    _defineProperty(_assertThisInitialized(_this), "renderMonthYearDropdown", function () {
      var overrideHide = arguments.length > 0 && arguments[0] !== undefined ? arguments[0] : false;

      if (!_this.props.showMonthYearDropdown || overrideHide) {
        return;
      }

      return ___EmotionJSX(MonthYearDropdown, {
        dropdownMode: _this.props.dropdownMode,
        locale: _this.props.locale,
        dateFormat: _this.props.dateFormat,
        onChange: _this.changeMonthYear,
        minDate: _this.props.minDate,
        maxDate: _this.props.maxDate,
        date: _this.state.date,
        scrollableMonthYearDropdown: _this.props.scrollableMonthYearDropdown,
        accessibleMode: _this.props.accessibleMode
      });
    });

    _defineProperty(_assertThisInitialized(_this), "renderTodayButton", function () {
      if (!_this.props.todayButton || _this.props.showTimeSelectOnly) {
        return;
      }

      return ___EmotionJSX("div", {
        className: "react-datepicker__today-button",
        onClick: function onClick(e) {
          return _this.props.onSelect(getStartOfDate(now(_this.props.utcOffset)), e);
        }
      }, _this.props.todayButton);
    });

    _defineProperty(_assertThisInitialized(_this), "renderDefaultHeader", function (_ref) {
      var monthDate = _ref.monthDate,
          i = _ref.i;
      return ___EmotionJSX("div", {
        className: "react-datepicker__header"
      }, _this.renderCurrentMonth(monthDate), ___EmotionJSX("div", {
        className: "react-datepicker__header__dropdown react-datepicker__header__dropdown--".concat(_this.props.dropdownMode),
        onFocus: _this.handleDropdownFocus
      }, _this.renderMonthDropdown(i !== 0), _this.renderMonthYearDropdown(i !== 0), _this.renderYearDropdown(i !== 0)), ___EmotionJSX("div", {
        className: "react-datepicker__day-names"
      }, _this.header(monthDate)));
    });

    _defineProperty(_assertThisInitialized(_this), "renderCustomHeader", function (_ref2) {
      var monthDate = _ref2.monthDate,
          i = _ref2.i;

      if (i !== 0) {
        return null;
      }

      var prevMonthButtonDisabled = allDaysDisabledBefore(_this.state.date, "month", _this.props);
      var nextMonthButtonDisabled = allDaysDisabledAfter(_this.state.date, "month", _this.props);
      return ___EmotionJSX("div", {
        className: "react-datepicker__header react-datepicker__header--custom",
        onFocus: _this.props.onDropdownFocus
      }, _this.props.renderCustomHeader(_objectSpread(_objectSpread({}, _this.state), {}, {
        changeMonth: _this.changeMonth,
        changeYear: _this.changeYear,
        decreaseMonth: _this.decreaseMonth,
        increaseMonth: _this.increaseMonth,
        prevMonthButtonDisabled: prevMonthButtonDisabled,
        nextMonthButtonDisabled: nextMonthButtonDisabled
      })), ___EmotionJSX("div", {
        className: "react-datepicker__day-names"
      }, _this.header(monthDate)));
    });

    _defineProperty(_assertThisInitialized(_this), "renderMonths", function () {
      if (_this.props.showTimeSelectOnly) {
        return;
      }

      var monthList = [];

      for (var i = 0; i < _this.props.monthsShown; ++i) {
        var monthDate = addMonths(cloneDate(_this.state.date), i);
        var monthKey = "month-".concat(i);
        monthList.push(___EmotionJSX("div", {
          key: monthKey,
          ref: function ref(div) {
            _this.monthContainer = div;
          },
          className: "react-datepicker__month-container"
        }, _this.props.renderCustomHeader ? _this.renderCustomHeader({
          monthDate: monthDate,
          i: i
        }) : _this.renderDefaultHeader({
          monthDate: monthDate,
          i: i
        }), ___EmotionJSX(Month, {
          day: monthDate,
          dayClassName: _this.props.dayClassName,
          onDayClick: _this.handleDayClick,
          onDayMouseEnter: _this.handleDayMouseEnter,
          onMouseLeave: _this.handleMonthMouseLeave,
          onWeekSelect: _this.props.onWeekSelect,
          formatWeekNumber: _this.props.formatWeekNumber,
          minDate: _this.props.minDate,
          maxDate: _this.props.maxDate,
          excludeDates: _this.props.excludeDates,
          highlightDates: _this.props.highlightDates,
          selectingDate: _this.state.selectingDate,
          includeDates: _this.props.includeDates,
          inline: _this.props.inline,
          fixedHeight: _this.props.fixedHeight,
          filterDate: _this.props.filterDate,
          preSelection: _this.props.preSelection,
          selected: _this.props.selected,
          selectsStart: _this.props.selectsStart,
          selectsEnd: _this.props.selectsEnd,
          showWeekNumbers: _this.props.showWeekNumbers,
          startDate: _this.props.startDate,
          endDate: _this.props.endDate,
          peekNextMonth: _this.props.peekNextMonth,
          utcOffset: _this.props.utcOffset,
          setOpen: _this.props.setOpen,
          shouldCloseOnSelect: _this.props.shouldCloseOnSelect,
          renderDayContents: _this.props.renderDayContents,
          disabledKeyboardNavigation: _this.props.disabledKeyboardNavigation,
          updateSelection: _this.props.updateSelection,
          accessibleMode: _this.props.accessibleMode
        })));
      }

      return monthList;
    });

    _defineProperty(_assertThisInitialized(_this), "renderTimeSection", function () {
      if (_this.props.showTimeSelect && (_this.state.monthContainer || _this.props.showTimeSelectOnly)) {
        return ___EmotionJSX(Time, {
          selected: _this.props.selected,
          onChange: _this.props.onTimeChange,
          format: _this.props.timeFormat,
          includeTimes: _this.props.includeTimes,
          intervals: _this.props.timeIntervals,
          minTime: _this.props.minTime,
          maxTime: _this.props.maxTime,
          excludeTimes: _this.props.excludeTimes,
          timeCaption: _this.props.timeCaption,
          todayButton: _this.props.todayButton,
          showMonthDropdown: _this.props.showMonthDropdown,
          showMonthYearDropdown: _this.props.showMonthYearDropdown,
          showYearDropdown: _this.props.showYearDropdown,
          withPortal: _this.props.withPortal,
          monthRef: _this.state.monthContainer,
          injectTimes: _this.props.injectTimes,
          accessibleMode: _this.props.accessibleMode
        });
      }
    });

    _this.state = {
      date: _this.localizeDate(_this.getDateInView()),
      selectingDate: null,
      monthContainer: null,
      pauseFocusTrap: false
    };
    _this.monthRef = /*#__PURE__*/React.createRef();
    _this.yearRef = /*#__PURE__*/React.createRef();
    return _this;
  }

  _createClass(Calendar, [{
    key: "componentDidMount",
    value: function componentDidMount() {
      var _this2 = this;

      // monthContainer height is needed in time component
      // to determine the height for the ul in the time component
      // setState here so height is given after final component
      // layout is rendered
      if (this.props.showTimeSelect) {
        this.assignMonthContainer = function () {
          _this2.setState({
            monthContainer: _this2.monthContainer
          });
        }();
      }
    }
  }, {
    key: "componentDidUpdate",
    value: function componentDidUpdate(prevProps) {
      if (this.props.preSelection && !isSameDay(this.props.preSelection, prevProps.preSelection)) {
        this.setState({
          date: this.localizeDate(this.props.preSelection)
        });
      } else if (this.props.openToDate && !isSameDay(this.props.openToDate, prevProps.openToDate)) {
        this.setState({
          date: this.localizeDate(this.props.openToDate)
        });
      }
    }
  }, {
    key: "render",
    value: function render() {
      var Container = this.props.container || CalendarContainer;
      var trapFocus = this.props.accessibleMode && !this.props.inline;
      var initialFocusTarget = this.props.showTimeSelectOnly ? ".react-datepicker__time-box--accessible" : ".react-datepicker__month--accessible";

      if (trapFocus) {
        return ___EmotionJSX(Container, {
          className: classnames("react-datepicker", this.props.className, {
            "react-datepicker--time-only": this.props.showTimeSelectOnly
          })
        }, ___EmotionJSX(EuiFocusTrap, {
          disabled: this.state.pauseFocusTrap || !this.props.enableFocusTrap,
          className: "react-datepicker__focusTrap",
          initialFocus: initialFocusTarget,
          onClickOutside: this.handleClickOutside
        }, this.renderPreviousMonthButton(), this.renderNextMonthButton(), this.renderMonths(), this.renderTodayButton(), this.renderTimeSection(), this.props.children));
      } else {
        return ___EmotionJSX(Container, {
          className: classnames("react-datepicker", this.props.className, {
            "react-datepicker--time-only": this.props.showTimeSelectOnly
          })
        }, this.renderPreviousMonthButton(), this.renderNextMonthButton(), this.renderMonths(), this.renderTodayButton(), this.renderTimeSection(), this.props.children);
      }
    }
  }]);

  return Calendar;
}(React.Component);

_defineProperty(Calendar, "propTypes", {
  adjustDateOnChange: PropTypes.bool,
  className: PropTypes.string,
  children: PropTypes.node,
  container: PropTypes.func,
  dateFormat: PropTypes.oneOfType([PropTypes.string, PropTypes.array]).isRequired,
  dayClassName: PropTypes.func,
  disabledKeyboardNavigation: PropTypes.bool,
  dropdownMode: PropTypes.oneOf(["scroll", "select"]),
  endDate: PropTypes.object,
  excludeDates: PropTypes.array,
  filterDate: PropTypes.func,
  fixedHeight: PropTypes.bool,
  formatWeekNumber: PropTypes.func,
  highlightDates: PropTypes.instanceOf(Map),
  includeDates: PropTypes.array,
  includeTimes: PropTypes.array,
  injectTimes: PropTypes.array,
  inline: PropTypes.bool,
  locale: PropTypes.string,
  maxDate: PropTypes.object,
  minDate: PropTypes.object,
  monthsShown: PropTypes.number,
  onClickOutside: PropTypes.func.isRequired,
  onMonthChange: PropTypes.func,
  onYearChange: PropTypes.func,
  forceShowMonthNavigation: PropTypes.bool,
  onDropdownFocus: PropTypes.func,
  onSelect: PropTypes.func.isRequired,
  onWeekSelect: PropTypes.func,
  showTimeSelect: PropTypes.bool,
  showTimeSelectOnly: PropTypes.bool,
  timeFormat: PropTypes.string,
  timeIntervals: PropTypes.number,
  onTimeChange: PropTypes.func,
  minTime: PropTypes.object,
  maxTime: PropTypes.object,
  excludeTimes: PropTypes.array,
  timeCaption: PropTypes.string,
  openToDate: PropTypes.object,
  peekNextMonth: PropTypes.bool,
  scrollableYearDropdown: PropTypes.bool,
  scrollableMonthYearDropdown: PropTypes.bool,
  preSelection: PropTypes.object,
  selected: PropTypes.object,
  selectsEnd: PropTypes.bool,
  selectsStart: PropTypes.bool,
  showMonthDropdown: PropTypes.bool,
  showMonthYearDropdown: PropTypes.bool,
  showWeekNumbers: PropTypes.bool,
  showYearDropdown: PropTypes.bool,
  startDate: PropTypes.object,
  todayButton: PropTypes.node,
  useWeekdaysShort: PropTypes.bool,
  formatWeekDay: PropTypes.func,
  withPortal: PropTypes.bool,
  utcOffset: PropTypes.oneOfType([PropTypes.number, PropTypes.string]),
  weekLabel: PropTypes.string,
  yearDropdownItemNumber: PropTypes.number,
  setOpen: PropTypes.func,
  shouldCloseOnSelect: PropTypes.bool,
  useShortMonthInDropdown: PropTypes.bool,
  showDisabledMonthNavigation: PropTypes.bool,
  previousMonthButtonLabel: PropTypes.string,
  nextMonthButtonLabel: PropTypes.string,
  renderCustomHeader: PropTypes.func,
  renderDayContents: PropTypes.func,
  updateSelection: PropTypes.func.isRequired,
  accessibleMode: PropTypes.bool,
  enableFocusTrap: PropTypes.bool
});

export { Calendar as default };