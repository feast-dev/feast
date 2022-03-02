"use strict";

var _interopRequireDefault = require("@babel/runtime/helpers/interopRequireDefault");

Object.defineProperty(exports, "__esModule", {
  value: true
});
Object.defineProperty(exports, "CalendarContainer", {
  enumerable: true,
  get: function get() {
    return _calendar_container.default;
  }
});
exports.default = void 0;

var _extends2 = _interopRequireDefault(require("@babel/runtime/helpers/extends"));

var _classCallCheck2 = _interopRequireDefault(require("@babel/runtime/helpers/classCallCheck"));

var _assertThisInitialized2 = _interopRequireDefault(require("@babel/runtime/helpers/assertThisInitialized"));

var _createClass2 = _interopRequireDefault(require("@babel/runtime/helpers/createClass"));

var _inherits2 = _interopRequireDefault(require("@babel/runtime/helpers/inherits"));

var _possibleConstructorReturn2 = _interopRequireDefault(require("@babel/runtime/helpers/possibleConstructorReturn"));

var _getPrototypeOf2 = _interopRequireDefault(require("@babel/runtime/helpers/getPrototypeOf"));

var _defineProperty2 = _interopRequireDefault(require("@babel/runtime/helpers/defineProperty"));

var _calendar = _interopRequireDefault(require("./calendar"));

var _react = _interopRequireDefault(require("react"));

var _propTypes = _interopRequireDefault(require("prop-types"));

var _classnames2 = _interopRequireDefault(require("classnames"));

var _date_utils = require("./date_utils");

var _popover = require("../../../popover/popover");

var _react2 = require("@emotion/react");

var _calendar_container = _interopRequireDefault(require("./calendar_container"));

function _createSuper(Derived) { var hasNativeReflectConstruct = _isNativeReflectConstruct(); return function _createSuperInternal() { var Super = (0, _getPrototypeOf2.default)(Derived), result; if (hasNativeReflectConstruct) { var NewTarget = (0, _getPrototypeOf2.default)(this).constructor; result = Reflect.construct(Super, arguments, NewTarget); } else { result = Super.apply(this, arguments); } return (0, _possibleConstructorReturn2.default)(this, result); }; }

function _isNativeReflectConstruct() { if (typeof Reflect === "undefined" || !Reflect.construct) return false; if (Reflect.construct.sham) return false; if (typeof Proxy === "function") return true; try { Date.prototype.toString.call(Reflect.construct(Date, [], function () {})); return true; } catch (e) { return false; } }

var outsideClickIgnoreClass = "react-datepicker-ignore-onclickoutside"; // Compares dates year+month combinations

function hasPreSelectionChanged(date1, date2) {
  if (date1 && date2) {
    return (0, _date_utils.getMonth)(date1) !== (0, _date_utils.getMonth)(date2) || (0, _date_utils.getYear)(date1) !== (0, _date_utils.getYear)(date2);
  }

  return date1 !== date2;
}

function hasSelectionChanged(date1, date2) {
  if (date1 && date2) {
    return !(0, _date_utils.equals)(date1, date2);
  }

  return false;
}
/**
 * General datepicker component.
 */


var INPUT_ERR_1 = "Date input not valid.";

var DatePicker = /*#__PURE__*/function (_React$Component) {
  (0, _inherits2.default)(DatePicker, _React$Component);

  var _super = _createSuper(DatePicker);

  (0, _createClass2.default)(DatePicker, null, [{
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

    (0, _classCallCheck2.default)(this, DatePicker);
    _this = _super.call(this, props);
    (0, _defineProperty2.default)((0, _assertThisInitialized2.default)(_this), "getPreSelection", function () {
      return _this.props.openToDate ? (0, _date_utils.newDate)(_this.props.openToDate) : _this.props.selectsEnd && _this.props.startDate ? (0, _date_utils.newDate)(_this.props.startDate) : _this.props.selectsStart && _this.props.endDate ? (0, _date_utils.newDate)(_this.props.endDate) : (0, _date_utils.now)(_this.props.utcOffset);
    });
    (0, _defineProperty2.default)((0, _assertThisInitialized2.default)(_this), "calcInitialState", function () {
      var defaultPreSelection = _this.getPreSelection();

      var minDate = (0, _date_utils.getEffectiveMinDate)(_this.props);
      var maxDate = (0, _date_utils.getEffectiveMaxDate)(_this.props);
      var boundedPreSelection = minDate && (0, _date_utils.isBefore)(defaultPreSelection, minDate) ? minDate : maxDate && (0, _date_utils.isAfter)(defaultPreSelection, maxDate) ? maxDate : defaultPreSelection;
      return {
        open: _this.props.startOpen || false,
        preventFocus: false,
        preSelection: _this.props.selected ? (0, _date_utils.newDate)(_this.props.selected) : boundedPreSelection,
        // transforming highlighted days (perhaps nested array)
        // to flat Map for faster access in day.jsx
        highlightDates: (0, _date_utils.getHightLightDaysMap)(_this.props.highlightDates),
        focused: false,
        // We attempt to handle focus trap activation manually,
        // but that is not possible with custom inputs like buttons.
        // Err on the side of a11y and trap focus when we can't be certain
        // that the trigger comoponent will work with our keyDown logic.
        enableFocusTrap: _this.props.customInput && _this.props.customInput.type !== 'input' ? true : false
      };
    });
    (0, _defineProperty2.default)((0, _assertThisInitialized2.default)(_this), "clearPreventFocusTimeout", function () {
      if (_this.preventFocusTimeout) {
        clearTimeout(_this.preventFocusTimeout);
      }
    });
    (0, _defineProperty2.default)((0, _assertThisInitialized2.default)(_this), "setFocus", function () {
      if (_this.input && _this.input.focus) {
        _this.input.focus();
      }
    });
    (0, _defineProperty2.default)((0, _assertThisInitialized2.default)(_this), "setBlur", function () {
      if (_this.input && _this.input.blur) {
        _this.input.blur();
      }

      if (_this.props.onBlur) {
        _this.props.onBlur();
      }

      _this.cancelFocusInput();
    });
    (0, _defineProperty2.default)((0, _assertThisInitialized2.default)(_this), "setOpen", function (open) {
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
    (0, _defineProperty2.default)((0, _assertThisInitialized2.default)(_this), "inputOk", function () {
      return (0, _date_utils.isMoment)(_this.state.preSelection) || (0, _date_utils.isDate)(_this.state.preSelection);
    });
    (0, _defineProperty2.default)((0, _assertThisInitialized2.default)(_this), "isCalendarOpen", function () {
      return _this.props.open === undefined ? _this.state.open && !_this.props.disabled && !_this.props.readOnly : _this.props.open;
    });
    (0, _defineProperty2.default)((0, _assertThisInitialized2.default)(_this), "handleFocus", function (event) {
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
    (0, _defineProperty2.default)((0, _assertThisInitialized2.default)(_this), "cancelFocusInput", function () {
      clearTimeout(_this.inputFocusTimeout);
      _this.inputFocusTimeout = null;
    });
    (0, _defineProperty2.default)((0, _assertThisInitialized2.default)(_this), "deferFocusInput", function () {
      _this.cancelFocusInput();

      _this.inputFocusTimeout = setTimeout(function () {
        return _this.setFocus();
      }, 1);
    });
    (0, _defineProperty2.default)((0, _assertThisInitialized2.default)(_this), "handleDropdownFocus", function () {
      _this.cancelFocusInput();
    });
    (0, _defineProperty2.default)((0, _assertThisInitialized2.default)(_this), "handleBlur", function (event) {
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
    (0, _defineProperty2.default)((0, _assertThisInitialized2.default)(_this), "handleCalendarClickOutside", function (event) {
      if (!_this.props.inline) {
        _this.setOpen(false, true);
      }

      _this.props.onClickOutside(event);

      if (_this.props.withPortal) {
        event.preventDefault();
      }
    });
    (0, _defineProperty2.default)((0, _assertThisInitialized2.default)(_this), "handleChange", function () {
      for (var _len = arguments.length, allArgs = new Array(_len), _key = 0; _key < _len; _key++) {
        allArgs[_key] = arguments[_key];
      }

      var event = allArgs[0];

      if (_this.props.onChangeRaw) {
        _this.props.onChangeRaw.apply((0, _assertThisInitialized2.default)(_this), allArgs);

        if (typeof event.isDefaultPrevented !== "function" || event.isDefaultPrevented()) {
          return;
        }
      }

      _this.setState({
        inputValue: event.target.value,
        lastPreSelectChange: PRESELECT_CHANGE_VIA_INPUT
      });

      var date = (0, _date_utils.parseDate)(event.target.value, _this.props);

      if (date || !event.target.value) {
        _this.setSelected(date, event, true);
      }
    });
    (0, _defineProperty2.default)((0, _assertThisInitialized2.default)(_this), "handleSelect", function (date, event) {
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
    (0, _defineProperty2.default)((0, _assertThisInitialized2.default)(_this), "updateSelection", function (newSelection) {
      if (_this.props.adjustDateOnChange) {
        _this.setSelected(newSelection);
      }

      _this.setPreSelection(newSelection);
    });
    (0, _defineProperty2.default)((0, _assertThisInitialized2.default)(_this), "setSelected", function (date, event, keepInput) {
      var changedDate = date;

      if (changedDate !== null && (0, _date_utils.isDayDisabled)(changedDate, _this.props)) {
        if ((0, _date_utils.isOutOfBounds)(changedDate, _this.props)) {
          _this.props.onChange(date, event);

          _this.props.onSelect(changedDate, event);
        }

        return;
      }

      if (changedDate !== null && _this.props.selected) {
        var selected = _this.props.selected;
        if (keepInput) selected = (0, _date_utils.newDate)(changedDate);
        changedDate = (0, _date_utils.setTime)((0, _date_utils.newDate)(changedDate), {
          hour: (0, _date_utils.getHour)(selected),
          minute: (0, _date_utils.getMinute)(selected),
          second: (0, _date_utils.getSecond)(selected),
          millisecond: (0, _date_utils.getMillisecond)(selected)
        });
      }

      if (!(0, _date_utils.isSameTime)(_this.props.selected, changedDate) || _this.props.allowSameDay) {
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
    (0, _defineProperty2.default)((0, _assertThisInitialized2.default)(_this), "setPreSelection", function (date) {
      var isDateRangePresent = typeof _this.props.minDate !== "undefined" && typeof _this.props.maxDate !== "undefined";
      var isValidDateSelection = isDateRangePresent && date ? (0, _date_utils.isDayInRange)(date, _this.props.minDate, _this.props.maxDate) : true;

      if (isValidDateSelection) {
        _this.setState({
          preSelection: date
        });
      }
    });
    (0, _defineProperty2.default)((0, _assertThisInitialized2.default)(_this), "handleTimeChange", function (time) {
      var selected = _this.props.selected ? _this.props.selected : _this.getPreSelection();
      var changedDate = (0, _date_utils.setTime)((0, _date_utils.cloneDate)(selected), {
        hour: (0, _date_utils.getHour)(time),
        minute: (0, _date_utils.getMinute)(time),
        second: 0,
        millisecond: 0
      });

      if (!(0, _date_utils.isSameTime)(selected, changedDate)) {
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
    (0, _defineProperty2.default)((0, _assertThisInitialized2.default)(_this), "onInputClick", function () {
      if (!_this.props.disabled && !_this.props.readOnly) {
        _this.setOpen(true);
      }

      _this.props.onInputClick();
    });
    (0, _defineProperty2.default)((0, _assertThisInitialized2.default)(_this), "onAccessibleModeButtonKeyDown", function (event) {
      var eventKey = event.key;

      if (eventKey === " " || eventKey === "Enter") {
        event.preventDefault();

        _this.onInputClick();
      }
    });
    (0, _defineProperty2.default)((0, _assertThisInitialized2.default)(_this), "onInputKeyDown", function (event) {
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

      var copy = (0, _date_utils.newDate)(_this.state.preSelection);

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
            newSelection = (0, _date_utils.subtractDays)(copy, 1);
            break;

          case "ArrowRight":
            newSelection = (0, _date_utils.addDays)(copy, 1);
            break;

          case "ArrowUp":
            newSelection = (0, _date_utils.subtractWeeks)(copy, 1);
            break;

          case "ArrowDown":
            newSelection = (0, _date_utils.addWeeks)(copy, 1);
            break;

          case "PageUp":
            newSelection = (0, _date_utils.subtractMonths)(copy, 1);
            break;

          case "PageDown":
            newSelection = (0, _date_utils.addMonths)(copy, 1);
            break;

          case "Home":
            newSelection = (0, _date_utils.subtractYears)(copy, 1);
            break;

          case "End":
            newSelection = (0, _date_utils.addYears)(copy, 1);
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
    (0, _defineProperty2.default)((0, _assertThisInitialized2.default)(_this), "onClearClick", function (event) {
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
    (0, _defineProperty2.default)((0, _assertThisInitialized2.default)(_this), "clear", function () {
      _this.onClearClick();
    });
    (0, _defineProperty2.default)((0, _assertThisInitialized2.default)(_this), "renderCalendar", function () {
      if (!_this.props.inline && !_this.isCalendarOpen()) {
        return null;
      }

      var calendar = (0, _react2.jsx)(_calendar.default, {
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
    (0, _defineProperty2.default)((0, _assertThisInitialized2.default)(_this), "renderDateInput", function () {
      var _React$cloneElement;

      var className = (0, _classnames2.default)(_this.props.className, (0, _defineProperty2.default)({}, outsideClickIgnoreClass, _this.state.open));
      var customInput = _this.props.customInput || (0, _react2.jsx)("input", {
        type: "text"
      });
      var customInputRef = _this.props.customInputRef || "ref";
      var inputValue = typeof _this.props.value === "string" ? _this.props.value : typeof _this.state.inputValue === "string" ? _this.state.inputValue : (0, _date_utils.safeDateFormat)(_this.props.selected, _this.props);
      return /*#__PURE__*/_react.default.cloneElement(customInput, (_React$cloneElement = {}, (0, _defineProperty2.default)(_React$cloneElement, customInputRef, function (input) {
        _this.input = input;
      }), (0, _defineProperty2.default)(_React$cloneElement, "value", inputValue), (0, _defineProperty2.default)(_React$cloneElement, "onBlur", _this.handleBlur), (0, _defineProperty2.default)(_React$cloneElement, "onChange", _this.handleChange), (0, _defineProperty2.default)(_React$cloneElement, "onClick", _this.onInputClick), (0, _defineProperty2.default)(_React$cloneElement, "onFocus", _this.handleFocus), (0, _defineProperty2.default)(_React$cloneElement, "onKeyDown", _this.onInputKeyDown), (0, _defineProperty2.default)(_React$cloneElement, "id", _this.props.id), (0, _defineProperty2.default)(_React$cloneElement, "name", _this.props.name), (0, _defineProperty2.default)(_React$cloneElement, "autoFocus", _this.props.autoFocus), (0, _defineProperty2.default)(_React$cloneElement, "placeholder", _this.props.placeholderText), (0, _defineProperty2.default)(_React$cloneElement, "disabled", _this.props.disabled), (0, _defineProperty2.default)(_React$cloneElement, "autoComplete", _this.props.autoComplete), (0, _defineProperty2.default)(_React$cloneElement, "className", className), (0, _defineProperty2.default)(_React$cloneElement, "title", _this.props.title), (0, _defineProperty2.default)(_React$cloneElement, "readOnly", _this.props.readOnly), (0, _defineProperty2.default)(_React$cloneElement, "required", _this.props.required), (0, _defineProperty2.default)(_React$cloneElement, "tabIndex", _this.props.tabIndex), (0, _defineProperty2.default)(_React$cloneElement, "aria-label", _this.state.open ? 'Press the down key to enter a popover containing a calendar. Press the escape key to close the popover.' : 'Press the down key to open a popover containing a calendar.'), _React$cloneElement));
    });
    (0, _defineProperty2.default)((0, _assertThisInitialized2.default)(_this), "renderClearButton", function () {
      if (_this.props.isClearable && _this.props.selected != null) {
        return (0, _react2.jsx)("button", {
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
    (0, _defineProperty2.default)((0, _assertThisInitialized2.default)(_this), "renderAccessibleButton", function () {
      if (_this.props.accessibleModeButton != null) {
        return /*#__PURE__*/_react.default.cloneElement(_this.props.accessibleModeButton, {
          onClick: _this.onInputClick,
          onKeyDown: _this.onAccessibleModeButtonKeyDown,
          className: (0, _classnames2.default)(_this.props.accessibleModeButton.props.className, "react-datepicker__accessible-icon"),
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

  (0, _createClass2.default)(DatePicker, [{
    key: "componentDidUpdate",
    value: function componentDidUpdate(prevProps, prevState) {
      if (prevProps.inline && hasPreSelectionChanged(prevProps.selected, this.props.selected)) {
        this.setPreSelection(this.props.selected);
      }

      if (prevProps.highlightDates !== this.props.highlightDates) {
        this.setState({
          highlightDates: (0, _date_utils.getHightLightDaysMap)(this.props.highlightDates)
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
        return (0, _react2.jsx)("div", null, !this.props.inline ? (0, _react2.jsx)("div", {
          className: "react-datepicker__input-container"
        }, this.renderDateInput(), this.renderClearButton(), this.renderAccessibleButton()) : null, this.state.open || this.props.inline ? (0, _react2.jsx)("div", {
          className: "react-datepicker__portal"
        }, calendar) : null);
      }

      return (0, _react2.jsx)(_popover.EuiPopover, (0, _extends2.default)({
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
        button: (0, _react2.jsx)("div", {
          className: "react-datepicker__input-container"
        }, this.renderDateInput(), this.renderClearButton(), this.renderAccessibleButton())
      }), calendar);
    }
  }]);
  return DatePicker;
}(_react.default.Component);

exports.default = DatePicker;
(0, _defineProperty2.default)(DatePicker, "propTypes", {
  adjustDateOnChange: _propTypes.default.bool,
  allowSameDay: _propTypes.default.bool,
  autoComplete: _propTypes.default.string,
  autoFocus: _propTypes.default.bool,
  calendarClassName: _propTypes.default.string,
  calendarContainer: _propTypes.default.func,
  children: _propTypes.default.node,
  className: _propTypes.default.string,
  customInput: _propTypes.default.element,
  customInputRef: _propTypes.default.string,
  // eslint-disable-next-line react/no-unused-prop-types
  dateFormat: _propTypes.default.oneOfType([_propTypes.default.string, _propTypes.default.array]),
  dateFormatCalendar: _propTypes.default.string,
  dayClassName: _propTypes.default.func,
  disabled: _propTypes.default.bool,
  disabledKeyboardNavigation: _propTypes.default.bool,
  dropdownMode: _propTypes.default.oneOf(["scroll", "select"]).isRequired,
  endDate: _propTypes.default.object,
  excludeDates: _propTypes.default.array,
  filterDate: _propTypes.default.func,
  fixedHeight: _propTypes.default.bool,
  formatWeekNumber: _propTypes.default.func,
  highlightDates: _propTypes.default.array,
  id: _propTypes.default.string,
  includeDates: _propTypes.default.array,
  includeTimes: _propTypes.default.array,
  injectTimes: _propTypes.default.array,
  inline: _propTypes.default.bool,
  isClearable: _propTypes.default.bool,
  locale: _propTypes.default.string,
  maxDate: _propTypes.default.object,
  minDate: _propTypes.default.object,
  monthsShown: _propTypes.default.number,
  name: _propTypes.default.string,
  onBlur: _propTypes.default.func,
  onChange: _propTypes.default.func.isRequired,
  onSelect: _propTypes.default.func,
  onWeekSelect: _propTypes.default.func,
  onClickOutside: _propTypes.default.func,
  onChangeRaw: _propTypes.default.func,
  onFocus: _propTypes.default.func,
  onInputClick: _propTypes.default.func,
  onKeyDown: _propTypes.default.func,
  onMonthChange: _propTypes.default.func,
  onYearChange: _propTypes.default.func,
  onInputError: _propTypes.default.func,
  open: _propTypes.default.bool,
  openToDate: _propTypes.default.object,
  peekNextMonth: _propTypes.default.bool,
  placeholderText: _propTypes.default.string,
  popperContainer: _propTypes.default.func,
  popperClassName: _propTypes.default.string,
  // <PopperComponent/> props
  popperModifiers: _propTypes.default.object,
  // <PopperComponent/> props
  popperPlacement: _propTypes.default.oneOf(_popover.popoverAnchorPosition),
  popperProps: _propTypes.default.object,
  preventOpenOnFocus: _propTypes.default.bool,
  readOnly: _propTypes.default.bool,
  required: _propTypes.default.bool,
  scrollableYearDropdown: _propTypes.default.bool,
  scrollableMonthYearDropdown: _propTypes.default.bool,
  selected: _propTypes.default.object,
  selectsEnd: _propTypes.default.bool,
  selectsStart: _propTypes.default.bool,
  showMonthDropdown: _propTypes.default.bool,
  showMonthYearDropdown: _propTypes.default.bool,
  showWeekNumbers: _propTypes.default.bool,
  showYearDropdown: _propTypes.default.bool,
  forceShowMonthNavigation: _propTypes.default.bool,
  showDisabledMonthNavigation: _propTypes.default.bool,
  startDate: _propTypes.default.object,
  startOpen: _propTypes.default.bool,
  tabIndex: _propTypes.default.number,
  timeCaption: _propTypes.default.string,
  title: _propTypes.default.string,
  todayButton: _propTypes.default.node,
  useWeekdaysShort: _propTypes.default.bool,
  formatWeekDay: _propTypes.default.func,
  utcOffset: _propTypes.default.oneOfType([_propTypes.default.number, _propTypes.default.string]),
  value: _propTypes.default.string,
  weekLabel: _propTypes.default.string,
  withPortal: _propTypes.default.bool,
  yearDropdownItemNumber: _propTypes.default.number,
  shouldCloseOnSelect: _propTypes.default.bool,
  showTimeSelect: _propTypes.default.bool,
  showTimeSelectOnly: _propTypes.default.bool,
  timeFormat: _propTypes.default.string,
  timeIntervals: _propTypes.default.number,
  minTime: _propTypes.default.object,
  maxTime: _propTypes.default.object,
  excludeTimes: _propTypes.default.array,
  useShortMonthInDropdown: _propTypes.default.bool,
  clearButtonTitle: _propTypes.default.string,
  previousMonthButtonLabel: _propTypes.default.string,
  nextMonthButtonLabel: _propTypes.default.string,
  renderCustomHeader: _propTypes.default.func,
  renderDayContents: _propTypes.default.func,
  accessibleMode: _propTypes.default.bool,
  accessibleModeButton: _propTypes.default.element,
  strictParsing: _propTypes.default.bool // eslint-disable-line react/no-unused-prop-types

});
var PRESELECT_CHANGE_VIA_INPUT = "input";
var PRESELECT_CHANGE_VIA_NAVIGATE = "navigate";