"use strict";

var _interopRequireDefault = require("@babel/runtime/helpers/interopRequireDefault");

Object.defineProperty(exports, "__esModule", {
  value: true
});
exports.default = void 0;

var _classCallCheck2 = _interopRequireDefault(require("@babel/runtime/helpers/classCallCheck"));

var _assertThisInitialized2 = _interopRequireDefault(require("@babel/runtime/helpers/assertThisInitialized"));

var _createClass2 = _interopRequireDefault(require("@babel/runtime/helpers/createClass"));

var _inherits2 = _interopRequireDefault(require("@babel/runtime/helpers/inherits"));

var _possibleConstructorReturn2 = _interopRequireDefault(require("@babel/runtime/helpers/possibleConstructorReturn"));

var _getPrototypeOf2 = _interopRequireDefault(require("@babel/runtime/helpers/getPrototypeOf"));

var _defineProperty2 = _interopRequireDefault(require("@babel/runtime/helpers/defineProperty"));

var _extends2 = _interopRequireDefault(require("@babel/runtime/helpers/extends"));

var _year_dropdown = _interopRequireDefault(require("./year_dropdown"));

var _month_dropdown = _interopRequireDefault(require("./month_dropdown"));

var _month_year_dropdown = _interopRequireDefault(require("./month_year_dropdown"));

var _month = _interopRequireDefault(require("./month"));

var _time = _interopRequireDefault(require("./time"));

var _react = _interopRequireDefault(require("react"));

var _propTypes = _interopRequireDefault(require("prop-types"));

var _classnames = _interopRequireDefault(require("classnames"));

var _calendar_container = _interopRequireDefault(require("./calendar_container"));

var _date_utils = require("./date_utils");

var _focus_trap = require("../../../focus_trap");

var _react2 = require("@emotion/react");

function ownKeys(object, enumerableOnly) { var keys = Object.keys(object); if (Object.getOwnPropertySymbols) { var symbols = Object.getOwnPropertySymbols(object); if (enumerableOnly) symbols = symbols.filter(function (sym) { return Object.getOwnPropertyDescriptor(object, sym).enumerable; }); keys.push.apply(keys, symbols); } return keys; }

function _objectSpread(target) { for (var i = 1; i < arguments.length; i++) { var source = arguments[i] != null ? arguments[i] : {}; if (i % 2) { ownKeys(Object(source), true).forEach(function (key) { (0, _defineProperty2.default)(target, key, source[key]); }); } else if (Object.getOwnPropertyDescriptors) { Object.defineProperties(target, Object.getOwnPropertyDescriptors(source)); } else { ownKeys(Object(source)).forEach(function (key) { Object.defineProperty(target, key, Object.getOwnPropertyDescriptor(source, key)); }); } } return target; }

function _createSuper(Derived) { var hasNativeReflectConstruct = _isNativeReflectConstruct(); return function _createSuperInternal() { var Super = (0, _getPrototypeOf2.default)(Derived), result; if (hasNativeReflectConstruct) { var NewTarget = (0, _getPrototypeOf2.default)(this).constructor; result = Reflect.construct(Super, arguments, NewTarget); } else { result = Super.apply(this, arguments); } return (0, _possibleConstructorReturn2.default)(this, result); }; }

function _isNativeReflectConstruct() { if (typeof Reflect === "undefined" || !Reflect.construct) return false; if (Reflect.construct.sham) return false; if (typeof Proxy === "function") return true; try { Date.prototype.toString.call(Reflect.construct(Date, [], function () {})); return true; } catch (e) { return false; } }

var FocusTrapContainer = /*#__PURE__*/_react.default.forwardRef(function (props, ref) {
  return (0, _react2.jsx)("div", (0, _extends2.default)({
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
  (0, _inherits2.default)(Calendar, _React$Component);

  var _super = _createSuper(Calendar);

  (0, _createClass2.default)(Calendar, null, [{
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

    (0, _classCallCheck2.default)(this, Calendar);
    _this = _super.call(this, props);
    (0, _defineProperty2.default)((0, _assertThisInitialized2.default)(_this), "setMonthRef", function (node) {
      _this.monthRef = node;
    });
    (0, _defineProperty2.default)((0, _assertThisInitialized2.default)(_this), "setYearRef", function (node) {
      _this.yearRef = node;
    });
    (0, _defineProperty2.default)((0, _assertThisInitialized2.default)(_this), "handleOnDropdownToggle", function (isOpen, dropdown) {
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
    (0, _defineProperty2.default)((0, _assertThisInitialized2.default)(_this), "handleClickOutside", function (event) {
      _this.props.onClickOutside(event);
    });
    (0, _defineProperty2.default)((0, _assertThisInitialized2.default)(_this), "handleDropdownFocus", function (event) {
      if (isDropdownSelect(event.target)) {
        _this.props.onDropdownFocus();
      }
    });
    (0, _defineProperty2.default)((0, _assertThisInitialized2.default)(_this), "getDateInView", function () {
      var _this$props = _this.props,
          preSelection = _this$props.preSelection,
          selected = _this$props.selected,
          openToDate = _this$props.openToDate,
          utcOffset = _this$props.utcOffset;
      var minDate = (0, _date_utils.getEffectiveMinDate)(_this.props);
      var maxDate = (0, _date_utils.getEffectiveMaxDate)(_this.props);
      var current = (0, _date_utils.now)(utcOffset);
      var initialDate = openToDate || selected || preSelection;

      if (initialDate) {
        return initialDate;
      } else {
        if (minDate && (0, _date_utils.isBefore)(current, minDate)) {
          return minDate;
        } else if (maxDate && (0, _date_utils.isAfter)(current, maxDate)) {
          return maxDate;
        }
      }

      return current;
    });
    (0, _defineProperty2.default)((0, _assertThisInitialized2.default)(_this), "localizeDate", function (date) {
      return (0, _date_utils.localizeDate)(date, _this.props.locale);
    });
    (0, _defineProperty2.default)((0, _assertThisInitialized2.default)(_this), "increaseMonth", function () {
      _this.setState({
        date: (0, _date_utils.addMonths)((0, _date_utils.cloneDate)(_this.state.date), 1)
      }, function () {
        return _this.handleMonthChange(_this.state.date);
      });
    });
    (0, _defineProperty2.default)((0, _assertThisInitialized2.default)(_this), "decreaseMonth", function () {
      _this.setState({
        date: (0, _date_utils.subtractMonths)((0, _date_utils.cloneDate)(_this.state.date), 1)
      }, function () {
        return _this.handleMonthChange(_this.state.date);
      });
    });
    (0, _defineProperty2.default)((0, _assertThisInitialized2.default)(_this), "handleDayClick", function (day, event) {
      return _this.props.onSelect(day, event);
    });
    (0, _defineProperty2.default)((0, _assertThisInitialized2.default)(_this), "handleDayMouseEnter", function (day) {
      return _this.setState({
        selectingDate: day
      });
    });
    (0, _defineProperty2.default)((0, _assertThisInitialized2.default)(_this), "handleMonthMouseLeave", function () {
      return _this.setState({
        selectingDate: null
      });
    });
    (0, _defineProperty2.default)((0, _assertThisInitialized2.default)(_this), "handleYearChange", function (date) {
      if (_this.props.onYearChange) {
        _this.props.onYearChange(date);
      }

      if (_this.props.accessibleMode) {
        _this.handleSelectionChange(date);
      }
    });
    (0, _defineProperty2.default)((0, _assertThisInitialized2.default)(_this), "handleMonthChange", function (date) {
      if (_this.props.onMonthChange) {
        _this.props.onMonthChange(date);
      }

      if (_this.props.accessibleMode) {
        _this.handleSelectionChange(date);
      }
    });
    (0, _defineProperty2.default)((0, _assertThisInitialized2.default)(_this), "handleSelectionChange", function (date) {
      if (_this.props.adjustDateOnChange) {
        _this.props.updateSelection(date);
      } else {
        _this.props.updateSelection((0, _date_utils.getStartOfMonth)((0, _date_utils.cloneDate)(date)));
      }
    });
    (0, _defineProperty2.default)((0, _assertThisInitialized2.default)(_this), "handleMonthYearChange", function (date) {
      _this.handleYearChange(date);

      _this.handleMonthChange(date);
    });
    (0, _defineProperty2.default)((0, _assertThisInitialized2.default)(_this), "changeYear", function (year) {
      _this.setState({
        date: (0, _date_utils.setYear)((0, _date_utils.cloneDate)(_this.state.date), year)
      }, function () {
        return _this.handleYearChange(_this.state.date);
      });
    });
    (0, _defineProperty2.default)((0, _assertThisInitialized2.default)(_this), "changeMonth", function (month) {
      _this.setState({
        date: (0, _date_utils.setMonth)((0, _date_utils.cloneDate)(_this.state.date), month)
      }, function () {
        return _this.handleMonthChange(_this.state.date);
      });
    });
    (0, _defineProperty2.default)((0, _assertThisInitialized2.default)(_this), "changeMonthYear", function (monthYear) {
      _this.setState({
        date: (0, _date_utils.setYear)((0, _date_utils.setMonth)((0, _date_utils.cloneDate)(_this.state.date), (0, _date_utils.getMonth)(monthYear)), (0, _date_utils.getYear)(monthYear))
      }, function () {
        return _this.handleMonthYearChange(_this.state.date);
      });
    });
    (0, _defineProperty2.default)((0, _assertThisInitialized2.default)(_this), "header", function () {
      var date = arguments.length > 0 && arguments[0] !== undefined ? arguments[0] : _this.state.date;
      var startOfWeek = (0, _date_utils.getStartOfWeek)((0, _date_utils.cloneDate)(date));
      var dayNames = [];

      if (_this.props.showWeekNumbers) {
        dayNames.push((0, _react2.jsx)("div", {
          key: "W",
          className: "react-datepicker__day-name"
        }, _this.props.weekLabel || "#"));
      }

      return dayNames.concat([0, 1, 2, 3, 4, 5, 6].map(function (offset) {
        var day = (0, _date_utils.addDays)((0, _date_utils.cloneDate)(startOfWeek), offset);
        var localeData = (0, _date_utils.getLocaleData)(day);

        var weekDayName = _this.formatWeekday(localeData, day);

        return (0, _react2.jsx)("div", {
          key: offset,
          className: "react-datepicker__day-name"
        }, weekDayName);
      }));
    });
    (0, _defineProperty2.default)((0, _assertThisInitialized2.default)(_this), "formatWeekday", function (localeData, day) {
      if (_this.props.formatWeekDay) {
        return (0, _date_utils.getFormattedWeekdayInLocale)(localeData, day, _this.props.formatWeekDay);
      }

      return _this.props.useWeekdaysShort ? (0, _date_utils.getWeekdayShortInLocale)(localeData, day) : (0, _date_utils.getWeekdayMinInLocale)(localeData, day);
    });
    (0, _defineProperty2.default)((0, _assertThisInitialized2.default)(_this), "renderPreviousMonthButton", function () {
      if (_this.props.renderCustomHeader) {
        return;
      }

      var allPrevDaysDisabled = (0, _date_utils.allDaysDisabledBefore)(_this.state.date, "month", _this.props);

      if (!_this.props.forceShowMonthNavigation && !_this.props.showDisabledMonthNavigation && allPrevDaysDisabled || _this.props.showTimeSelectOnly) {
        return;
      }

      var classes = ["react-datepicker__navigation", "react-datepicker__navigation--previous"];
      var clickHandler = _this.decreaseMonth;

      if (allPrevDaysDisabled && _this.props.showDisabledMonthNavigation) {
        classes.push("react-datepicker__navigation--previous--disabled");
        clickHandler = null;
      }

      return (0, _react2.jsx)("button", {
        type: "button",
        className: classes.join(" "),
        onClick: clickHandler
      }, _this.props.previousMonthButtonLabel);
    });
    (0, _defineProperty2.default)((0, _assertThisInitialized2.default)(_this), "renderNextMonthButton", function () {
      if (_this.props.renderCustomHeader) {
        return;
      }

      var allNextDaysDisabled = (0, _date_utils.allDaysDisabledAfter)(_this.state.date, "month", _this.props);

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

      return (0, _react2.jsx)("button", {
        type: "button",
        className: classes.join(" "),
        onClick: clickHandler
      }, _this.props.nextMonthButtonLabel);
    });
    (0, _defineProperty2.default)((0, _assertThisInitialized2.default)(_this), "renderCurrentMonth", function () {
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

      return (0, _react2.jsx)("div", {
        className: classes.join(" ")
      }, (0, _date_utils.formatDate)(date, _this.props.dateFormat));
    });
    (0, _defineProperty2.default)((0, _assertThisInitialized2.default)(_this), "renderYearDropdown", function () {
      var overrideHide = arguments.length > 0 && arguments[0] !== undefined ? arguments[0] : false;

      if (!_this.props.showYearDropdown || overrideHide) {
        return;
      }

      return (0, _react2.jsx)(_year_dropdown.default, {
        adjustDateOnChange: _this.props.adjustDateOnChange,
        date: _this.state.date,
        onSelect: _this.props.onSelect,
        setOpen: _this.props.setOpen,
        dropdownMode: _this.props.dropdownMode,
        onChange: _this.changeYear,
        minDate: _this.props.minDate,
        maxDate: _this.props.maxDate,
        year: (0, _date_utils.getYear)(_this.state.date),
        scrollableYearDropdown: _this.props.scrollableYearDropdown,
        yearDropdownItemNumber: _this.props.yearDropdownItemNumber,
        accessibleMode: _this.props.accessibleMode,
        onDropdownToggle: _this.handleOnDropdownToggle,
        buttonRef: _this.setYearRef
      });
    });
    (0, _defineProperty2.default)((0, _assertThisInitialized2.default)(_this), "renderMonthDropdown", function () {
      var overrideHide = arguments.length > 0 && arguments[0] !== undefined ? arguments[0] : false;

      if (!_this.props.showMonthDropdown || overrideHide) {
        return;
      }

      return (0, _react2.jsx)(_month_dropdown.default, {
        dropdownMode: _this.props.dropdownMode,
        locale: _this.props.locale,
        dateFormat: _this.props.dateFormat,
        onChange: _this.changeMonth,
        month: (0, _date_utils.getMonth)(_this.state.date),
        useShortMonthInDropdown: _this.props.useShortMonthInDropdown,
        accessibleMode: _this.props.accessibleMode,
        onDropdownToggle: _this.handleOnDropdownToggle,
        buttonRef: _this.setMonthRef
      });
    });
    (0, _defineProperty2.default)((0, _assertThisInitialized2.default)(_this), "renderMonthYearDropdown", function () {
      var overrideHide = arguments.length > 0 && arguments[0] !== undefined ? arguments[0] : false;

      if (!_this.props.showMonthYearDropdown || overrideHide) {
        return;
      }

      return (0, _react2.jsx)(_month_year_dropdown.default, {
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
    (0, _defineProperty2.default)((0, _assertThisInitialized2.default)(_this), "renderTodayButton", function () {
      if (!_this.props.todayButton || _this.props.showTimeSelectOnly) {
        return;
      }

      return (0, _react2.jsx)("div", {
        className: "react-datepicker__today-button",
        onClick: function onClick(e) {
          return _this.props.onSelect((0, _date_utils.getStartOfDate)((0, _date_utils.now)(_this.props.utcOffset)), e);
        }
      }, _this.props.todayButton);
    });
    (0, _defineProperty2.default)((0, _assertThisInitialized2.default)(_this), "renderDefaultHeader", function (_ref) {
      var monthDate = _ref.monthDate,
          i = _ref.i;
      return (0, _react2.jsx)("div", {
        className: "react-datepicker__header"
      }, _this.renderCurrentMonth(monthDate), (0, _react2.jsx)("div", {
        className: "react-datepicker__header__dropdown react-datepicker__header__dropdown--".concat(_this.props.dropdownMode),
        onFocus: _this.handleDropdownFocus
      }, _this.renderMonthDropdown(i !== 0), _this.renderMonthYearDropdown(i !== 0), _this.renderYearDropdown(i !== 0)), (0, _react2.jsx)("div", {
        className: "react-datepicker__day-names"
      }, _this.header(monthDate)));
    });
    (0, _defineProperty2.default)((0, _assertThisInitialized2.default)(_this), "renderCustomHeader", function (_ref2) {
      var monthDate = _ref2.monthDate,
          i = _ref2.i;

      if (i !== 0) {
        return null;
      }

      var prevMonthButtonDisabled = (0, _date_utils.allDaysDisabledBefore)(_this.state.date, "month", _this.props);
      var nextMonthButtonDisabled = (0, _date_utils.allDaysDisabledAfter)(_this.state.date, "month", _this.props);
      return (0, _react2.jsx)("div", {
        className: "react-datepicker__header react-datepicker__header--custom",
        onFocus: _this.props.onDropdownFocus
      }, _this.props.renderCustomHeader(_objectSpread(_objectSpread({}, _this.state), {}, {
        changeMonth: _this.changeMonth,
        changeYear: _this.changeYear,
        decreaseMonth: _this.decreaseMonth,
        increaseMonth: _this.increaseMonth,
        prevMonthButtonDisabled: prevMonthButtonDisabled,
        nextMonthButtonDisabled: nextMonthButtonDisabled
      })), (0, _react2.jsx)("div", {
        className: "react-datepicker__day-names"
      }, _this.header(monthDate)));
    });
    (0, _defineProperty2.default)((0, _assertThisInitialized2.default)(_this), "renderMonths", function () {
      if (_this.props.showTimeSelectOnly) {
        return;
      }

      var monthList = [];

      for (var i = 0; i < _this.props.monthsShown; ++i) {
        var monthDate = (0, _date_utils.addMonths)((0, _date_utils.cloneDate)(_this.state.date), i);
        var monthKey = "month-".concat(i);
        monthList.push((0, _react2.jsx)("div", {
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
        }), (0, _react2.jsx)(_month.default, {
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
    (0, _defineProperty2.default)((0, _assertThisInitialized2.default)(_this), "renderTimeSection", function () {
      if (_this.props.showTimeSelect && (_this.state.monthContainer || _this.props.showTimeSelectOnly)) {
        return (0, _react2.jsx)(_time.default, {
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
    _this.monthRef = /*#__PURE__*/_react.default.createRef();
    _this.yearRef = /*#__PURE__*/_react.default.createRef();
    return _this;
  }

  (0, _createClass2.default)(Calendar, [{
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
      if (this.props.preSelection && !(0, _date_utils.isSameDay)(this.props.preSelection, prevProps.preSelection)) {
        this.setState({
          date: this.localizeDate(this.props.preSelection)
        });
      } else if (this.props.openToDate && !(0, _date_utils.isSameDay)(this.props.openToDate, prevProps.openToDate)) {
        this.setState({
          date: this.localizeDate(this.props.openToDate)
        });
      }
    }
  }, {
    key: "render",
    value: function render() {
      var Container = this.props.container || _calendar_container.default;
      var trapFocus = this.props.accessibleMode && !this.props.inline;
      var initialFocusTarget = this.props.showTimeSelectOnly ? ".react-datepicker__time-box--accessible" : ".react-datepicker__month--accessible";

      if (trapFocus) {
        return (0, _react2.jsx)(Container, {
          className: (0, _classnames.default)("react-datepicker", this.props.className, {
            "react-datepicker--time-only": this.props.showTimeSelectOnly
          })
        }, (0, _react2.jsx)(_focus_trap.EuiFocusTrap, {
          disabled: this.state.pauseFocusTrap || !this.props.enableFocusTrap,
          className: "react-datepicker__focusTrap",
          initialFocus: initialFocusTarget,
          onClickOutside: this.handleClickOutside
        }, this.renderPreviousMonthButton(), this.renderNextMonthButton(), this.renderMonths(), this.renderTodayButton(), this.renderTimeSection(), this.props.children));
      } else {
        return (0, _react2.jsx)(Container, {
          className: (0, _classnames.default)("react-datepicker", this.props.className, {
            "react-datepicker--time-only": this.props.showTimeSelectOnly
          })
        }, this.renderPreviousMonthButton(), this.renderNextMonthButton(), this.renderMonths(), this.renderTodayButton(), this.renderTimeSection(), this.props.children);
      }
    }
  }]);
  return Calendar;
}(_react.default.Component);

exports.default = Calendar;
(0, _defineProperty2.default)(Calendar, "propTypes", {
  adjustDateOnChange: _propTypes.default.bool,
  className: _propTypes.default.string,
  children: _propTypes.default.node,
  container: _propTypes.default.func,
  dateFormat: _propTypes.default.oneOfType([_propTypes.default.string, _propTypes.default.array]).isRequired,
  dayClassName: _propTypes.default.func,
  disabledKeyboardNavigation: _propTypes.default.bool,
  dropdownMode: _propTypes.default.oneOf(["scroll", "select"]),
  endDate: _propTypes.default.object,
  excludeDates: _propTypes.default.array,
  filterDate: _propTypes.default.func,
  fixedHeight: _propTypes.default.bool,
  formatWeekNumber: _propTypes.default.func,
  highlightDates: _propTypes.default.instanceOf(Map),
  includeDates: _propTypes.default.array,
  includeTimes: _propTypes.default.array,
  injectTimes: _propTypes.default.array,
  inline: _propTypes.default.bool,
  locale: _propTypes.default.string,
  maxDate: _propTypes.default.object,
  minDate: _propTypes.default.object,
  monthsShown: _propTypes.default.number,
  onClickOutside: _propTypes.default.func.isRequired,
  onMonthChange: _propTypes.default.func,
  onYearChange: _propTypes.default.func,
  forceShowMonthNavigation: _propTypes.default.bool,
  onDropdownFocus: _propTypes.default.func,
  onSelect: _propTypes.default.func.isRequired,
  onWeekSelect: _propTypes.default.func,
  showTimeSelect: _propTypes.default.bool,
  showTimeSelectOnly: _propTypes.default.bool,
  timeFormat: _propTypes.default.string,
  timeIntervals: _propTypes.default.number,
  onTimeChange: _propTypes.default.func,
  minTime: _propTypes.default.object,
  maxTime: _propTypes.default.object,
  excludeTimes: _propTypes.default.array,
  timeCaption: _propTypes.default.string,
  openToDate: _propTypes.default.object,
  peekNextMonth: _propTypes.default.bool,
  scrollableYearDropdown: _propTypes.default.bool,
  scrollableMonthYearDropdown: _propTypes.default.bool,
  preSelection: _propTypes.default.object,
  selected: _propTypes.default.object,
  selectsEnd: _propTypes.default.bool,
  selectsStart: _propTypes.default.bool,
  showMonthDropdown: _propTypes.default.bool,
  showMonthYearDropdown: _propTypes.default.bool,
  showWeekNumbers: _propTypes.default.bool,
  showYearDropdown: _propTypes.default.bool,
  startDate: _propTypes.default.object,
  todayButton: _propTypes.default.node,
  useWeekdaysShort: _propTypes.default.bool,
  formatWeekDay: _propTypes.default.func,
  withPortal: _propTypes.default.bool,
  utcOffset: _propTypes.default.oneOfType([_propTypes.default.number, _propTypes.default.string]),
  weekLabel: _propTypes.default.string,
  yearDropdownItemNumber: _propTypes.default.number,
  setOpen: _propTypes.default.func,
  shouldCloseOnSelect: _propTypes.default.bool,
  useShortMonthInDropdown: _propTypes.default.bool,
  showDisabledMonthNavigation: _propTypes.default.bool,
  previousMonthButtonLabel: _propTypes.default.string,
  nextMonthButtonLabel: _propTypes.default.string,
  renderCustomHeader: _propTypes.default.func,
  renderDayContents: _propTypes.default.func,
  updateSelection: _propTypes.default.func.isRequired,
  accessibleMode: _propTypes.default.bool,
  enableFocusTrap: _propTypes.default.bool
});
module.exports = exports.default;