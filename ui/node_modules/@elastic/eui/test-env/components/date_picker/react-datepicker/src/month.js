"use strict";

var _interopRequireWildcard = require("@babel/runtime/helpers/interopRequireWildcard");

var _interopRequireDefault = require("@babel/runtime/helpers/interopRequireDefault");

Object.defineProperty(exports, "__esModule", {
  value: true
});
exports.default = void 0;

var _classCallCheck2 = _interopRequireDefault(require("@babel/runtime/helpers/classCallCheck"));

var _createClass2 = _interopRequireDefault(require("@babel/runtime/helpers/createClass"));

var _assertThisInitialized2 = _interopRequireDefault(require("@babel/runtime/helpers/assertThisInitialized"));

var _inherits2 = _interopRequireDefault(require("@babel/runtime/helpers/inherits"));

var _possibleConstructorReturn2 = _interopRequireDefault(require("@babel/runtime/helpers/possibleConstructorReturn"));

var _getPrototypeOf2 = _interopRequireDefault(require("@babel/runtime/helpers/getPrototypeOf"));

var _defineProperty2 = _interopRequireDefault(require("@babel/runtime/helpers/defineProperty"));

var _react = _interopRequireDefault(require("react"));

var _propTypes = _interopRequireDefault(require("prop-types"));

var _classnames = _interopRequireDefault(require("classnames"));

var _week = _interopRequireDefault(require("./week"));

var utils = _interopRequireWildcard(require("./date_utils"));

var _accessibility = require("../../../accessibility");

var _react2 = require("@emotion/react");

function _createSuper(Derived) { var hasNativeReflectConstruct = _isNativeReflectConstruct(); return function _createSuperInternal() { var Super = (0, _getPrototypeOf2.default)(Derived), result; if (hasNativeReflectConstruct) { var NewTarget = (0, _getPrototypeOf2.default)(this).constructor; result = Reflect.construct(Super, arguments, NewTarget); } else { result = Super.apply(this, arguments); } return (0, _possibleConstructorReturn2.default)(this, result); }; }

function _isNativeReflectConstruct() { if (typeof Reflect === "undefined" || !Reflect.construct) return false; if (Reflect.construct.sham) return false; if (typeof Proxy === "function") return true; try { Date.prototype.toString.call(Reflect.construct(Date, [], function () {})); return true; } catch (e) { return false; } }

var FIXED_HEIGHT_STANDARD_WEEK_COUNT = 6;

var Month = /*#__PURE__*/function (_React$Component) {
  (0, _inherits2.default)(Month, _React$Component);

  var _super = _createSuper(Month);

  function Month(props) {
    var _this;

    (0, _classCallCheck2.default)(this, Month);
    _this = _super.call(this, props);
    (0, _defineProperty2.default)((0, _assertThisInitialized2.default)(_this), "handleDayClick", function (day, event) {
      if (_this.props.onDayClick) {
        _this.props.onDayClick(day, event);
      }
    });
    (0, _defineProperty2.default)((0, _assertThisInitialized2.default)(_this), "handleDayMouseEnter", function (day) {
      if (_this.props.onDayMouseEnter) {
        _this.props.onDayMouseEnter(day);
      }
    });
    (0, _defineProperty2.default)((0, _assertThisInitialized2.default)(_this), "handleMouseLeave", function () {
      if (_this.props.onMouseLeave) {
        _this.props.onMouseLeave();
      }
    });
    (0, _defineProperty2.default)((0, _assertThisInitialized2.default)(_this), "onFocus", function () {
      if (_this.props.accessibleMode) {
        _this.setState({
          readInstructions: true
        });
      }
    });
    (0, _defineProperty2.default)((0, _assertThisInitialized2.default)(_this), "onBlur", function () {
      if (_this.props.accessibleMode) {
        _this.setState({
          readInstructions: false
        });
      }
    });
    (0, _defineProperty2.default)((0, _assertThisInitialized2.default)(_this), "onInputKeyDown", function (event) {
      var eventKey = event.key; // `preSelection` can be `null` but `day` is required. Use it as a fallback if necessary for invalid entries.

      var copy = _this.props.preSelection ? utils.newDate(_this.props.preSelection) : utils.newDate(_this.props.day);
      var newSelection;

      switch (eventKey) {
        case "ArrowLeft":
          newSelection = utils.subtractDays(copy, 1);
          break;

        case "ArrowRight":
          newSelection = utils.addDays(copy, 1);
          break;

        case "ArrowUp":
          newSelection = utils.subtractWeeks(copy, 1);
          break;

        case "ArrowDown":
          newSelection = utils.addWeeks(copy, 1);
          break;

        case "PageUp":
          newSelection = utils.subtractMonths(copy, 1);
          break;

        case "PageDown":
          newSelection = utils.addMonths(copy, 1);
          break;

        case "Home":
          newSelection = utils.subtractYears(copy, 1);
          break;

        case "End":
          newSelection = utils.addYears(copy, 1);
          break;

        case " ":
        case "Enter":
          event.preventDefault();

          _this.handleDayClick(copy, event);

          break;
      }

      if (!newSelection) return; // Let the input component handle this keydown

      event.preventDefault();

      _this.props.updateSelection(newSelection);
    });
    (0, _defineProperty2.default)((0, _assertThisInitialized2.default)(_this), "isWeekInMonth", function (startOfWeek) {
      var day = _this.props.day;
      var endOfWeek = utils.addDays(utils.cloneDate(startOfWeek), 6);
      return utils.isSameMonth(startOfWeek, day) || utils.isSameMonth(endOfWeek, day);
    });
    (0, _defineProperty2.default)((0, _assertThisInitialized2.default)(_this), "renderWeeks", function () {
      var weeks = [];
      var isFixedHeight = _this.props.fixedHeight;
      var currentWeekStart = utils.getStartOfWeek(utils.getStartOfMonth(utils.cloneDate(_this.props.day)));
      var i = 0;
      var breakAfterNextPush = false;

      while (true) {
        weeks.push((0, _react2.jsx)(_week.default, {
          key: i,
          day: currentWeekStart,
          month: utils.getMonth(_this.props.day),
          onDayClick: _this.handleDayClick,
          onDayMouseEnter: _this.handleDayMouseEnter,
          onWeekSelect: _this.props.onWeekSelect,
          formatWeekNumber: _this.props.formatWeekNumber,
          minDate: _this.props.minDate,
          maxDate: _this.props.maxDate,
          excludeDates: _this.props.excludeDates,
          includeDates: _this.props.includeDates,
          inline: _this.props.inline,
          highlightDates: _this.props.highlightDates,
          selectingDate: _this.props.selectingDate,
          filterDate: _this.props.filterDate,
          preSelection: _this.props.preSelection,
          selected: _this.props.selected,
          selectsStart: _this.props.selectsStart,
          selectsEnd: _this.props.selectsEnd,
          showWeekNumber: _this.props.showWeekNumbers,
          startDate: _this.props.startDate,
          endDate: _this.props.endDate,
          dayClassName: _this.props.dayClassName,
          utcOffset: _this.props.utcOffset,
          setOpen: _this.props.setOpen,
          shouldCloseOnSelect: _this.props.shouldCloseOnSelect,
          disabledKeyboardNavigation: _this.props.disabledKeyboardNavigation,
          renderDayContents: _this.props.renderDayContents,
          accessibleMode: _this.props.accessibleMode
        }));
        if (breakAfterNextPush) break;
        i++;
        currentWeekStart = utils.addWeeks(utils.cloneDate(currentWeekStart), 1); // If one of these conditions is true, we will either break on this week
        // or break on the next week

        var isFixedAndFinalWeek = isFixedHeight && i >= FIXED_HEIGHT_STANDARD_WEEK_COUNT;
        var isNonFixedAndOutOfMonth = !isFixedHeight && !_this.isWeekInMonth(currentWeekStart);

        if (isFixedAndFinalWeek || isNonFixedAndOutOfMonth) {
          if (_this.props.peekNextMonth) {
            breakAfterNextPush = true;
          } else {
            break;
          }
        }
      }

      return weeks;
    });
    (0, _defineProperty2.default)((0, _assertThisInitialized2.default)(_this), "getClassNames", function () {
      var _this$props = _this.props,
          selectingDate = _this$props.selectingDate,
          selectsStart = _this$props.selectsStart,
          selectsEnd = _this$props.selectsEnd;
      return (0, _classnames.default)("react-datepicker__month", {
        "react-datepicker__month--accessible": _this.props.accessibleMode,
        "react-datepicker__month--selecting-range": selectingDate && (selectsStart || selectsEnd)
      });
    });
    _this.dayFormat = "MMMM D, YYYY.";
    _this.state = {
      readInstructions: false
    };
    return _this;
  }

  (0, _createClass2.default)(Month, [{
    key: "render",
    value: function render() {
      var screenReaderInstructions;

      if (this.state.readInstructions) {
        screenReaderInstructions = (0, _react2.jsx)("p", {
          "aria-live": true
        }, "You are focused on a calendar. Use the arrow keys to navigate the days in the month. Use the page up and down keys to navigate from month to month. Use the home and end keys to navigate from year to year.", this.props.preSelection ? "".concat(utils.formatDate(this.props.preSelection, this.dayFormat), " is the\n          currently focused date.") : "No date is currently focused.");
      }

      return (0, _react2.jsx)("div", {
        className: this.getClassNames(),
        onMouseLeave: this.handleMouseLeave,
        role: "listbox",
        "aria-label": this.props.day.format("MMMM, YYYY"),
        tabIndex: this.props.accessibleMode ? 0 : -1,
        onKeyDown: this.onInputKeyDown,
        onFocus: this.onFocus,
        onBlur: this.onBlur
      }, (0, _react2.jsx)(_accessibility.EuiScreenReaderOnly, null, (0, _react2.jsx)("span", null, screenReaderInstructions)), this.renderWeeks());
    }
  }]);
  return Month;
}(_react.default.Component);

exports.default = Month;
(0, _defineProperty2.default)(Month, "propTypes", {
  disabledKeyboardNavigation: _propTypes.default.bool,
  day: _propTypes.default.object.isRequired,
  dayClassName: _propTypes.default.func,
  endDate: _propTypes.default.object,
  excludeDates: _propTypes.default.array,
  filterDate: _propTypes.default.func,
  fixedHeight: _propTypes.default.bool,
  formatWeekNumber: _propTypes.default.func,
  highlightDates: _propTypes.default.instanceOf(Map),
  includeDates: _propTypes.default.array,
  inline: _propTypes.default.bool,
  maxDate: _propTypes.default.object,
  minDate: _propTypes.default.object,
  onDayClick: _propTypes.default.func,
  onDayMouseEnter: _propTypes.default.func,
  onMouseLeave: _propTypes.default.func,
  onWeekSelect: _propTypes.default.func,
  peekNextMonth: _propTypes.default.bool,
  preSelection: _propTypes.default.object,
  selected: _propTypes.default.object,
  selectingDate: _propTypes.default.object,
  selectsEnd: _propTypes.default.bool,
  selectsStart: _propTypes.default.bool,
  showWeekNumbers: _propTypes.default.bool,
  startDate: _propTypes.default.object,
  setOpen: _propTypes.default.func,
  shouldCloseOnSelect: _propTypes.default.bool,
  utcOffset: _propTypes.default.oneOfType([_propTypes.default.number, _propTypes.default.string]),
  renderDayContents: _propTypes.default.func,
  updateSelection: _propTypes.default.func.isRequired,
  accessibleMode: _propTypes.default.bool
});
module.exports = exports.default;