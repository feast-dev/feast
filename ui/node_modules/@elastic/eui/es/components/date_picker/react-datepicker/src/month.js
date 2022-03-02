function _typeof(obj) { "@babel/helpers - typeof"; if (typeof Symbol === "function" && typeof Symbol.iterator === "symbol") { _typeof = function _typeof(obj) { return typeof obj; }; } else { _typeof = function _typeof(obj) { return obj && typeof Symbol === "function" && obj.constructor === Symbol && obj !== Symbol.prototype ? "symbol" : typeof obj; }; } return _typeof(obj); }

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
import React from "react";
import PropTypes from "prop-types";
import classnames from "classnames";
import Week from "./week";
import * as utils from "./date_utils";
import { EuiScreenReaderOnly } from '../../../accessibility';
import { jsx as ___EmotionJSX } from "@emotion/react";
var FIXED_HEIGHT_STANDARD_WEEK_COUNT = 6;

var Month = /*#__PURE__*/function (_React$Component) {
  _inherits(Month, _React$Component);

  var _super = _createSuper(Month);

  function Month(props) {
    var _this;

    _classCallCheck(this, Month);

    _this = _super.call(this, props);

    _defineProperty(_assertThisInitialized(_this), "handleDayClick", function (day, event) {
      if (_this.props.onDayClick) {
        _this.props.onDayClick(day, event);
      }
    });

    _defineProperty(_assertThisInitialized(_this), "handleDayMouseEnter", function (day) {
      if (_this.props.onDayMouseEnter) {
        _this.props.onDayMouseEnter(day);
      }
    });

    _defineProperty(_assertThisInitialized(_this), "handleMouseLeave", function () {
      if (_this.props.onMouseLeave) {
        _this.props.onMouseLeave();
      }
    });

    _defineProperty(_assertThisInitialized(_this), "onFocus", function () {
      if (_this.props.accessibleMode) {
        _this.setState({
          readInstructions: true
        });
      }
    });

    _defineProperty(_assertThisInitialized(_this), "onBlur", function () {
      if (_this.props.accessibleMode) {
        _this.setState({
          readInstructions: false
        });
      }
    });

    _defineProperty(_assertThisInitialized(_this), "onInputKeyDown", function (event) {
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

    _defineProperty(_assertThisInitialized(_this), "isWeekInMonth", function (startOfWeek) {
      var day = _this.props.day;
      var endOfWeek = utils.addDays(utils.cloneDate(startOfWeek), 6);
      return utils.isSameMonth(startOfWeek, day) || utils.isSameMonth(endOfWeek, day);
    });

    _defineProperty(_assertThisInitialized(_this), "renderWeeks", function () {
      var weeks = [];
      var isFixedHeight = _this.props.fixedHeight;
      var currentWeekStart = utils.getStartOfWeek(utils.getStartOfMonth(utils.cloneDate(_this.props.day)));
      var i = 0;
      var breakAfterNextPush = false;

      while (true) {
        weeks.push(___EmotionJSX(Week, {
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

    _defineProperty(_assertThisInitialized(_this), "getClassNames", function () {
      var _this$props = _this.props,
          selectingDate = _this$props.selectingDate,
          selectsStart = _this$props.selectsStart,
          selectsEnd = _this$props.selectsEnd;
      return classnames("react-datepicker__month", {
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

  _createClass(Month, [{
    key: "render",
    value: function render() {
      var screenReaderInstructions;

      if (this.state.readInstructions) {
        screenReaderInstructions = ___EmotionJSX("p", {
          "aria-live": true
        }, "You are focused on a calendar. Use the arrow keys to navigate the days in the month. Use the page up and down keys to navigate from month to month. Use the home and end keys to navigate from year to year.", this.props.preSelection ? "".concat(utils.formatDate(this.props.preSelection, this.dayFormat), " is the\n          currently focused date.") : "No date is currently focused.");
      }

      return ___EmotionJSX("div", {
        className: this.getClassNames(),
        onMouseLeave: this.handleMouseLeave,
        role: "listbox",
        "aria-label": this.props.day.format("MMMM, YYYY"),
        tabIndex: this.props.accessibleMode ? 0 : -1,
        onKeyDown: this.onInputKeyDown,
        onFocus: this.onFocus,
        onBlur: this.onBlur
      }, ___EmotionJSX(EuiScreenReaderOnly, null, ___EmotionJSX("span", null, screenReaderInstructions)), this.renderWeeks());
    }
  }]);

  return Month;
}(React.Component);

_defineProperty(Month, "propTypes", {
  disabledKeyboardNavigation: PropTypes.bool,
  day: PropTypes.object.isRequired,
  dayClassName: PropTypes.func,
  endDate: PropTypes.object,
  excludeDates: PropTypes.array,
  filterDate: PropTypes.func,
  fixedHeight: PropTypes.bool,
  formatWeekNumber: PropTypes.func,
  highlightDates: PropTypes.instanceOf(Map),
  includeDates: PropTypes.array,
  inline: PropTypes.bool,
  maxDate: PropTypes.object,
  minDate: PropTypes.object,
  onDayClick: PropTypes.func,
  onDayMouseEnter: PropTypes.func,
  onMouseLeave: PropTypes.func,
  onWeekSelect: PropTypes.func,
  peekNextMonth: PropTypes.bool,
  preSelection: PropTypes.object,
  selected: PropTypes.object,
  selectingDate: PropTypes.object,
  selectsEnd: PropTypes.bool,
  selectsStart: PropTypes.bool,
  showWeekNumbers: PropTypes.bool,
  startDate: PropTypes.object,
  setOpen: PropTypes.func,
  shouldCloseOnSelect: PropTypes.bool,
  utcOffset: PropTypes.oneOfType([PropTypes.number, PropTypes.string]),
  renderDayContents: PropTypes.func,
  updateSelection: PropTypes.func.isRequired,
  accessibleMode: PropTypes.bool
});

export { Month as default };