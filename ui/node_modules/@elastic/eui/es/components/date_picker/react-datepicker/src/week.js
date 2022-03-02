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
import Day from "./day";
import WeekNumber from "./week_number";
import * as utils from "./date_utils";
import { jsx as ___EmotionJSX } from "@emotion/react";

var Week = /*#__PURE__*/function (_React$Component) {
  _inherits(Week, _React$Component);

  var _super = _createSuper(Week);

  function Week() {
    var _this;

    _classCallCheck(this, Week);

    for (var _len = arguments.length, args = new Array(_len), _key = 0; _key < _len; _key++) {
      args[_key] = arguments[_key];
    }

    _this = _super.call.apply(_super, [this].concat(args));

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

    _defineProperty(_assertThisInitialized(_this), "handleWeekClick", function (day, weekNumber, event) {
      if (typeof _this.props.onWeekSelect === "function") {
        _this.props.onWeekSelect(day, weekNumber, event);
      }

      if (_this.props.shouldCloseOnSelect) {
        _this.props.setOpen(false);
      }
    });

    _defineProperty(_assertThisInitialized(_this), "formatWeekNumber", function (startOfWeek) {
      if (_this.props.formatWeekNumber) {
        return _this.props.formatWeekNumber(startOfWeek);
      }

      return utils.getWeek(startOfWeek);
    });

    _defineProperty(_assertThisInitialized(_this), "renderDays", function () {
      var startOfWeek = utils.getStartOfWeek(utils.cloneDate(_this.props.day));
      var days = [];

      var weekNumber = _this.formatWeekNumber(startOfWeek);

      if (_this.props.showWeekNumber) {
        var onClickAction = _this.props.onWeekSelect ? _this.handleWeekClick.bind(_assertThisInitialized(_this), startOfWeek, weekNumber) : undefined;
        days.push(___EmotionJSX(WeekNumber, {
          key: "W",
          weekNumber: weekNumber,
          onClick: onClickAction
        }));
      }

      return days.concat([0, 1, 2, 3, 4, 5, 6].map(function (offset) {
        var day = utils.addDays(utils.cloneDate(startOfWeek), offset);
        return ___EmotionJSX(Day, {
          key: offset,
          day: day,
          month: _this.props.month,
          onClick: _this.handleDayClick.bind(_assertThisInitialized(_this), day),
          onMouseEnter: _this.handleDayMouseEnter.bind(_assertThisInitialized(_this), day),
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
          startDate: _this.props.startDate,
          endDate: _this.props.endDate,
          dayClassName: _this.props.dayClassName,
          utcOffset: _this.props.utcOffset,
          renderDayContents: _this.props.renderDayContents,
          disabledKeyboardNavigation: _this.props.disabledKeyboardNavigation,
          accessibleMode: _this.props.accessibleMode
        });
      }));
    });

    return _this;
  }

  _createClass(Week, [{
    key: "render",
    value: function render() {
      return ___EmotionJSX("div", {
        className: "react-datepicker__week"
      }, this.renderDays());
    }
  }], [{
    key: "defaultProps",
    get: function get() {
      return {
        shouldCloseOnSelect: true
      };
    }
  }]);

  return Week;
}(React.Component);

_defineProperty(Week, "propTypes", {
  disabledKeyboardNavigation: PropTypes.bool,
  day: PropTypes.object.isRequired,
  dayClassName: PropTypes.func,
  endDate: PropTypes.object,
  excludeDates: PropTypes.array,
  filterDate: PropTypes.func,
  formatWeekNumber: PropTypes.func,
  highlightDates: PropTypes.instanceOf(Map),
  includeDates: PropTypes.array,
  inline: PropTypes.bool,
  maxDate: PropTypes.object,
  minDate: PropTypes.object,
  month: PropTypes.number,
  onDayClick: PropTypes.func,
  onDayMouseEnter: PropTypes.func,
  onWeekSelect: PropTypes.func,
  preSelection: PropTypes.object,
  selected: PropTypes.object,
  selectingDate: PropTypes.object,
  selectsEnd: PropTypes.bool,
  selectsStart: PropTypes.bool,
  showWeekNumber: PropTypes.bool,
  startDate: PropTypes.object,
  setOpen: PropTypes.func,
  shouldCloseOnSelect: PropTypes.bool,
  utcOffset: PropTypes.oneOfType([PropTypes.number, PropTypes.string]),
  renderDayContents: PropTypes.func,
  accessibleMode: PropTypes.bool
});

export { Week as default };