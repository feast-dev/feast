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
import MonthYearDropdownOptions from "./month_year_dropdown_options";
import { addMonths, formatDate, getStartOfMonth, isAfter, isSameMonth, isSameYear, localizeDate, newDate } from "./date_utils";
import { jsx as ___EmotionJSX } from "@emotion/react";

var MonthYearDropdown = /*#__PURE__*/function (_React$Component) {
  _inherits(MonthYearDropdown, _React$Component);

  var _super = _createSuper(MonthYearDropdown);

  function MonthYearDropdown() {
    var _this;

    _classCallCheck(this, MonthYearDropdown);

    for (var _len = arguments.length, args = new Array(_len), _key = 0; _key < _len; _key++) {
      args[_key] = arguments[_key];
    }

    _this = _super.call.apply(_super, [this].concat(args));

    _defineProperty(_assertThisInitialized(_this), "state", {
      dropdownVisible: false
    });

    _defineProperty(_assertThisInitialized(_this), "setReadViewRef", function (ref) {
      _this.readViewref = ref;
    });

    _defineProperty(_assertThisInitialized(_this), "onReadViewKeyDown", function (event) {
      var eventKey = event.key;

      switch (eventKey) {
        case " ":
        case "Enter":
          event.preventDefault();
          event.stopPropagation();

          _this.toggleDropdown();

          break;
      }
    });

    _defineProperty(_assertThisInitialized(_this), "onDropDownKeyDown", function (event) {
      var eventKey = event.key;

      switch (eventKey) {
        case " ":
        case "Enter":
          event.preventDefault();
          event.stopPropagation();

          _this.toggleDropdown();

          break;
      }
    });

    _defineProperty(_assertThisInitialized(_this), "renderSelectOptions", function () {
      var currDate = getStartOfMonth(localizeDate(_this.props.minDate, _this.props.locale));
      var lastDate = getStartOfMonth(localizeDate(_this.props.maxDate, _this.props.locale));
      var options = [];

      while (!isAfter(currDate, lastDate)) {
        var timepoint = currDate.valueOf();
        options.push(___EmotionJSX("option", {
          key: timepoint,
          value: timepoint
        }, formatDate(currDate, _this.props.dateFormat)));
        addMonths(currDate, 1);
      }

      return options;
    });

    _defineProperty(_assertThisInitialized(_this), "onSelectChange", function (e) {
      _this.onChange(e.target.value);
    });

    _defineProperty(_assertThisInitialized(_this), "renderSelectMode", function () {
      return ___EmotionJSX("select", {
        value: getStartOfMonth(_this.props.date).valueOf(),
        className: "react-datepicker__month-year-select",
        onChange: _this.onSelectChange
      }, _this.renderSelectOptions());
    });

    _defineProperty(_assertThisInitialized(_this), "renderReadView", function (visible) {
      var yearMonth = formatDate(localizeDate(newDate(_this.props.date), _this.props.locale), _this.props.dateFormat);
      return ___EmotionJSX("div", {
        key: "read",
        ref: _this.setReadViewRef,
        style: {
          visibility: visible ? "visible" : "hidden"
        },
        className: "react-datepicker__month-year-read-view",
        onClick: function onClick(event) {
          return _this.toggleDropdown(event);
        },
        onKeyDown: _this.onReadViewKeyDown,
        tabIndex: _this.props.accessibleMode ? "0" : undefined,
        "aria-label": "Button. Open the month selector. ".concat(yearMonth, " is currently selected.")
      }, ___EmotionJSX("span", {
        className: "react-datepicker__month-year-read-view--down-arrow"
      }), ___EmotionJSX("span", {
        className: "react-datepicker__month-year-read-view--selected-month-year"
      }, yearMonth));
    });

    _defineProperty(_assertThisInitialized(_this), "renderDropdown", function () {
      return ___EmotionJSX(MonthYearDropdownOptions, {
        key: "dropdown",
        ref: "options",
        date: _this.props.date,
        dateFormat: _this.props.dateFormat,
        onChange: _this.onChange,
        onCancel: _this.toggleDropdown,
        minDate: localizeDate(_this.props.minDate, _this.props.locale),
        maxDate: localizeDate(_this.props.maxDate, _this.props.locale),
        scrollableMonthYearDropdown: _this.props.scrollableMonthYearDropdown,
        accessibleMode: _this.props.accessibleMode
      });
    });

    _defineProperty(_assertThisInitialized(_this), "renderScrollMode", function () {
      var dropdownVisible = _this.state.dropdownVisible;
      var result = [_this.renderReadView(!dropdownVisible)];

      if (dropdownVisible) {
        result.unshift(_this.renderDropdown());
      }

      return result;
    });

    _defineProperty(_assertThisInitialized(_this), "onChange", function (monthYearPoint) {
      _this.toggleDropdown();

      var changedDate = newDate(parseInt(monthYearPoint));

      if (isSameYear(_this.props.date, changedDate) && isSameMonth(_this.props.date, changedDate)) {
        return;
      }

      _this.props.onChange(changedDate);
    });

    _defineProperty(_assertThisInitialized(_this), "toggleDropdown", function () {
      return _this.setState({
        dropdownVisible: !_this.state.dropdownVisible
      });
    });

    return _this;
  }

  _createClass(MonthYearDropdown, [{
    key: "componentDidUpdate",
    value: function componentDidUpdate(prevProps, prevState) {
      if (this.props.accessibleMode && // in accessibleMode
      prevState.dropdownVisible !== this.state.dropdownVisible && // dropdown visibility changed
      this.state.dropdownVisible === false // dropdown is no longer visible
      ) {
          this.readViewref.focus();
        }
    }
  }, {
    key: "render",
    value: function render() {
      var renderedDropdown;

      switch (this.props.dropdownMode) {
        case "scroll":
          renderedDropdown = this.renderScrollMode();
          break;

        case "select":
          renderedDropdown = this.renderSelectMode();
          break;
      }

      return ___EmotionJSX("div", {
        className: "react-datepicker__month-year-dropdown-container react-datepicker__month-year-dropdown-container--".concat(this.props.dropdownMode)
      }, renderedDropdown);
    }
  }]);

  return MonthYearDropdown;
}(React.Component);

_defineProperty(MonthYearDropdown, "propTypes", {
  dropdownMode: PropTypes.oneOf(["scroll", "select"]).isRequired,
  dateFormat: PropTypes.string.isRequired,
  locale: PropTypes.string,
  maxDate: PropTypes.object.isRequired,
  minDate: PropTypes.object.isRequired,
  date: PropTypes.object.isRequired,
  onChange: PropTypes.func.isRequired,
  scrollableMonthYearDropdown: PropTypes.bool,
  accessibleMode: PropTypes.bool
});

export { MonthYearDropdown as default };