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
import classNames from "classnames";
import { addMonths, cloneDate, formatDate, getStartOfMonth, isAfter, isSameMonth, isSameYear, isBefore } from "./date_utils";
import { EuiFocusTrap } from '../../../focus_trap';
import { EuiScreenReaderOnly } from '../../../accessibility';
import { jsx as ___EmotionJSX } from "@emotion/react";

function generateMonthYears(minDate, maxDate) {
  var list = [];
  var currDate = getStartOfMonth(cloneDate(minDate));
  var lastDate = getStartOfMonth(cloneDate(maxDate));

  while (!isAfter(currDate, lastDate)) {
    list.push(cloneDate(currDate));
    addMonths(currDate, 1);
  }

  return list;
}

var MonthYearDropdownOptions = /*#__PURE__*/function (_React$Component) {
  _inherits(MonthYearDropdownOptions, _React$Component);

  var _super = _createSuper(MonthYearDropdownOptions);

  function MonthYearDropdownOptions(props) {
    var _this;

    _classCallCheck(this, MonthYearDropdownOptions);

    _this = _super.call(this, props);

    _defineProperty(_assertThisInitialized(_this), "renderOptions", function () {
      return _this.state.monthYearsList.map(function (monthYear) {
        var monthYearPoint = monthYear.valueOf();
        var isSameMonthYear = isSameYear(_this.props.date, monthYear) && isSameMonth(_this.props.date, monthYear);
        var isPreselectionSameMonthYear = isSameYear(_this.state.preSelection, monthYear) && isSameMonth(_this.state.preSelection, monthYear);
        return ___EmotionJSX("div", {
          className: classNames("react-datepicker__month-year-option", {
            "--selected_month-year": isSameMonthYear,
            "react-datepicker__month-year-option--preselected": _this.props.accessibleMode && isPreselectionSameMonthYear
          }),
          key: monthYearPoint,
          ref: function ref(div) {
            if (_this.props.accessibleMode && isPreselectionSameMonthYear) {
              _this.preSelectionDiv = div;
            }
          },
          onClick: _this.onChange.bind(_assertThisInitialized(_this), monthYearPoint)
        }, isSameMonthYear ? ___EmotionJSX("span", {
          className: "react-datepicker__month-year-option--selected"
        }, "\u2713") : "", formatDate(monthYear, _this.props.dateFormat));
      });
    });

    _defineProperty(_assertThisInitialized(_this), "onFocus", function () {
      if (_this.props.accessibleMode) {
        _this.setState({
          readInstructions: true
        });
      }
    });

    _defineProperty(_assertThisInitialized(_this), "onChange", function (monthYear) {
      return _this.props.onChange(monthYear);
    });

    _defineProperty(_assertThisInitialized(_this), "handleClickOutside", function () {
      _this.props.onCancel();
    });

    _defineProperty(_assertThisInitialized(_this), "onInputKeyDown", function (event) {
      var eventKey = event.key;
      var newSelection;

      switch (eventKey) {
        case "ArrowUp":
          event.preventDefault();
          event.stopPropagation();
          newSelection = addMonths(cloneDate(_this.state.preSelection), -1);
          break;

        case "ArrowDown":
          event.preventDefault();
          event.stopPropagation();
          newSelection = addMonths(cloneDate(_this.state.preSelection), 1);
          break;

        case "Escape":
          event.preventDefault();
          event.stopPropagation();

          _this.props.onCancel();

          break;

        case " ":
        case "Enter":
          event.preventDefault();
          event.stopPropagation();

          _this.props.onChange(_this.state.preSelection.valueOf());

          break;
      }

      if (newSelection) {
        var minMonthYear = _this.state.monthYearsList[0];
        var maxMonthYear = _this.state.monthYearsList[_this.state.monthYearsList.length - 1];
        if (isBefore(newSelection, minMonthYear)) newSelection = maxMonthYear;
        if (isAfter(newSelection, maxMonthYear)) newSelection = minMonthYear;

        _this.setState({
          preSelection: newSelection
        });
      }
    });

    _this.state = {
      monthYearsList: generateMonthYears(_this.props.minDate, _this.props.maxDate),
      preSelection: getStartOfMonth(cloneDate(_this.props.date)),
      readInstructions: false
    };
    return _this;
  }

  _createClass(MonthYearDropdownOptions, [{
    key: "componentDidMount",
    value: function componentDidMount() {
      if (this.preSelectionDiv) {
        this.preSelectionDiv.scrollIntoView({
          behavior: "instant",
          block: "nearest",
          inline: "nearest"
        });
      }
    }
  }, {
    key: "componentDidUpdate",
    value: function componentDidUpdate(prevProps, prevState) {
      if (this.preSelectionDiv) {
        this.preSelectionDiv.scrollIntoView({
          behavior: "instant",
          block: "nearest",
          inline: "nearest"
        });
      }
    }
  }, {
    key: "render",
    value: function render() {
      var dropdownClass = classNames({
        "react-datepicker__month-year-dropdown": true,
        "react-datepicker__month-year-dropdown--scrollable": this.props.scrollableMonthYearDropdown
      });
      var screenReaderInstructions;

      if (this.state.readInstructions) {
        screenReaderInstructions = ___EmotionJSX("p", {
          "aria-live": true
        }, "You are focused on a month / year selector menu. Use the up and down arrows to select a month / year combination, then hit enter to confirm your selection.", formatDate(this.state.preSelection, this.props.dateFormat), " is the currently focused month / year.");
      }

      return this.props.accessibleMode ? ___EmotionJSX(EuiFocusTrap, {
        onClickOutside: this.handleClickOutside
      }, ___EmotionJSX("div", {
        className: dropdownClass,
        tabIndex: "0",
        onKeyDown: this.onInputKeyDown,
        onFocus: this.onFocus
      }, ___EmotionJSX(EuiScreenReaderOnly, null, ___EmotionJSX("span", null, screenReaderInstructions)), this.renderOptions())) : ___EmotionJSX("div", {
        className: dropdownClass
      }, this.renderOptions());
      return ___EmotionJSX("div", {
        className: dropdownClass
      }, this.renderOptions());
    }
  }]);

  return MonthYearDropdownOptions;
}(React.Component);

_defineProperty(MonthYearDropdownOptions, "propTypes", {
  minDate: PropTypes.object.isRequired,
  maxDate: PropTypes.object.isRequired,
  onCancel: PropTypes.func.isRequired,
  onChange: PropTypes.func.isRequired,
  scrollableMonthYearDropdown: PropTypes.bool,
  date: PropTypes.object.isRequired,
  dateFormat: PropTypes.string.isRequired,
  accessibleMode: PropTypes.bool
});

export { MonthYearDropdownOptions as default };