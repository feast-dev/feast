import _classCallCheck from "@babel/runtime/helpers/classCallCheck";
import _createClass from "@babel/runtime/helpers/createClass";
import _assertThisInitialized from "@babel/runtime/helpers/assertThisInitialized";
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
import React from "react";
import PropTypes from "prop-types";
import classNames from "classnames";
import { EuiFocusTrap } from '../../../focus_trap';
import { EuiScreenReaderOnly } from '../../../accessibility';
import { jsx as ___EmotionJSX } from "@emotion/react";

function generateYears(year, noOfYear, minDate, maxDate) {
  var list = [];

  for (var i = 0; i < 2 * noOfYear + 1; i++) {
    var newYear = year + noOfYear - i;
    var isInRange = true;

    if (minDate) {
      isInRange = minDate.year() <= newYear;
    }

    if (maxDate && isInRange) {
      isInRange = maxDate.year() >= newYear;
    }

    if (isInRange) {
      list.push(newYear);
    }
  }

  return list;
}

var YearDropdownOptions = /*#__PURE__*/function (_React$Component) {
  _inherits(YearDropdownOptions, _React$Component);

  var _super = _createSuper(YearDropdownOptions);

  function YearDropdownOptions(props) {
    var _this;

    _classCallCheck(this, YearDropdownOptions);

    _this = _super.call(this, props);

    _defineProperty(_assertThisInitialized(_this), "renderOptions", function () {
      var selectedYear = _this.props.year;

      var options = _this.state.yearsList.map(function (year) {
        return ___EmotionJSX("div", {
          className: classNames("react-datepicker__year-option", {
            "react-datepicker__year-option--selected_year": selectedYear === year,
            "react-datepicker__year-option--preselected": _this.props.accessibleMode && _this.state.preSelection === year
          }),
          key: year,
          ref: function ref(div) {
            if (_this.props.accessibleMode && _this.state.preSelection === year) {
              _this.preSelectionDiv = div;
            }
          },
          onClick: _this.onChange.bind(_assertThisInitialized(_this), year)
        }, selectedYear === year ? ___EmotionJSX("span", {
          className: "react-datepicker__year-option--selected"
        }, "\u2713") : "", year);
      });

      var minYear = _this.props.minDate ? _this.props.minDate.year() : null;
      var maxYear = _this.props.maxDate ? _this.props.maxDate.year() : null; // These elements were hidden with `display: none;` by custom EUI styles,
      // which caused problems when `minDate` or `maxDate` were configured: https://github.com/elastic/eui/issues/5058
      // Keeping a reference for now, but we may opt for removing these 
      // elements entirely during https://github.com/elastic/eui/issues/3901
      // if (!maxYear || !this.state.yearsList.find(year => year === maxYear)) {
      //   options.unshift(
      //     <div
      //       className="react-datepicker__year-option"
      //       ref={"upcoming"}
      //       key={"upcoming"}
      //       onClick={this.incrementYears}
      //     >
      //       <a className="react-datepicker__navigation react-datepicker__navigation--years react-datepicker__navigation--years-upcoming" />
      //     </div>
      //   );
      // }
      // if (!minYear || !this.state.yearsList.find(year => year === minYear)) {
      //   options.push(
      //     <div
      //       className="react-datepicker__year-option"
      //       ref={"previous"}
      //       key={"previous"}
      //       onClick={this.decrementYears}
      //     >
      //       <a className="react-datepicker__navigation react-datepicker__navigation--years react-datepicker__navigation--years-previous" />
      //     </div>
      //   );
      // }

      return options;
    });

    _defineProperty(_assertThisInitialized(_this), "onFocus", function () {
      if (_this.props.accessibleMode) {
        _this.setState({
          readInstructions: true
        });
      }
    });

    _defineProperty(_assertThisInitialized(_this), "onChange", function (year) {
      _this.props.onChange(year);
    });

    _defineProperty(_assertThisInitialized(_this), "handleClickOutside", function () {
      _this.props.onCancel();
    });

    _defineProperty(_assertThisInitialized(_this), "shiftYears", function (amount) {
      var years = _this.state.yearsList.map(function (year) {
        return year + amount;
      });

      _this.setState({
        yearsList: years
      });
    });

    _defineProperty(_assertThisInitialized(_this), "incrementYears", function () {
      return _this.shiftYears(1);
    });

    _defineProperty(_assertThisInitialized(_this), "decrementYears", function () {
      return _this.shiftYears(-1);
    });

    _defineProperty(_assertThisInitialized(_this), "onInputKeyDown", function (event) {
      var eventKey = event.key;
      var selectionChange = 0;

      switch (eventKey) {
        case "ArrowUp":
          event.preventDefault();
          event.stopPropagation();
          selectionChange = -1;
          break;

        case "ArrowDown":
          event.preventDefault();
          event.stopPropagation();
          selectionChange = 1;
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

          _this.props.onChange(_this.state.preSelection);

          break;
      }

      if (selectionChange) {
        _this.setState(function (_ref) {
          var preSelection = _ref.preSelection;
          var maxYear = _this.state.yearsList[0];
          var minYear = _this.state.yearsList[_this.state.yearsList.length - 1];
          var nextSelection = preSelection + selectionChange;
          if (nextSelection < minYear) nextSelection = maxYear;
          if (nextSelection > maxYear) nextSelection = minYear;
          return {
            preSelection: nextSelection
          };
        });
      }
    });

    var yearDropdownItemNumber = props.yearDropdownItemNumber,
        scrollableYearDropdown = props.scrollableYearDropdown;
    var noOfYear = yearDropdownItemNumber || (scrollableYearDropdown ? 10 : 5);
    _this.state = {
      yearsList: generateYears(_this.props.year, noOfYear, _this.props.minDate, _this.props.maxDate),
      preSelection: _this.props.year,
      readInstructions: false
    };
    return _this;
  }

  _createClass(YearDropdownOptions, [{
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
      if (this.preSelectionDiv && prevState.preSelection !== this.state.preSelection) {
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
        "react-datepicker__year-dropdown": true,
        "react-datepicker__year-dropdown--scrollable": this.props.scrollableYearDropdown
      });
      var screenReaderInstructions;

      if (this.state.readInstructions) {
        screenReaderInstructions = ___EmotionJSX("p", {
          "aria-live": true
        }, "You are focused on a year selector menu. Use the up and down arrows to select a year, then hit enter to confirm your selection.", this.state.preSelection, " is the currently focused year.");
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
    }
  }]);

  return YearDropdownOptions;
}(React.Component);

_defineProperty(YearDropdownOptions, "propTypes", {
  minDate: PropTypes.object,
  maxDate: PropTypes.object,
  onCancel: PropTypes.func.isRequired,
  onChange: PropTypes.func.isRequired,
  scrollableYearDropdown: PropTypes.bool,
  year: PropTypes.number.isRequired,
  yearDropdownItemNumber: PropTypes.number,
  accessibleMode: PropTypes.bool
});

export { YearDropdownOptions as default };