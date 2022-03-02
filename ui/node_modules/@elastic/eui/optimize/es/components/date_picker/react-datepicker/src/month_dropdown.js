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
import MonthDropdownOptions from "./month_dropdown_options";
import * as utils from "./date_utils";
import { jsx as ___EmotionJSX } from "@emotion/react";

var MonthDropdown = /*#__PURE__*/function (_React$Component) {
  _inherits(MonthDropdown, _React$Component);

  var _super = _createSuper(MonthDropdown);

  function MonthDropdown(props) {
    var _this;

    _classCallCheck(this, MonthDropdown);

    _this = _super.call(this, props);

    _defineProperty(_assertThisInitialized(_this), "setReadViewRef", function (ref) {
      _this.readViewref = ref;

      _this.props.buttonRef(ref);
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

    _defineProperty(_assertThisInitialized(_this), "renderSelectOptions", function (monthNames) {
      return monthNames.map(function (M, i) {
        return ___EmotionJSX("option", {
          key: i,
          value: i
        }, M);
      });
    });

    _defineProperty(_assertThisInitialized(_this), "renderSelectMode", function (monthNames) {
      return ___EmotionJSX("select", {
        value: _this.props.month,
        className: "react-datepicker__month-select",
        onChange: function onChange(e) {
          return _this.onChange(e.target.value);
        }
      }, _this.renderSelectOptions(monthNames));
    });

    _defineProperty(_assertThisInitialized(_this), "renderReadView", function (visible, monthNames) {
      return ___EmotionJSX("div", {
        key: "read",
        ref: _this.setReadViewRef,
        style: {
          visibility: visible ? "visible" : "hidden"
        },
        className: "react-datepicker__month-read-view",
        onClick: _this.toggleDropdown,
        onKeyDown: _this.onReadViewKeyDown,
        tabIndex: _this.props.accessibleMode ? "0" : undefined,
        "aria-label": "Button. Open the month selector. ".concat(monthNames[_this.props.month], " is currently selected.")
      }, ___EmotionJSX("span", {
        className: "react-datepicker__month-read-view--down-arrow"
      }), ___EmotionJSX("span", {
        className: "react-datepicker__month-read-view--selected-month"
      }, monthNames[_this.props.month]));
    });

    _defineProperty(_assertThisInitialized(_this), "renderDropdown", function (monthNames) {
      return ___EmotionJSX(MonthDropdownOptions, {
        key: "dropdown",
        ref: "options",
        month: _this.props.month,
        monthNames: monthNames,
        onChange: _this.onChange,
        onCancel: _this.toggleDropdown,
        accessibleMode: _this.props.accessibleMode
      });
    });

    _defineProperty(_assertThisInitialized(_this), "renderScrollMode", function (monthNames) {
      var dropdownVisible = _this.state.dropdownVisible;
      var result = [_this.renderReadView(!dropdownVisible, monthNames)];

      if (dropdownVisible) {
        result.unshift(_this.renderDropdown(monthNames));
      }

      return result;
    });

    _defineProperty(_assertThisInitialized(_this), "onChange", function (month) {
      _this.toggleDropdown();

      if (month !== _this.props.month) {
        _this.props.onChange(month);
      }
    });

    _defineProperty(_assertThisInitialized(_this), "toggleDropdown", function () {
      var isOpen = !_this.state.dropdownVisible;

      _this.setState({
        dropdownVisible: isOpen
      });

      _this.props.onDropdownToggle(isOpen, 'month');
    });

    _this.localeData = utils.getLocaleDataForLocale(_this.props.locale);
    _this.monthNames = [0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11].map(_this.props.useShortMonthInDropdown ? function (M) {
      return utils.getMonthShortInLocale(_this.localeData, utils.newDate({
        M: M
      }));
    } : function (M) {
      return utils.getMonthInLocale(_this.localeData, utils.newDate({
        M: M
      }), _this.props.dateFormat);
    });
    _this.state = {
      dropdownVisible: false
    };
    return _this;
  }

  _createClass(MonthDropdown, [{
    key: "componentDidUpdate",
    value: function componentDidUpdate(prevProps, prevState) {
      var _this2 = this;

      if (this.props.accessibleMode && // in accessibleMode
      prevState.dropdownVisible !== this.state.dropdownVisible && // dropdown visibility changed
      this.state.dropdownVisible === false // dropdown is no longer visible
      ) {
          this.readViewref.focus();
        }

      if (prevProps.locale !== this.props.locale) {
        this.localeData = utils.getLocaleDataForLocale(this.props.locale);
        this.monthNames = [0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11].map(this.props.useShortMonthInDropdown ? function (M) {
          return utils.getMonthShortInLocale(_this2.localeData, utils.newDate({
            M: M
          }));
        } : function (M) {
          return utils.getMonthInLocale(_this2.localeData, utils.newDate({
            M: M
          }), _this2.props.dateFormat);
        });
        this.forceUpdate();
      }
    }
  }, {
    key: "render",
    value: function render() {
      var renderedDropdown;

      switch (this.props.dropdownMode) {
        case "scroll":
          renderedDropdown = this.renderScrollMode(this.monthNames);
          break;

        case "select":
          renderedDropdown = this.renderSelectMode(this.monthNames);
          break;
      }

      return ___EmotionJSX("div", {
        className: "react-datepicker__month-dropdown-container react-datepicker__month-dropdown-container--".concat(this.props.dropdownMode)
      }, renderedDropdown);
    }
  }]);

  return MonthDropdown;
}(React.Component);

_defineProperty(MonthDropdown, "propTypes", {
  dropdownMode: PropTypes.oneOf(["scroll", "select"]).isRequired,
  locale: PropTypes.string,
  dateFormat: PropTypes.string.isRequired,
  month: PropTypes.number.isRequired,
  onChange: PropTypes.func.isRequired,
  useShortMonthInDropdown: PropTypes.bool,
  accessibleMode: PropTypes.bool,
  onDropdownToggle: PropTypes.func,
  buttonRef: PropTypes.func
});

export { MonthDropdown as default };