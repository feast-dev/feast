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
import classnames from "classnames";
import { EuiFocusTrap } from '../../../focus_trap';
import { EuiScreenReaderOnly } from '../../../accessibility';
import { jsx as ___EmotionJSX } from "@emotion/react";

var MonthDropdownOptions = /*#__PURE__*/function (_React$Component) {
  _inherits(MonthDropdownOptions, _React$Component);

  var _super = _createSuper(MonthDropdownOptions);

  function MonthDropdownOptions() {
    var _this;

    _classCallCheck(this, MonthDropdownOptions);

    for (var _len = arguments.length, args = new Array(_len), _key = 0; _key < _len; _key++) {
      args[_key] = arguments[_key];
    }

    _this = _super.call.apply(_super, [this].concat(args));

    _defineProperty(_assertThisInitialized(_this), "renderOptions", function () {
      return _this.props.monthNames.map(function (month, i) {
        return ___EmotionJSX("div", {
          className: classnames("react-datepicker__month-option", {
            "react-datepicker__month-option--selected_month": _this.props.month === i,
            "react-datepicker__month-option--preselected": _this.props.accessibleMode && _this.state.preSelection === i
          }),
          key: month,
          ref: month,
          onClick: _this.onChange.bind(_assertThisInitialized(_this), i)
        }, _this.props.month === i ? ___EmotionJSX("span", {
          className: "react-datepicker__month-option--selected"
        }, "\u2713") : "", month);
      });
    });

    _defineProperty(_assertThisInitialized(_this), "onFocus", function () {
      if (_this.props.accessibleMode) {
        _this.setState({
          readInstructions: true
        });
      }
    });

    _defineProperty(_assertThisInitialized(_this), "onChange", function (month) {
      return _this.props.onChange(month);
    });

    _defineProperty(_assertThisInitialized(_this), "handleClickOutside", function () {
      return _this.props.onCancel();
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
          var nextSelection = preSelection + selectionChange;
          if (nextSelection < 0) nextSelection = 11;
          if (nextSelection === 12) nextSelection = 0;
          return {
            preSelection: nextSelection
          };
        });
      }
    });

    _this.state = {
      preSelection: _this.props.month,
      readInstructions: false
    };
    return _this;
  }

  _createClass(MonthDropdownOptions, [{
    key: "render",
    value: function render() {
      var screenReaderInstructions;

      if (this.state.readInstructions) {
        screenReaderInstructions = ___EmotionJSX("p", {
          "aria-live": true
        }, "You are focused on a month selector menu. Use the up and down arrows to select a year, then hit enter to confirm your selection.", this.props.monthNames[this.state.preSelection], " is the currently focused month.");
      }

      return this.props.accessibleMode ? ___EmotionJSX(EuiFocusTrap, {
        onClickOutside: this.handleClickOutside
      }, ___EmotionJSX("div", {
        className: "react-datepicker__month-dropdown",
        tabIndex: "0",
        onKeyDown: this.onInputKeyDown,
        onFocus: this.onFocus
      }, ___EmotionJSX(EuiScreenReaderOnly, null, ___EmotionJSX("span", null, screenReaderInstructions)), this.renderOptions())) : ___EmotionJSX("div", {
        className: "react-datepicker__month-dropdown"
      }, this.renderOptions());
    }
  }]);

  return MonthDropdownOptions;
}(React.Component);

_defineProperty(MonthDropdownOptions, "propTypes", {
  onCancel: PropTypes.func.isRequired,
  onChange: PropTypes.func.isRequired,
  month: PropTypes.number.isRequired,
  monthNames: PropTypes.arrayOf(PropTypes.string.isRequired).isRequired,
  accessibleMode: PropTypes.bool
});

export { MonthDropdownOptions as default };