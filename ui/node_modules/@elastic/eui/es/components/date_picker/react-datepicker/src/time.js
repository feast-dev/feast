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
import { getHour, getMinute, newDate, getStartOfDay, addMinutes, cloneDate, formatDate, isTimeInDisabledRange, isTimeDisabled, timesToInjectAfter, setTime } from "./date_utils";
import { EuiScreenReaderOnly } from '../../../accessibility';
import { htmlIdGenerator } from '../../../../services/accessibility/html_id_generator';
import { jsx as ___EmotionJSX } from "@emotion/react";

function doHoursAndMinutesAlign(time1, time2) {
  if (time1 == null || time2 == null) return false;
  return getHour(time1) === getHour(time2) && getMinute(time1) === getMinute(time2);
}

var Time = /*#__PURE__*/function (_React$Component) {
  _inherits(Time, _React$Component);

  var _super = _createSuper(Time);

  _createClass(Time, null, [{
    key: "defaultProps",
    get: function get() {
      return {
        intervals: 30,
        onTimeChange: function onTimeChange() {},
        todayButton: null,
        timeCaption: "Time"
      };
    }
  }]);

  function Time() {
    var _this;

    _classCallCheck(this, Time);

    for (var _len = arguments.length, args = new Array(_len), _key = 0; _key < _len; _key++) {
      args[_key] = arguments[_key];
    }

    _this = _super.call.apply(_super, [this].concat(args));

    _defineProperty(_assertThisInitialized(_this), "onFocus", function () {
      if (_this.props.accessibleMode) {
        _this.setState({
          readInstructions: true,
          isFocused: true
        });
      }
    });

    _defineProperty(_assertThisInitialized(_this), "onBlur", function () {
      if (_this.props.accessibleMode) {
        _this.setState({
          readInstructions: false,
          isFocused: false
        });
      }
    });

    _defineProperty(_assertThisInitialized(_this), "onInputKeyDown", function (event) {
      var eventKey = event.key;
      var copy = newDate(_this.state.preSelection);
      var newSelection;

      switch (eventKey) {
        case "ArrowUp":
          newSelection = addMinutes(copy, -_this.props.intervals);
          break;

        case "ArrowDown":
          newSelection = addMinutes(copy, _this.props.intervals);
          break;

        case " ":
        case "Enter":
          event.preventDefault();

          _this.handleClick(_this.state.preSelection);

          break;
      }

      if (!newSelection) return; // Let the input component handle this keydown

      event.preventDefault();

      _this.setState({
        preSelection: newSelection,
        needsScrollToPreSelection: true
      });
    });

    _defineProperty(_assertThisInitialized(_this), "handleClick", function (time) {
      if ((_this.props.minTime || _this.props.maxTime) && isTimeInDisabledRange(time, _this.props) || _this.props.excludeTimes && isTimeDisabled(time, _this.props.excludeTimes) || _this.props.includeTimes && !isTimeDisabled(time, _this.props.includeTimes)) {
        return;
      }

      _this.props.onChange(time);
    });

    _defineProperty(_assertThisInitialized(_this), "liClasses", function (time, activeTime) {
      var classes = ["react-datepicker__time-list-item"];

      if (doHoursAndMinutesAlign(time, activeTime)) {
        classes.push("react-datepicker__time-list-item--selected");
      } else if (_this.state.preSelection && doHoursAndMinutesAlign(time, _this.state.preSelection)) {
        classes.push("react-datepicker__time-list-item--preselected");
      }

      if ((_this.props.minTime || _this.props.maxTime) && isTimeInDisabledRange(time, _this.props) || _this.props.excludeTimes && isTimeDisabled(time, _this.props.excludeTimes) || _this.props.includeTimes && !isTimeDisabled(time, _this.props.includeTimes)) {
        classes.push("react-datepicker__time-list-item--disabled");
      }

      if (_this.props.injectTimes && (getHour(time) * 60 + getMinute(time)) % _this.props.intervals !== 0) {
        classes.push("react-datepicker__time-list-item--injected");
      }

      return classes.join(" ");
    });

    _defineProperty(_assertThisInitialized(_this), "generateTimes", function () {
      var times = [];
      var intervals = _this.props.intervals;
      var base = getStartOfDay(newDate());
      var multiplier = 1440 / intervals;

      var sortedInjectTimes = _this.props.injectTimes && _this.props.injectTimes.sort(function (a, b) {
        return a - b;
      });

      for (var i = 0; i < multiplier; i++) {
        var currentTime = addMinutes(cloneDate(base), i * intervals);
        times.push(currentTime);

        if (sortedInjectTimes) {
          var timesToInject = timesToInjectAfter(base, currentTime, i, intervals, sortedInjectTimes);
          times = times.concat(timesToInject);
        }
      }

      return times;
    });

    _defineProperty(_assertThisInitialized(_this), "renderTimes", function () {
      var times = _this.generateTimes();

      var activeTime = _this.props.selected ? _this.props.selected : newDate();
      var format = _this.props.format ? _this.props.format : _this.timeFormat;
      var currH = getHour(activeTime);
      var currM = getMinute(activeTime);
      return times.map(function (time, i) {
        return ___EmotionJSX("li", {
          key: i,
          onClick: _this.handleClick.bind(_assertThisInitialized(_this), time),
          className: _this.liClasses(time, activeTime),
          ref: function ref(li) {
            if (li && li.classList.contains("react-datepicker__time-list-item--preselected")) {
              _this.preselectedLi = li;
            }

            if (li && li.classList.contains("react-datepicker__time-list-item--selected")) {
              _this.selectedLi = li;
            }
          },
          role: "option",
          id: _this.timeOptionId("datepicker_time_".concat(i))
        }, formatDate(time, format));
      });
    });

    var _times = _this.generateTimes();

    var preSelection = _times.reduce(function (preSelection, time) {
      if (preSelection) return preSelection;

      if (doHoursAndMinutesAlign(time, _this.props.selected)) {
        return time;
      }
    }, null);

    if (preSelection == null) {
      // there is no exact pre-selection, find the element closest to the selected time and preselect it
      var currH = _this.props.selected ? getHour(_this.props.selected) : getHour(newDate());
      var currM = _this.props.selected ? getMinute(_this.props.selected) : getMinute(newDate());
      var closestTimeIndex = Math.floor((60 * currH + currM) / _this.props.intervals);
      var closestMinutes = closestTimeIndex * _this.props.intervals;
      preSelection = setTime(newDate(), {
        hour: Math.floor(closestMinutes / 60),
        minute: closestMinutes % 60,
        second: 0,
        millisecond: 0
      });
    }

    _this.timeOptionId = htmlIdGenerator();
    _this.timeFormat = "hh:mm A";
    _this.state = {
      preSelection: preSelection,
      needsScrollToPreSelection: false,
      readInstructions: false,
      isFocused: false
    };
    return _this;
  }

  _createClass(Time, [{
    key: "componentDidMount",
    value: function componentDidMount() {
      // code to ensure selected time will always be in focus within time window when it first appears
      var scrollParent = this.list;
      scrollParent.scrollTop = Time.calcCenterPosition(this.props.monthRef ? this.props.monthRef.clientHeight - this.header.clientHeight : this.list.clientHeight, this.selectedLi || this.preselectedLi);
    }
  }, {
    key: "componentDidUpdate",
    value: function componentDidUpdate(prevProps) {
      // if selection changed, scroll to the selected item
      if (this.props.selected && this.props.selected.isSame(prevProps.selected) === false) {
        var scrollToElement = this.selectedLi;

        if (scrollToElement) {
          // an element matches the selected time, scroll to it
          scrollToElement.scrollIntoView({
            behavior: "instant",
            block: "nearest",
            inline: "nearest"
          });
        } // update preSelection to the selection


        this.setState(function (prevState) {
          return {
            preSelection: prevState.selected
          };
        });
      }

      if (this.state.needsScrollToPreSelection) {
        var _scrollToElement = this.preselectedLi;

        if (_scrollToElement) {
          // an element matches the selected time, scroll to it
          _scrollToElement.scrollIntoView({
            behavior: "instant",
            block: "nearest",
            inline: "nearest"
          });
        }

        this.setState({
          needsScrollToPreSelection: false
        });
      }
    }
  }, {
    key: "render",
    value: function render() {
      var _this2 = this;

      var height = null;

      if (this.props.monthRef && this.header) {
        height = this.props.monthRef.clientHeight - this.header.clientHeight;
      }

      var classNames = classnames("react-datepicker__time-container", {
        "react-datepicker__time-container--with-today-button": this.props.todayButton,
        "react-datepicker__time-container--focus": this.state.isFocused
      });
      var timeBoxClassNames = classnames("react-datepicker__time-box", {
        "react-datepicker__time-box--accessible": this.props.accessibleMode
      });
      var screenReaderInstructions;

      if (this.state.readInstructions) {
        screenReaderInstructions = ___EmotionJSX("p", {
          "aria-live": true
        }, "You are a in a time selector. Use the up and down keys to select from other common times then press enter to confirm.", this.state.preSelection ? "".concat(formatDate(this.state.preSelection, this.timeFormat), " is currently\n          focused.") : "No time is currently focused.");
      }

      return ___EmotionJSX("div", {
        className: classNames
      }, ___EmotionJSX("div", {
        className: "react-datepicker__header react-datepicker__header--time",
        ref: function ref(header) {
          _this2.header = header;
        }
      }, ___EmotionJSX("div", {
        className: "react-datepicker-time__header"
      }, this.props.timeCaption), ___EmotionJSX(EuiScreenReaderOnly, null, ___EmotionJSX("span", null, screenReaderInstructions))), ___EmotionJSX("div", {
        className: "react-datepicker__time"
      }, ___EmotionJSX("div", {
        className: timeBoxClassNames,
        onKeyDown: this.onInputKeyDown,
        onFocus: this.onFocus,
        onBlur: this.onBlur
      }, ___EmotionJSX("ul", {
        "aria-label": this.props.timeCaption,
        className: "react-datepicker__time-list",
        ref: function ref(list) {
          _this2.list = list;
        },
        style: height ? {
          height: height
        } : {},
        role: "listbox",
        tabIndex: this.props.accessibleMode ? 0 : -1
      }, this.renderTimes.bind(this)()))));
    }
  }]);

  return Time;
}(React.Component);

_defineProperty(Time, "propTypes", {
  format: PropTypes.string,
  includeTimes: PropTypes.array,
  intervals: PropTypes.number,
  selected: PropTypes.object,
  onChange: PropTypes.func,
  todayButton: PropTypes.node,
  minTime: PropTypes.object,
  maxTime: PropTypes.object,
  excludeTimes: PropTypes.array,
  monthRef: PropTypes.object,
  timeCaption: PropTypes.string,
  injectTimes: PropTypes.array,
  accessibleMode: PropTypes.bool
});

_defineProperty(Time, "calcCenterPosition", function (listHeight, centerLiRef) {
  return centerLiRef.offsetTop - (listHeight / 2 - centerLiRef.clientHeight / 2);
});

export { Time as default };