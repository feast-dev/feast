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

var _react = _interopRequireDefault(require("react"));

var _propTypes = _interopRequireDefault(require("prop-types"));

var _classnames = _interopRequireDefault(require("classnames"));

var _date_utils = require("./date_utils");

var _accessibility = require("../../../accessibility");

var _html_id_generator = require("../../../../services/accessibility/html_id_generator");

var _react2 = require("@emotion/react");

function _createSuper(Derived) { var hasNativeReflectConstruct = _isNativeReflectConstruct(); return function _createSuperInternal() { var Super = (0, _getPrototypeOf2.default)(Derived), result; if (hasNativeReflectConstruct) { var NewTarget = (0, _getPrototypeOf2.default)(this).constructor; result = Reflect.construct(Super, arguments, NewTarget); } else { result = Super.apply(this, arguments); } return (0, _possibleConstructorReturn2.default)(this, result); }; }

function _isNativeReflectConstruct() { if (typeof Reflect === "undefined" || !Reflect.construct) return false; if (Reflect.construct.sham) return false; if (typeof Proxy === "function") return true; try { Date.prototype.toString.call(Reflect.construct(Date, [], function () {})); return true; } catch (e) { return false; } }

function doHoursAndMinutesAlign(time1, time2) {
  if (time1 == null || time2 == null) return false;
  return (0, _date_utils.getHour)(time1) === (0, _date_utils.getHour)(time2) && (0, _date_utils.getMinute)(time1) === (0, _date_utils.getMinute)(time2);
}

var Time = /*#__PURE__*/function (_React$Component) {
  (0, _inherits2.default)(Time, _React$Component);

  var _super = _createSuper(Time);

  (0, _createClass2.default)(Time, null, [{
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

    (0, _classCallCheck2.default)(this, Time);

    for (var _len = arguments.length, args = new Array(_len), _key = 0; _key < _len; _key++) {
      args[_key] = arguments[_key];
    }

    _this = _super.call.apply(_super, [this].concat(args));
    (0, _defineProperty2.default)((0, _assertThisInitialized2.default)(_this), "onFocus", function () {
      if (_this.props.accessibleMode) {
        _this.setState({
          readInstructions: true,
          isFocused: true
        });
      }
    });
    (0, _defineProperty2.default)((0, _assertThisInitialized2.default)(_this), "onBlur", function () {
      if (_this.props.accessibleMode) {
        _this.setState({
          readInstructions: false,
          isFocused: false
        });
      }
    });
    (0, _defineProperty2.default)((0, _assertThisInitialized2.default)(_this), "onInputKeyDown", function (event) {
      var eventKey = event.key;
      var copy = (0, _date_utils.newDate)(_this.state.preSelection);
      var newSelection;

      switch (eventKey) {
        case "ArrowUp":
          newSelection = (0, _date_utils.addMinutes)(copy, -_this.props.intervals);
          break;

        case "ArrowDown":
          newSelection = (0, _date_utils.addMinutes)(copy, _this.props.intervals);
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
    (0, _defineProperty2.default)((0, _assertThisInitialized2.default)(_this), "handleClick", function (time) {
      if ((_this.props.minTime || _this.props.maxTime) && (0, _date_utils.isTimeInDisabledRange)(time, _this.props) || _this.props.excludeTimes && (0, _date_utils.isTimeDisabled)(time, _this.props.excludeTimes) || _this.props.includeTimes && !(0, _date_utils.isTimeDisabled)(time, _this.props.includeTimes)) {
        return;
      }

      _this.props.onChange(time);
    });
    (0, _defineProperty2.default)((0, _assertThisInitialized2.default)(_this), "liClasses", function (time, activeTime) {
      var classes = ["react-datepicker__time-list-item"];

      if (doHoursAndMinutesAlign(time, activeTime)) {
        classes.push("react-datepicker__time-list-item--selected");
      } else if (_this.state.preSelection && doHoursAndMinutesAlign(time, _this.state.preSelection)) {
        classes.push("react-datepicker__time-list-item--preselected");
      }

      if ((_this.props.minTime || _this.props.maxTime) && (0, _date_utils.isTimeInDisabledRange)(time, _this.props) || _this.props.excludeTimes && (0, _date_utils.isTimeDisabled)(time, _this.props.excludeTimes) || _this.props.includeTimes && !(0, _date_utils.isTimeDisabled)(time, _this.props.includeTimes)) {
        classes.push("react-datepicker__time-list-item--disabled");
      }

      if (_this.props.injectTimes && ((0, _date_utils.getHour)(time) * 60 + (0, _date_utils.getMinute)(time)) % _this.props.intervals !== 0) {
        classes.push("react-datepicker__time-list-item--injected");
      }

      return classes.join(" ");
    });
    (0, _defineProperty2.default)((0, _assertThisInitialized2.default)(_this), "generateTimes", function () {
      var times = [];
      var intervals = _this.props.intervals;
      var base = (0, _date_utils.getStartOfDay)((0, _date_utils.newDate)());
      var multiplier = 1440 / intervals;

      var sortedInjectTimes = _this.props.injectTimes && _this.props.injectTimes.sort(function (a, b) {
        return a - b;
      });

      for (var i = 0; i < multiplier; i++) {
        var currentTime = (0, _date_utils.addMinutes)((0, _date_utils.cloneDate)(base), i * intervals);
        times.push(currentTime);

        if (sortedInjectTimes) {
          var timesToInject = (0, _date_utils.timesToInjectAfter)(base, currentTime, i, intervals, sortedInjectTimes);
          times = times.concat(timesToInject);
        }
      }

      return times;
    });
    (0, _defineProperty2.default)((0, _assertThisInitialized2.default)(_this), "renderTimes", function () {
      var times = _this.generateTimes();

      var activeTime = _this.props.selected ? _this.props.selected : (0, _date_utils.newDate)();
      var format = _this.props.format ? _this.props.format : _this.timeFormat;
      var currH = (0, _date_utils.getHour)(activeTime);
      var currM = (0, _date_utils.getMinute)(activeTime);
      return times.map(function (time, i) {
        return (0, _react2.jsx)("li", {
          key: i,
          onClick: _this.handleClick.bind((0, _assertThisInitialized2.default)(_this), time),
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
        }, (0, _date_utils.formatDate)(time, format));
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
      var currH = _this.props.selected ? (0, _date_utils.getHour)(_this.props.selected) : (0, _date_utils.getHour)((0, _date_utils.newDate)());
      var currM = _this.props.selected ? (0, _date_utils.getMinute)(_this.props.selected) : (0, _date_utils.getMinute)((0, _date_utils.newDate)());
      var closestTimeIndex = Math.floor((60 * currH + currM) / _this.props.intervals);
      var closestMinutes = closestTimeIndex * _this.props.intervals;
      preSelection = (0, _date_utils.setTime)((0, _date_utils.newDate)(), {
        hour: Math.floor(closestMinutes / 60),
        minute: closestMinutes % 60,
        second: 0,
        millisecond: 0
      });
    }

    _this.timeOptionId = (0, _html_id_generator.htmlIdGenerator)();
    _this.timeFormat = "hh:mm A";
    _this.state = {
      preSelection: preSelection,
      needsScrollToPreSelection: false,
      readInstructions: false,
      isFocused: false
    };
    return _this;
  }

  (0, _createClass2.default)(Time, [{
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

      var classNames = (0, _classnames.default)("react-datepicker__time-container", {
        "react-datepicker__time-container--with-today-button": this.props.todayButton,
        "react-datepicker__time-container--focus": this.state.isFocused
      });
      var timeBoxClassNames = (0, _classnames.default)("react-datepicker__time-box", {
        "react-datepicker__time-box--accessible": this.props.accessibleMode
      });
      var screenReaderInstructions;

      if (this.state.readInstructions) {
        screenReaderInstructions = (0, _react2.jsx)("p", {
          "aria-live": true
        }, "You are a in a time selector. Use the up and down keys to select from other common times then press enter to confirm.", this.state.preSelection ? "".concat((0, _date_utils.formatDate)(this.state.preSelection, this.timeFormat), " is currently\n          focused.") : "No time is currently focused.");
      }

      return (0, _react2.jsx)("div", {
        className: classNames
      }, (0, _react2.jsx)("div", {
        className: "react-datepicker__header react-datepicker__header--time",
        ref: function ref(header) {
          _this2.header = header;
        }
      }, (0, _react2.jsx)("div", {
        className: "react-datepicker-time__header"
      }, this.props.timeCaption), (0, _react2.jsx)(_accessibility.EuiScreenReaderOnly, null, (0, _react2.jsx)("span", null, screenReaderInstructions))), (0, _react2.jsx)("div", {
        className: "react-datepicker__time"
      }, (0, _react2.jsx)("div", {
        className: timeBoxClassNames,
        onKeyDown: this.onInputKeyDown,
        onFocus: this.onFocus,
        onBlur: this.onBlur
      }, (0, _react2.jsx)("ul", {
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
}(_react.default.Component);

exports.default = Time;
(0, _defineProperty2.default)(Time, "propTypes", {
  format: _propTypes.default.string,
  includeTimes: _propTypes.default.array,
  intervals: _propTypes.default.number,
  selected: _propTypes.default.object,
  onChange: _propTypes.default.func,
  todayButton: _propTypes.default.node,
  minTime: _propTypes.default.object,
  maxTime: _propTypes.default.object,
  excludeTimes: _propTypes.default.array,
  monthRef: _propTypes.default.object,
  timeCaption: _propTypes.default.string,
  injectTimes: _propTypes.default.array,
  accessibleMode: _propTypes.default.bool
});
(0, _defineProperty2.default)(Time, "calcCenterPosition", function (listHeight, centerLiRef) {
  return centerLiRef.offsetTop - (listHeight / 2 - centerLiRef.clientHeight / 2);
});
module.exports = exports.default;