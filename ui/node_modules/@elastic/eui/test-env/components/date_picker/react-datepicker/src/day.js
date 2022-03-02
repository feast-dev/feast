"use strict";

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

var _date_utils = require("./date_utils");

var _react2 = require("@emotion/react");

function _createSuper(Derived) { var hasNativeReflectConstruct = _isNativeReflectConstruct(); return function _createSuperInternal() { var Super = (0, _getPrototypeOf2.default)(Derived), result; if (hasNativeReflectConstruct) { var NewTarget = (0, _getPrototypeOf2.default)(this).constructor; result = Reflect.construct(Super, arguments, NewTarget); } else { result = Super.apply(this, arguments); } return (0, _possibleConstructorReturn2.default)(this, result); }; }

function _isNativeReflectConstruct() { if (typeof Reflect === "undefined" || !Reflect.construct) return false; if (Reflect.construct.sham) return false; if (typeof Proxy === "function") return true; try { Date.prototype.toString.call(Reflect.construct(Date, [], function () {})); return true; } catch (e) { return false; } }

var Day = /*#__PURE__*/function (_React$Component) {
  (0, _inherits2.default)(Day, _React$Component);

  var _super = _createSuper(Day);

  function Day() {
    var _this;

    (0, _classCallCheck2.default)(this, Day);

    for (var _len = arguments.length, args = new Array(_len), _key = 0; _key < _len; _key++) {
      args[_key] = arguments[_key];
    }

    _this = _super.call.apply(_super, [this].concat(args));
    (0, _defineProperty2.default)((0, _assertThisInitialized2.default)(_this), "handleClick", function (event) {
      if (!_this.isDisabled() && _this.props.onClick) {
        _this.props.onClick(event);
      }
    });
    (0, _defineProperty2.default)((0, _assertThisInitialized2.default)(_this), "handleMouseEnter", function (event) {
      if (!_this.isDisabled() && _this.props.onMouseEnter) {
        _this.props.onMouseEnter(event);
      }
    });
    (0, _defineProperty2.default)((0, _assertThisInitialized2.default)(_this), "isSameDay", function (other) {
      return (0, _date_utils.isSameDay)(_this.props.day, other);
    });
    (0, _defineProperty2.default)((0, _assertThisInitialized2.default)(_this), "isKeyboardSelected", function () {
      return !_this.props.disabledKeyboardNavigation && (!_this.props.inline || _this.props.accessibleMode) && !_this.isSameDay(_this.props.selected) && _this.isSameDay(_this.props.preSelection);
    });
    (0, _defineProperty2.default)((0, _assertThisInitialized2.default)(_this), "isDisabled", function () {
      return (0, _date_utils.isDayDisabled)(_this.props.day, _this.props);
    });
    (0, _defineProperty2.default)((0, _assertThisInitialized2.default)(_this), "getHighLightedClass", function (defaultClassName) {
      var _this$props = _this.props,
          day = _this$props.day,
          highlightDates = _this$props.highlightDates;

      if (!highlightDates) {
        return false;
      } // Looking for className in the Map of {'day string, 'className'}


      var dayStr = day.format("MM.DD.YYYY");
      return highlightDates.get(dayStr);
    });
    (0, _defineProperty2.default)((0, _assertThisInitialized2.default)(_this), "isInRange", function () {
      var _this$props2 = _this.props,
          day = _this$props2.day,
          startDate = _this$props2.startDate,
          endDate = _this$props2.endDate;

      if (!startDate || !endDate) {
        return false;
      }

      return (0, _date_utils.isDayInRange)(day, startDate, endDate);
    });
    (0, _defineProperty2.default)((0, _assertThisInitialized2.default)(_this), "isInSelectingRange", function () {
      var _this$props3 = _this.props,
          day = _this$props3.day,
          selectsStart = _this$props3.selectsStart,
          selectsEnd = _this$props3.selectsEnd,
          selectingDate = _this$props3.selectingDate,
          startDate = _this$props3.startDate,
          endDate = _this$props3.endDate;

      if (!(selectsStart || selectsEnd) || !selectingDate || _this.isDisabled()) {
        return false;
      }

      if (selectsStart && endDate && selectingDate.isSameOrBefore(endDate)) {
        return (0, _date_utils.isDayInRange)(day, selectingDate, endDate);
      }

      if (selectsEnd && startDate && selectingDate.isSameOrAfter(startDate)) {
        return (0, _date_utils.isDayInRange)(day, startDate, selectingDate);
      }

      return false;
    });
    (0, _defineProperty2.default)((0, _assertThisInitialized2.default)(_this), "isSelectingRangeStart", function () {
      if (!_this.isInSelectingRange()) {
        return false;
      }

      var _this$props4 = _this.props,
          day = _this$props4.day,
          selectingDate = _this$props4.selectingDate,
          startDate = _this$props4.startDate,
          selectsStart = _this$props4.selectsStart;

      if (selectsStart) {
        return (0, _date_utils.isSameDay)(day, selectingDate);
      } else {
        return (0, _date_utils.isSameDay)(day, startDate);
      }
    });
    (0, _defineProperty2.default)((0, _assertThisInitialized2.default)(_this), "isSelectingRangeEnd", function () {
      if (!_this.isInSelectingRange()) {
        return false;
      }

      var _this$props5 = _this.props,
          day = _this$props5.day,
          selectingDate = _this$props5.selectingDate,
          endDate = _this$props5.endDate,
          selectsEnd = _this$props5.selectsEnd;

      if (selectsEnd) {
        return (0, _date_utils.isSameDay)(day, selectingDate);
      } else {
        return (0, _date_utils.isSameDay)(day, endDate);
      }
    });
    (0, _defineProperty2.default)((0, _assertThisInitialized2.default)(_this), "isRangeStart", function () {
      var _this$props6 = _this.props,
          day = _this$props6.day,
          startDate = _this$props6.startDate,
          endDate = _this$props6.endDate;

      if (!startDate || !endDate) {
        return false;
      }

      return (0, _date_utils.isSameDay)(startDate, day);
    });
    (0, _defineProperty2.default)((0, _assertThisInitialized2.default)(_this), "isRangeEnd", function () {
      var _this$props7 = _this.props,
          day = _this$props7.day,
          startDate = _this$props7.startDate,
          endDate = _this$props7.endDate;

      if (!startDate || !endDate) {
        return false;
      }

      return (0, _date_utils.isSameDay)(endDate, day);
    });
    (0, _defineProperty2.default)((0, _assertThisInitialized2.default)(_this), "isWeekend", function () {
      var weekday = (0, _date_utils.getDay)(_this.props.day);
      return weekday === 0 || weekday === 6;
    });
    (0, _defineProperty2.default)((0, _assertThisInitialized2.default)(_this), "isOutsideMonth", function () {
      return _this.props.month !== undefined && _this.props.month !== (0, _date_utils.getMonth)(_this.props.day);
    });
    (0, _defineProperty2.default)((0, _assertThisInitialized2.default)(_this), "getClassNames", function (date) {
      var dayClassName = _this.props.dayClassName ? _this.props.dayClassName(date) : undefined;
      return (0, _classnames.default)("react-datepicker__day", dayClassName, "react-datepicker__day--" + (0, _date_utils.getDayOfWeekCode)(_this.props.day), {
        "react-datepicker__day--disabled": _this.isDisabled(),
        "react-datepicker__day--selected": _this.isSameDay(_this.props.selected),
        "react-datepicker__day--keyboard-selected": _this.isKeyboardSelected(),
        "react-datepicker__day--range-start": _this.isRangeStart(),
        "react-datepicker__day--range-end": _this.isRangeEnd(),
        "react-datepicker__day--in-range": _this.isInRange(),
        "react-datepicker__day--in-selecting-range": _this.isInSelectingRange(),
        "react-datepicker__day--selecting-range-start": _this.isSelectingRangeStart(),
        "react-datepicker__day--selecting-range-end": _this.isSelectingRangeEnd(),
        "react-datepicker__day--today": _this.isSameDay((0, _date_utils.now)(_this.props.utcOffset)),
        "react-datepicker__day--weekend": _this.isWeekend(),
        "react-datepicker__day--outside-month": _this.isOutsideMonth()
      }, _this.getHighLightedClass("react-datepicker__day--highlighted"));
    });
    return _this;
  }

  (0, _createClass2.default)(Day, [{
    key: "render",
    value: function render() {
      return (0, _react2.jsx)("div", {
        className: this.getClassNames(this.props.day),
        onClick: this.handleClick,
        onMouseEnter: this.handleMouseEnter,
        "aria-label": "day-".concat((0, _date_utils.getDate)(this.props.day)),
        role: "option"
      }, this.props.renderDayContents ? this.props.renderDayContents((0, _date_utils.getDate)(this.props.day)) : (0, _date_utils.getDate)(this.props.day));
    }
  }]);
  return Day;
}(_react.default.Component);

exports.default = Day;
(0, _defineProperty2.default)(Day, "propTypes", {
  disabledKeyboardNavigation: _propTypes.default.bool,
  day: _propTypes.default.object.isRequired,
  dayClassName: _propTypes.default.func,
  endDate: _propTypes.default.object,
  highlightDates: _propTypes.default.instanceOf(Map),
  inline: _propTypes.default.bool,
  month: _propTypes.default.number,
  onClick: _propTypes.default.func,
  onMouseEnter: _propTypes.default.func,
  preSelection: _propTypes.default.object,
  selected: _propTypes.default.object,
  selectingDate: _propTypes.default.object,
  selectsEnd: _propTypes.default.bool,
  selectsStart: _propTypes.default.bool,
  startDate: _propTypes.default.object,
  utcOffset: _propTypes.default.oneOfType([_propTypes.default.number, _propTypes.default.string]),
  renderDayContents: _propTypes.default.func,
  accessibleMode: _propTypes.default.bool
});
module.exports = exports.default;