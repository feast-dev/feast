"use strict";

var _interopRequireWildcard = require("@babel/runtime/helpers/interopRequireWildcard");

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

var _day = _interopRequireDefault(require("./day"));

var _week_number = _interopRequireDefault(require("./week_number"));

var utils = _interopRequireWildcard(require("./date_utils"));

var _react2 = require("@emotion/react");

function _createSuper(Derived) { var hasNativeReflectConstruct = _isNativeReflectConstruct(); return function _createSuperInternal() { var Super = (0, _getPrototypeOf2.default)(Derived), result; if (hasNativeReflectConstruct) { var NewTarget = (0, _getPrototypeOf2.default)(this).constructor; result = Reflect.construct(Super, arguments, NewTarget); } else { result = Super.apply(this, arguments); } return (0, _possibleConstructorReturn2.default)(this, result); }; }

function _isNativeReflectConstruct() { if (typeof Reflect === "undefined" || !Reflect.construct) return false; if (Reflect.construct.sham) return false; if (typeof Proxy === "function") return true; try { Date.prototype.toString.call(Reflect.construct(Date, [], function () {})); return true; } catch (e) { return false; } }

var Week = /*#__PURE__*/function (_React$Component) {
  (0, _inherits2.default)(Week, _React$Component);

  var _super = _createSuper(Week);

  function Week() {
    var _this;

    (0, _classCallCheck2.default)(this, Week);

    for (var _len = arguments.length, args = new Array(_len), _key = 0; _key < _len; _key++) {
      args[_key] = arguments[_key];
    }

    _this = _super.call.apply(_super, [this].concat(args));
    (0, _defineProperty2.default)((0, _assertThisInitialized2.default)(_this), "handleDayClick", function (day, event) {
      if (_this.props.onDayClick) {
        _this.props.onDayClick(day, event);
      }
    });
    (0, _defineProperty2.default)((0, _assertThisInitialized2.default)(_this), "handleDayMouseEnter", function (day) {
      if (_this.props.onDayMouseEnter) {
        _this.props.onDayMouseEnter(day);
      }
    });
    (0, _defineProperty2.default)((0, _assertThisInitialized2.default)(_this), "handleWeekClick", function (day, weekNumber, event) {
      if (typeof _this.props.onWeekSelect === "function") {
        _this.props.onWeekSelect(day, weekNumber, event);
      }

      if (_this.props.shouldCloseOnSelect) {
        _this.props.setOpen(false);
      }
    });
    (0, _defineProperty2.default)((0, _assertThisInitialized2.default)(_this), "formatWeekNumber", function (startOfWeek) {
      if (_this.props.formatWeekNumber) {
        return _this.props.formatWeekNumber(startOfWeek);
      }

      return utils.getWeek(startOfWeek);
    });
    (0, _defineProperty2.default)((0, _assertThisInitialized2.default)(_this), "renderDays", function () {
      var startOfWeek = utils.getStartOfWeek(utils.cloneDate(_this.props.day));
      var days = [];

      var weekNumber = _this.formatWeekNumber(startOfWeek);

      if (_this.props.showWeekNumber) {
        var onClickAction = _this.props.onWeekSelect ? _this.handleWeekClick.bind((0, _assertThisInitialized2.default)(_this), startOfWeek, weekNumber) : undefined;
        days.push((0, _react2.jsx)(_week_number.default, {
          key: "W",
          weekNumber: weekNumber,
          onClick: onClickAction
        }));
      }

      return days.concat([0, 1, 2, 3, 4, 5, 6].map(function (offset) {
        var day = utils.addDays(utils.cloneDate(startOfWeek), offset);
        return (0, _react2.jsx)(_day.default, {
          key: offset,
          day: day,
          month: _this.props.month,
          onClick: _this.handleDayClick.bind((0, _assertThisInitialized2.default)(_this), day),
          onMouseEnter: _this.handleDayMouseEnter.bind((0, _assertThisInitialized2.default)(_this), day),
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

  (0, _createClass2.default)(Week, [{
    key: "render",
    value: function render() {
      return (0, _react2.jsx)("div", {
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
}(_react.default.Component);

exports.default = Week;
(0, _defineProperty2.default)(Week, "propTypes", {
  disabledKeyboardNavigation: _propTypes.default.bool,
  day: _propTypes.default.object.isRequired,
  dayClassName: _propTypes.default.func,
  endDate: _propTypes.default.object,
  excludeDates: _propTypes.default.array,
  filterDate: _propTypes.default.func,
  formatWeekNumber: _propTypes.default.func,
  highlightDates: _propTypes.default.instanceOf(Map),
  includeDates: _propTypes.default.array,
  inline: _propTypes.default.bool,
  maxDate: _propTypes.default.object,
  minDate: _propTypes.default.object,
  month: _propTypes.default.number,
  onDayClick: _propTypes.default.func,
  onDayMouseEnter: _propTypes.default.func,
  onWeekSelect: _propTypes.default.func,
  preSelection: _propTypes.default.object,
  selected: _propTypes.default.object,
  selectingDate: _propTypes.default.object,
  selectsEnd: _propTypes.default.bool,
  selectsStart: _propTypes.default.bool,
  showWeekNumber: _propTypes.default.bool,
  startDate: _propTypes.default.object,
  setOpen: _propTypes.default.func,
  shouldCloseOnSelect: _propTypes.default.bool,
  utcOffset: _propTypes.default.oneOfType([_propTypes.default.number, _propTypes.default.string]),
  renderDayContents: _propTypes.default.func,
  accessibleMode: _propTypes.default.bool
});
module.exports = exports.default;