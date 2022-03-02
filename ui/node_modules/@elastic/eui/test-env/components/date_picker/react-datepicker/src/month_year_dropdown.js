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

var _month_year_dropdown_options = _interopRequireDefault(require("./month_year_dropdown_options"));

var _date_utils = require("./date_utils");

var _react2 = require("@emotion/react");

function _createSuper(Derived) { var hasNativeReflectConstruct = _isNativeReflectConstruct(); return function _createSuperInternal() { var Super = (0, _getPrototypeOf2.default)(Derived), result; if (hasNativeReflectConstruct) { var NewTarget = (0, _getPrototypeOf2.default)(this).constructor; result = Reflect.construct(Super, arguments, NewTarget); } else { result = Super.apply(this, arguments); } return (0, _possibleConstructorReturn2.default)(this, result); }; }

function _isNativeReflectConstruct() { if (typeof Reflect === "undefined" || !Reflect.construct) return false; if (Reflect.construct.sham) return false; if (typeof Proxy === "function") return true; try { Date.prototype.toString.call(Reflect.construct(Date, [], function () {})); return true; } catch (e) { return false; } }

var MonthYearDropdown = /*#__PURE__*/function (_React$Component) {
  (0, _inherits2.default)(MonthYearDropdown, _React$Component);

  var _super = _createSuper(MonthYearDropdown);

  function MonthYearDropdown() {
    var _this;

    (0, _classCallCheck2.default)(this, MonthYearDropdown);

    for (var _len = arguments.length, args = new Array(_len), _key = 0; _key < _len; _key++) {
      args[_key] = arguments[_key];
    }

    _this = _super.call.apply(_super, [this].concat(args));
    (0, _defineProperty2.default)((0, _assertThisInitialized2.default)(_this), "state", {
      dropdownVisible: false
    });
    (0, _defineProperty2.default)((0, _assertThisInitialized2.default)(_this), "setReadViewRef", function (ref) {
      _this.readViewref = ref;
    });
    (0, _defineProperty2.default)((0, _assertThisInitialized2.default)(_this), "onReadViewKeyDown", function (event) {
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
    (0, _defineProperty2.default)((0, _assertThisInitialized2.default)(_this), "onDropDownKeyDown", function (event) {
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
    (0, _defineProperty2.default)((0, _assertThisInitialized2.default)(_this), "renderSelectOptions", function () {
      var currDate = (0, _date_utils.getStartOfMonth)((0, _date_utils.localizeDate)(_this.props.minDate, _this.props.locale));
      var lastDate = (0, _date_utils.getStartOfMonth)((0, _date_utils.localizeDate)(_this.props.maxDate, _this.props.locale));
      var options = [];

      while (!(0, _date_utils.isAfter)(currDate, lastDate)) {
        var timepoint = currDate.valueOf();
        options.push((0, _react2.jsx)("option", {
          key: timepoint,
          value: timepoint
        }, (0, _date_utils.formatDate)(currDate, _this.props.dateFormat)));
        (0, _date_utils.addMonths)(currDate, 1);
      }

      return options;
    });
    (0, _defineProperty2.default)((0, _assertThisInitialized2.default)(_this), "onSelectChange", function (e) {
      _this.onChange(e.target.value);
    });
    (0, _defineProperty2.default)((0, _assertThisInitialized2.default)(_this), "renderSelectMode", function () {
      return (0, _react2.jsx)("select", {
        value: (0, _date_utils.getStartOfMonth)(_this.props.date).valueOf(),
        className: "react-datepicker__month-year-select",
        onChange: _this.onSelectChange
      }, _this.renderSelectOptions());
    });
    (0, _defineProperty2.default)((0, _assertThisInitialized2.default)(_this), "renderReadView", function (visible) {
      var yearMonth = (0, _date_utils.formatDate)((0, _date_utils.localizeDate)((0, _date_utils.newDate)(_this.props.date), _this.props.locale), _this.props.dateFormat);
      return (0, _react2.jsx)("div", {
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
      }, (0, _react2.jsx)("span", {
        className: "react-datepicker__month-year-read-view--down-arrow"
      }), (0, _react2.jsx)("span", {
        className: "react-datepicker__month-year-read-view--selected-month-year"
      }, yearMonth));
    });
    (0, _defineProperty2.default)((0, _assertThisInitialized2.default)(_this), "renderDropdown", function () {
      return (0, _react2.jsx)(_month_year_dropdown_options.default, {
        key: "dropdown",
        ref: "options",
        date: _this.props.date,
        dateFormat: _this.props.dateFormat,
        onChange: _this.onChange,
        onCancel: _this.toggleDropdown,
        minDate: (0, _date_utils.localizeDate)(_this.props.minDate, _this.props.locale),
        maxDate: (0, _date_utils.localizeDate)(_this.props.maxDate, _this.props.locale),
        scrollableMonthYearDropdown: _this.props.scrollableMonthYearDropdown,
        accessibleMode: _this.props.accessibleMode
      });
    });
    (0, _defineProperty2.default)((0, _assertThisInitialized2.default)(_this), "renderScrollMode", function () {
      var dropdownVisible = _this.state.dropdownVisible;
      var result = [_this.renderReadView(!dropdownVisible)];

      if (dropdownVisible) {
        result.unshift(_this.renderDropdown());
      }

      return result;
    });
    (0, _defineProperty2.default)((0, _assertThisInitialized2.default)(_this), "onChange", function (monthYearPoint) {
      _this.toggleDropdown();

      var changedDate = (0, _date_utils.newDate)(parseInt(monthYearPoint));

      if ((0, _date_utils.isSameYear)(_this.props.date, changedDate) && (0, _date_utils.isSameMonth)(_this.props.date, changedDate)) {
        return;
      }

      _this.props.onChange(changedDate);
    });
    (0, _defineProperty2.default)((0, _assertThisInitialized2.default)(_this), "toggleDropdown", function () {
      return _this.setState({
        dropdownVisible: !_this.state.dropdownVisible
      });
    });
    return _this;
  }

  (0, _createClass2.default)(MonthYearDropdown, [{
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

      return (0, _react2.jsx)("div", {
        className: "react-datepicker__month-year-dropdown-container react-datepicker__month-year-dropdown-container--".concat(this.props.dropdownMode)
      }, renderedDropdown);
    }
  }]);
  return MonthYearDropdown;
}(_react.default.Component);

exports.default = MonthYearDropdown;
(0, _defineProperty2.default)(MonthYearDropdown, "propTypes", {
  dropdownMode: _propTypes.default.oneOf(["scroll", "select"]).isRequired,
  dateFormat: _propTypes.default.string.isRequired,
  locale: _propTypes.default.string,
  maxDate: _propTypes.default.object.isRequired,
  minDate: _propTypes.default.object.isRequired,
  date: _propTypes.default.object.isRequired,
  onChange: _propTypes.default.func.isRequired,
  scrollableMonthYearDropdown: _propTypes.default.bool,
  accessibleMode: _propTypes.default.bool
});
module.exports = exports.default;