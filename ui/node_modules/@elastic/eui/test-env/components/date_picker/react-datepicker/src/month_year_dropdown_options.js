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

var _focus_trap = require("../../../focus_trap");

var _accessibility = require("../../../accessibility");

var _react2 = require("@emotion/react");

function _createSuper(Derived) { var hasNativeReflectConstruct = _isNativeReflectConstruct(); return function _createSuperInternal() { var Super = (0, _getPrototypeOf2.default)(Derived), result; if (hasNativeReflectConstruct) { var NewTarget = (0, _getPrototypeOf2.default)(this).constructor; result = Reflect.construct(Super, arguments, NewTarget); } else { result = Super.apply(this, arguments); } return (0, _possibleConstructorReturn2.default)(this, result); }; }

function _isNativeReflectConstruct() { if (typeof Reflect === "undefined" || !Reflect.construct) return false; if (Reflect.construct.sham) return false; if (typeof Proxy === "function") return true; try { Date.prototype.toString.call(Reflect.construct(Date, [], function () {})); return true; } catch (e) { return false; } }

function generateMonthYears(minDate, maxDate) {
  var list = [];
  var currDate = (0, _date_utils.getStartOfMonth)((0, _date_utils.cloneDate)(minDate));
  var lastDate = (0, _date_utils.getStartOfMonth)((0, _date_utils.cloneDate)(maxDate));

  while (!(0, _date_utils.isAfter)(currDate, lastDate)) {
    list.push((0, _date_utils.cloneDate)(currDate));
    (0, _date_utils.addMonths)(currDate, 1);
  }

  return list;
}

var MonthYearDropdownOptions = /*#__PURE__*/function (_React$Component) {
  (0, _inherits2.default)(MonthYearDropdownOptions, _React$Component);

  var _super = _createSuper(MonthYearDropdownOptions);

  function MonthYearDropdownOptions(props) {
    var _this;

    (0, _classCallCheck2.default)(this, MonthYearDropdownOptions);
    _this = _super.call(this, props);
    (0, _defineProperty2.default)((0, _assertThisInitialized2.default)(_this), "renderOptions", function () {
      return _this.state.monthYearsList.map(function (monthYear) {
        var monthYearPoint = monthYear.valueOf();
        var isSameMonthYear = (0, _date_utils.isSameYear)(_this.props.date, monthYear) && (0, _date_utils.isSameMonth)(_this.props.date, monthYear);
        var isPreselectionSameMonthYear = (0, _date_utils.isSameYear)(_this.state.preSelection, monthYear) && (0, _date_utils.isSameMonth)(_this.state.preSelection, monthYear);
        return (0, _react2.jsx)("div", {
          className: (0, _classnames.default)("react-datepicker__month-year-option", {
            "--selected_month-year": isSameMonthYear,
            "react-datepicker__month-year-option--preselected": _this.props.accessibleMode && isPreselectionSameMonthYear
          }),
          key: monthYearPoint,
          ref: function ref(div) {
            if (_this.props.accessibleMode && isPreselectionSameMonthYear) {
              _this.preSelectionDiv = div;
            }
          },
          onClick: _this.onChange.bind((0, _assertThisInitialized2.default)(_this), monthYearPoint)
        }, isSameMonthYear ? (0, _react2.jsx)("span", {
          className: "react-datepicker__month-year-option--selected"
        }, "\u2713") : "", (0, _date_utils.formatDate)(monthYear, _this.props.dateFormat));
      });
    });
    (0, _defineProperty2.default)((0, _assertThisInitialized2.default)(_this), "onFocus", function () {
      if (_this.props.accessibleMode) {
        _this.setState({
          readInstructions: true
        });
      }
    });
    (0, _defineProperty2.default)((0, _assertThisInitialized2.default)(_this), "onChange", function (monthYear) {
      return _this.props.onChange(monthYear);
    });
    (0, _defineProperty2.default)((0, _assertThisInitialized2.default)(_this), "handleClickOutside", function () {
      _this.props.onCancel();
    });
    (0, _defineProperty2.default)((0, _assertThisInitialized2.default)(_this), "onInputKeyDown", function (event) {
      var eventKey = event.key;
      var newSelection;

      switch (eventKey) {
        case "ArrowUp":
          event.preventDefault();
          event.stopPropagation();
          newSelection = (0, _date_utils.addMonths)((0, _date_utils.cloneDate)(_this.state.preSelection), -1);
          break;

        case "ArrowDown":
          event.preventDefault();
          event.stopPropagation();
          newSelection = (0, _date_utils.addMonths)((0, _date_utils.cloneDate)(_this.state.preSelection), 1);
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
        if ((0, _date_utils.isBefore)(newSelection, minMonthYear)) newSelection = maxMonthYear;
        if ((0, _date_utils.isAfter)(newSelection, maxMonthYear)) newSelection = minMonthYear;

        _this.setState({
          preSelection: newSelection
        });
      }
    });
    _this.state = {
      monthYearsList: generateMonthYears(_this.props.minDate, _this.props.maxDate),
      preSelection: (0, _date_utils.getStartOfMonth)((0, _date_utils.cloneDate)(_this.props.date)),
      readInstructions: false
    };
    return _this;
  }

  (0, _createClass2.default)(MonthYearDropdownOptions, [{
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
      var dropdownClass = (0, _classnames.default)({
        "react-datepicker__month-year-dropdown": true,
        "react-datepicker__month-year-dropdown--scrollable": this.props.scrollableMonthYearDropdown
      });
      var screenReaderInstructions;

      if (this.state.readInstructions) {
        screenReaderInstructions = (0, _react2.jsx)("p", {
          "aria-live": true
        }, "You are focused on a month / year selector menu. Use the up and down arrows to select a month / year combination, then hit enter to confirm your selection.", (0, _date_utils.formatDate)(this.state.preSelection, this.props.dateFormat), " is the currently focused month / year.");
      }

      return this.props.accessibleMode ? (0, _react2.jsx)(_focus_trap.EuiFocusTrap, {
        onClickOutside: this.handleClickOutside
      }, (0, _react2.jsx)("div", {
        className: dropdownClass,
        tabIndex: "0",
        onKeyDown: this.onInputKeyDown,
        onFocus: this.onFocus
      }, (0, _react2.jsx)(_accessibility.EuiScreenReaderOnly, null, (0, _react2.jsx)("span", null, screenReaderInstructions)), this.renderOptions())) : (0, _react2.jsx)("div", {
        className: dropdownClass
      }, this.renderOptions());
      return (0, _react2.jsx)("div", {
        className: dropdownClass
      }, this.renderOptions());
    }
  }]);
  return MonthYearDropdownOptions;
}(_react.default.Component);

exports.default = MonthYearDropdownOptions;
(0, _defineProperty2.default)(MonthYearDropdownOptions, "propTypes", {
  minDate: _propTypes.default.object.isRequired,
  maxDate: _propTypes.default.object.isRequired,
  onCancel: _propTypes.default.func.isRequired,
  onChange: _propTypes.default.func.isRequired,
  scrollableMonthYearDropdown: _propTypes.default.bool,
  date: _propTypes.default.object.isRequired,
  dateFormat: _propTypes.default.string.isRequired,
  accessibleMode: _propTypes.default.bool
});
module.exports = exports.default;