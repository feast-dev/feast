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

var _focus_trap = require("../../../focus_trap");

var _accessibility = require("../../../accessibility");

var _react2 = require("@emotion/react");

function _createSuper(Derived) { var hasNativeReflectConstruct = _isNativeReflectConstruct(); return function _createSuperInternal() { var Super = (0, _getPrototypeOf2.default)(Derived), result; if (hasNativeReflectConstruct) { var NewTarget = (0, _getPrototypeOf2.default)(this).constructor; result = Reflect.construct(Super, arguments, NewTarget); } else { result = Super.apply(this, arguments); } return (0, _possibleConstructorReturn2.default)(this, result); }; }

function _isNativeReflectConstruct() { if (typeof Reflect === "undefined" || !Reflect.construct) return false; if (Reflect.construct.sham) return false; if (typeof Proxy === "function") return true; try { Date.prototype.toString.call(Reflect.construct(Date, [], function () {})); return true; } catch (e) { return false; } }

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
  (0, _inherits2.default)(YearDropdownOptions, _React$Component);

  var _super = _createSuper(YearDropdownOptions);

  function YearDropdownOptions(props) {
    var _this;

    (0, _classCallCheck2.default)(this, YearDropdownOptions);
    _this = _super.call(this, props);
    (0, _defineProperty2.default)((0, _assertThisInitialized2.default)(_this), "renderOptions", function () {
      var selectedYear = _this.props.year;

      var options = _this.state.yearsList.map(function (year) {
        return (0, _react2.jsx)("div", {
          className: (0, _classnames.default)("react-datepicker__year-option", {
            "react-datepicker__year-option--selected_year": selectedYear === year,
            "react-datepicker__year-option--preselected": _this.props.accessibleMode && _this.state.preSelection === year
          }),
          key: year,
          ref: function ref(div) {
            if (_this.props.accessibleMode && _this.state.preSelection === year) {
              _this.preSelectionDiv = div;
            }
          },
          onClick: _this.onChange.bind((0, _assertThisInitialized2.default)(_this), year)
        }, selectedYear === year ? (0, _react2.jsx)("span", {
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
    (0, _defineProperty2.default)((0, _assertThisInitialized2.default)(_this), "onFocus", function () {
      if (_this.props.accessibleMode) {
        _this.setState({
          readInstructions: true
        });
      }
    });
    (0, _defineProperty2.default)((0, _assertThisInitialized2.default)(_this), "onChange", function (year) {
      _this.props.onChange(year);
    });
    (0, _defineProperty2.default)((0, _assertThisInitialized2.default)(_this), "handleClickOutside", function () {
      _this.props.onCancel();
    });
    (0, _defineProperty2.default)((0, _assertThisInitialized2.default)(_this), "shiftYears", function (amount) {
      var years = _this.state.yearsList.map(function (year) {
        return year + amount;
      });

      _this.setState({
        yearsList: years
      });
    });
    (0, _defineProperty2.default)((0, _assertThisInitialized2.default)(_this), "incrementYears", function () {
      return _this.shiftYears(1);
    });
    (0, _defineProperty2.default)((0, _assertThisInitialized2.default)(_this), "decrementYears", function () {
      return _this.shiftYears(-1);
    });
    (0, _defineProperty2.default)((0, _assertThisInitialized2.default)(_this), "onInputKeyDown", function (event) {
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

  (0, _createClass2.default)(YearDropdownOptions, [{
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
      var dropdownClass = (0, _classnames.default)({
        "react-datepicker__year-dropdown": true,
        "react-datepicker__year-dropdown--scrollable": this.props.scrollableYearDropdown
      });
      var screenReaderInstructions;

      if (this.state.readInstructions) {
        screenReaderInstructions = (0, _react2.jsx)("p", {
          "aria-live": true
        }, "You are focused on a year selector menu. Use the up and down arrows to select a year, then hit enter to confirm your selection.", this.state.preSelection, " is the currently focused year.");
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
    }
  }]);
  return YearDropdownOptions;
}(_react.default.Component);

exports.default = YearDropdownOptions;
(0, _defineProperty2.default)(YearDropdownOptions, "propTypes", {
  minDate: _propTypes.default.object,
  maxDate: _propTypes.default.object,
  onCancel: _propTypes.default.func.isRequired,
  onChange: _propTypes.default.func.isRequired,
  scrollableYearDropdown: _propTypes.default.bool,
  year: _propTypes.default.number.isRequired,
  yearDropdownItemNumber: _propTypes.default.number,
  accessibleMode: _propTypes.default.bool
});
module.exports = exports.default;