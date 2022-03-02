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

var _year_dropdown_options = _interopRequireDefault(require("./year_dropdown_options"));

var _date_utils = require("./date_utils");

var _react2 = require("@emotion/react");

function _createSuper(Derived) { var hasNativeReflectConstruct = _isNativeReflectConstruct(); return function _createSuperInternal() { var Super = (0, _getPrototypeOf2.default)(Derived), result; if (hasNativeReflectConstruct) { var NewTarget = (0, _getPrototypeOf2.default)(this).constructor; result = Reflect.construct(Super, arguments, NewTarget); } else { result = Super.apply(this, arguments); } return (0, _possibleConstructorReturn2.default)(this, result); }; }

function _isNativeReflectConstruct() { if (typeof Reflect === "undefined" || !Reflect.construct) return false; if (Reflect.construct.sham) return false; if (typeof Proxy === "function") return true; try { Date.prototype.toString.call(Reflect.construct(Date, [], function () {})); return true; } catch (e) { return false; } }

var YearDropdown = /*#__PURE__*/function (_React$Component) {
  (0, _inherits2.default)(YearDropdown, _React$Component);

  var _super = _createSuper(YearDropdown);

  function YearDropdown() {
    var _this;

    (0, _classCallCheck2.default)(this, YearDropdown);

    for (var _len = arguments.length, args = new Array(_len), _key = 0; _key < _len; _key++) {
      args[_key] = arguments[_key];
    }

    _this = _super.call.apply(_super, [this].concat(args));
    (0, _defineProperty2.default)((0, _assertThisInitialized2.default)(_this), "state", {
      dropdownVisible: false
    });
    (0, _defineProperty2.default)((0, _assertThisInitialized2.default)(_this), "setReadViewRef", function (ref) {
      _this.readViewref = ref;

      _this.props.buttonRef(ref);
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
      var minYear = _this.props.minDate ? (0, _date_utils.getYear)(_this.props.minDate) : 1900;
      var maxYear = _this.props.maxDate ? (0, _date_utils.getYear)(_this.props.maxDate) : 2100;
      var options = [];

      for (var i = minYear; i <= maxYear; i++) {
        options.push((0, _react2.jsx)("option", {
          key: i,
          value: i
        }, i));
      }

      return options;
    });
    (0, _defineProperty2.default)((0, _assertThisInitialized2.default)(_this), "onSelectChange", function (e) {
      _this.onChange(e.target.value);
    });
    (0, _defineProperty2.default)((0, _assertThisInitialized2.default)(_this), "renderSelectMode", function () {
      return (0, _react2.jsx)("select", {
        value: _this.props.year,
        className: "react-datepicker__year-select",
        onChange: _this.onSelectChange
      }, _this.renderSelectOptions());
    });
    (0, _defineProperty2.default)((0, _assertThisInitialized2.default)(_this), "renderReadView", function (visible) {
      return (0, _react2.jsx)("div", {
        key: "read",
        ref: _this.setReadViewRef,
        style: {
          visibility: visible ? "visible" : "hidden"
        },
        className: "react-datepicker__year-read-view",
        onClick: function onClick(event) {
          return _this.toggleDropdown(event);
        },
        onKeyDown: _this.onReadViewKeyDown,
        tabIndex: _this.props.accessibleMode ? "0" : undefined,
        "aria-label": "Button. Open the year selector. ".concat(_this.props.year, " is currently selected.")
      }, (0, _react2.jsx)("span", {
        className: "react-datepicker__year-read-view--down-arrow"
      }), (0, _react2.jsx)("span", {
        className: "react-datepicker__year-read-view--selected-year"
      }, _this.props.year));
    });
    (0, _defineProperty2.default)((0, _assertThisInitialized2.default)(_this), "renderDropdown", function () {
      return (0, _react2.jsx)(_year_dropdown_options.default, {
        key: "dropdown",
        ref: "options",
        year: _this.props.year,
        onChange: _this.onChange,
        onCancel: _this.toggleDropdown,
        minDate: _this.props.minDate,
        maxDate: _this.props.maxDate,
        scrollableYearDropdown: _this.props.scrollableYearDropdown,
        yearDropdownItemNumber: _this.props.yearDropdownItemNumber,
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
    (0, _defineProperty2.default)((0, _assertThisInitialized2.default)(_this), "onChange", function (year) {
      _this.toggleDropdown();

      if (year === _this.props.year) return;

      _this.props.onChange(year);
    });
    (0, _defineProperty2.default)((0, _assertThisInitialized2.default)(_this), "toggleDropdown", function () {
      var isOpen = !_this.state.dropdownVisible;

      _this.setState({
        dropdownVisible: isOpen
      });

      _this.props.onDropdownToggle(isOpen, 'year');
    });
    (0, _defineProperty2.default)((0, _assertThisInitialized2.default)(_this), "onSelect", function (date, event) {
      if (_this.props.onSelect) {
        _this.props.onSelect(date, event);
      }
    });
    return _this;
  }

  (0, _createClass2.default)(YearDropdown, [{
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
        className: "react-datepicker__year-dropdown-container react-datepicker__year-dropdown-container--".concat(this.props.dropdownMode)
      }, renderedDropdown);
    }
  }]);
  return YearDropdown;
}(_react.default.Component);

exports.default = YearDropdown;
(0, _defineProperty2.default)(YearDropdown, "propTypes", {
  adjustDateOnChange: _propTypes.default.bool,
  dropdownMode: _propTypes.default.oneOf(["scroll", "select"]).isRequired,
  maxDate: _propTypes.default.object,
  minDate: _propTypes.default.object,
  onChange: _propTypes.default.func.isRequired,
  scrollableYearDropdown: _propTypes.default.bool,
  year: _propTypes.default.number.isRequired,
  yearDropdownItemNumber: _propTypes.default.number,
  date: _propTypes.default.object,
  onSelect: _propTypes.default.func,
  setOpen: _propTypes.default.func,
  accessibleMode: _propTypes.default.bool,
  onDropdownToggle: _propTypes.default.func,
  buttonRef: _propTypes.default.func
});
module.exports = exports.default;