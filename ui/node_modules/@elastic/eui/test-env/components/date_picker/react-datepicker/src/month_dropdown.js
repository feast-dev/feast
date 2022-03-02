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

var _month_dropdown_options = _interopRequireDefault(require("./month_dropdown_options"));

var utils = _interopRequireWildcard(require("./date_utils"));

var _react2 = require("@emotion/react");

function _createSuper(Derived) { var hasNativeReflectConstruct = _isNativeReflectConstruct(); return function _createSuperInternal() { var Super = (0, _getPrototypeOf2.default)(Derived), result; if (hasNativeReflectConstruct) { var NewTarget = (0, _getPrototypeOf2.default)(this).constructor; result = Reflect.construct(Super, arguments, NewTarget); } else { result = Super.apply(this, arguments); } return (0, _possibleConstructorReturn2.default)(this, result); }; }

function _isNativeReflectConstruct() { if (typeof Reflect === "undefined" || !Reflect.construct) return false; if (Reflect.construct.sham) return false; if (typeof Proxy === "function") return true; try { Date.prototype.toString.call(Reflect.construct(Date, [], function () {})); return true; } catch (e) { return false; } }

var MonthDropdown = /*#__PURE__*/function (_React$Component) {
  (0, _inherits2.default)(MonthDropdown, _React$Component);

  var _super = _createSuper(MonthDropdown);

  function MonthDropdown(props) {
    var _this;

    (0, _classCallCheck2.default)(this, MonthDropdown);
    _this = _super.call(this, props);
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
    (0, _defineProperty2.default)((0, _assertThisInitialized2.default)(_this), "renderSelectOptions", function (monthNames) {
      return monthNames.map(function (M, i) {
        return (0, _react2.jsx)("option", {
          key: i,
          value: i
        }, M);
      });
    });
    (0, _defineProperty2.default)((0, _assertThisInitialized2.default)(_this), "renderSelectMode", function (monthNames) {
      return (0, _react2.jsx)("select", {
        value: _this.props.month,
        className: "react-datepicker__month-select",
        onChange: function onChange(e) {
          return _this.onChange(e.target.value);
        }
      }, _this.renderSelectOptions(monthNames));
    });
    (0, _defineProperty2.default)((0, _assertThisInitialized2.default)(_this), "renderReadView", function (visible, monthNames) {
      return (0, _react2.jsx)("div", {
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
      }, (0, _react2.jsx)("span", {
        className: "react-datepicker__month-read-view--down-arrow"
      }), (0, _react2.jsx)("span", {
        className: "react-datepicker__month-read-view--selected-month"
      }, monthNames[_this.props.month]));
    });
    (0, _defineProperty2.default)((0, _assertThisInitialized2.default)(_this), "renderDropdown", function (monthNames) {
      return (0, _react2.jsx)(_month_dropdown_options.default, {
        key: "dropdown",
        ref: "options",
        month: _this.props.month,
        monthNames: monthNames,
        onChange: _this.onChange,
        onCancel: _this.toggleDropdown,
        accessibleMode: _this.props.accessibleMode
      });
    });
    (0, _defineProperty2.default)((0, _assertThisInitialized2.default)(_this), "renderScrollMode", function (monthNames) {
      var dropdownVisible = _this.state.dropdownVisible;
      var result = [_this.renderReadView(!dropdownVisible, monthNames)];

      if (dropdownVisible) {
        result.unshift(_this.renderDropdown(monthNames));
      }

      return result;
    });
    (0, _defineProperty2.default)((0, _assertThisInitialized2.default)(_this), "onChange", function (month) {
      _this.toggleDropdown();

      if (month !== _this.props.month) {
        _this.props.onChange(month);
      }
    });
    (0, _defineProperty2.default)((0, _assertThisInitialized2.default)(_this), "toggleDropdown", function () {
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

  (0, _createClass2.default)(MonthDropdown, [{
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

      return (0, _react2.jsx)("div", {
        className: "react-datepicker__month-dropdown-container react-datepicker__month-dropdown-container--".concat(this.props.dropdownMode)
      }, renderedDropdown);
    }
  }]);
  return MonthDropdown;
}(_react.default.Component);

exports.default = MonthDropdown;
(0, _defineProperty2.default)(MonthDropdown, "propTypes", {
  dropdownMode: _propTypes.default.oneOf(["scroll", "select"]).isRequired,
  locale: _propTypes.default.string,
  dateFormat: _propTypes.default.string.isRequired,
  month: _propTypes.default.number.isRequired,
  onChange: _propTypes.default.func.isRequired,
  useShortMonthInDropdown: _propTypes.default.bool,
  accessibleMode: _propTypes.default.bool,
  onDropdownToggle: _propTypes.default.func,
  buttonRef: _propTypes.default.func
});
module.exports = exports.default;