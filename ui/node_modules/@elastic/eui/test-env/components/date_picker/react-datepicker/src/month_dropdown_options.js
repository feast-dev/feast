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

var MonthDropdownOptions = /*#__PURE__*/function (_React$Component) {
  (0, _inherits2.default)(MonthDropdownOptions, _React$Component);

  var _super = _createSuper(MonthDropdownOptions);

  function MonthDropdownOptions() {
    var _this;

    (0, _classCallCheck2.default)(this, MonthDropdownOptions);

    for (var _len = arguments.length, args = new Array(_len), _key = 0; _key < _len; _key++) {
      args[_key] = arguments[_key];
    }

    _this = _super.call.apply(_super, [this].concat(args));
    (0, _defineProperty2.default)((0, _assertThisInitialized2.default)(_this), "renderOptions", function () {
      return _this.props.monthNames.map(function (month, i) {
        return (0, _react2.jsx)("div", {
          className: (0, _classnames.default)("react-datepicker__month-option", {
            "react-datepicker__month-option--selected_month": _this.props.month === i,
            "react-datepicker__month-option--preselected": _this.props.accessibleMode && _this.state.preSelection === i
          }),
          key: month,
          ref: month,
          onClick: _this.onChange.bind((0, _assertThisInitialized2.default)(_this), i)
        }, _this.props.month === i ? (0, _react2.jsx)("span", {
          className: "react-datepicker__month-option--selected"
        }, "\u2713") : "", month);
      });
    });
    (0, _defineProperty2.default)((0, _assertThisInitialized2.default)(_this), "onFocus", function () {
      if (_this.props.accessibleMode) {
        _this.setState({
          readInstructions: true
        });
      }
    });
    (0, _defineProperty2.default)((0, _assertThisInitialized2.default)(_this), "onChange", function (month) {
      return _this.props.onChange(month);
    });
    (0, _defineProperty2.default)((0, _assertThisInitialized2.default)(_this), "handleClickOutside", function () {
      return _this.props.onCancel();
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

  (0, _createClass2.default)(MonthDropdownOptions, [{
    key: "render",
    value: function render() {
      var screenReaderInstructions;

      if (this.state.readInstructions) {
        screenReaderInstructions = (0, _react2.jsx)("p", {
          "aria-live": true
        }, "You are focused on a month selector menu. Use the up and down arrows to select a year, then hit enter to confirm your selection.", this.props.monthNames[this.state.preSelection], " is the currently focused month.");
      }

      return this.props.accessibleMode ? (0, _react2.jsx)(_focus_trap.EuiFocusTrap, {
        onClickOutside: this.handleClickOutside
      }, (0, _react2.jsx)("div", {
        className: "react-datepicker__month-dropdown",
        tabIndex: "0",
        onKeyDown: this.onInputKeyDown,
        onFocus: this.onFocus
      }, (0, _react2.jsx)(_accessibility.EuiScreenReaderOnly, null, (0, _react2.jsx)("span", null, screenReaderInstructions)), this.renderOptions())) : (0, _react2.jsx)("div", {
        className: "react-datepicker__month-dropdown"
      }, this.renderOptions());
    }
  }]);
  return MonthDropdownOptions;
}(_react.default.Component);

exports.default = MonthDropdownOptions;
(0, _defineProperty2.default)(MonthDropdownOptions, "propTypes", {
  onCancel: _propTypes.default.func.isRequired,
  onChange: _propTypes.default.func.isRequired,
  month: _propTypes.default.number.isRequired,
  monthNames: _propTypes.default.arrayOf(_propTypes.default.string.isRequired).isRequired,
  accessibleMode: _propTypes.default.bool
});
module.exports = exports.default;