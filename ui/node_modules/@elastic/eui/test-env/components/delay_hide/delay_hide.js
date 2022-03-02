"use strict";

var _interopRequireDefault = require("@babel/runtime/helpers/interopRequireDefault");

Object.defineProperty(exports, "__esModule", {
  value: true
});
exports.EuiDelayHide = void 0;

var _classCallCheck2 = _interopRequireDefault(require("@babel/runtime/helpers/classCallCheck"));

var _createClass2 = _interopRequireDefault(require("@babel/runtime/helpers/createClass"));

var _assertThisInitialized2 = _interopRequireDefault(require("@babel/runtime/helpers/assertThisInitialized"));

var _inherits2 = _interopRequireDefault(require("@babel/runtime/helpers/inherits"));

var _possibleConstructorReturn2 = _interopRequireDefault(require("@babel/runtime/helpers/possibleConstructorReturn"));

var _getPrototypeOf2 = _interopRequireDefault(require("@babel/runtime/helpers/getPrototypeOf"));

var _defineProperty2 = _interopRequireDefault(require("@babel/runtime/helpers/defineProperty"));

var _react = require("react");

var _propTypes = _interopRequireDefault(require("prop-types"));

function _createSuper(Derived) { var hasNativeReflectConstruct = _isNativeReflectConstruct(); return function _createSuperInternal() { var Super = (0, _getPrototypeOf2.default)(Derived), result; if (hasNativeReflectConstruct) { var NewTarget = (0, _getPrototypeOf2.default)(this).constructor; result = Reflect.construct(Super, arguments, NewTarget); } else { result = Super.apply(this, arguments); } return (0, _possibleConstructorReturn2.default)(this, result); }; }

function _isNativeReflectConstruct() { if (typeof Reflect === "undefined" || !Reflect.construct) return false; if (Reflect.construct.sham) return false; if (typeof Proxy === "function") return true; try { Date.prototype.toString.call(Reflect.construct(Date, [], function () {})); return true; } catch (e) { return false; } }

function isComponentBecomingVisible() {
  var prevHide = arguments.length > 0 && arguments[0] !== undefined ? arguments[0] : false;
  var nextHide = arguments.length > 1 && arguments[1] !== undefined ? arguments[1] : false;
  return prevHide === true && nextHide === false;
}

var EuiDelayHide = /*#__PURE__*/function (_Component) {
  (0, _inherits2.default)(EuiDelayHide, _Component);

  var _super = _createSuper(EuiDelayHide);

  function EuiDelayHide() {
    var _this;

    (0, _classCallCheck2.default)(this, EuiDelayHide);

    for (var _len = arguments.length, args = new Array(_len), _key = 0; _key < _len; _key++) {
      args[_key] = arguments[_key];
    }

    _this = _super.call.apply(_super, [this].concat(args));
    (0, _defineProperty2.default)((0, _assertThisInitialized2.default)(_this), "state", {
      hide: _this.props.hide,
      countdownExpired: _this.props.hide
    });
    (0, _defineProperty2.default)((0, _assertThisInitialized2.default)(_this), "timeoutId", void 0);
    (0, _defineProperty2.default)((0, _assertThisInitialized2.default)(_this), "startCountdown", function () {
      // only start the countdown if there is not one in progress
      if (_this.timeoutId == null) {
        _this.timeoutId = setTimeout(_this.finishCountdown, // even though `minimumDuration` cannot be undefined, passing a strict number type to setTimeout makes TS interpret
        // it as a NodeJS.Timer instead of a number. The DOM lib defines the setTimeout call as taking `number | undefined`
        // so we cast minimumDuration to this type instead to force TS's cooperation
        _this.props.minimumDuration);
      }
    });
    (0, _defineProperty2.default)((0, _assertThisInitialized2.default)(_this), "finishCountdown", function () {
      _this.timeoutId = undefined;

      _this.setState({
        countdownExpired: true
      });
    });
    return _this;
  }

  (0, _createClass2.default)(EuiDelayHide, [{
    key: "componentDidMount",
    value: function componentDidMount() {
      // if the component begins visible start counting
      if (this.props.hide === false) {
        this.startCountdown();
      }
    }
  }, {
    key: "componentDidUpdate",
    value: function componentDidUpdate(prevProps) {
      var isBecomingVisible = isComponentBecomingVisible(prevProps.hide, this.props.hide);

      if (isBecomingVisible) {
        this.startCountdown();
      }
    }
  }, {
    key: "componentWillUnmount",
    value: function componentWillUnmount() {
      if (this.timeoutId != null) {
        clearTimeout(this.timeoutId);
      }
    }
  }, {
    key: "render",
    value: function render() {
      var shouldHideContent = this.props.hide === true && this.state.countdownExpired;
      return shouldHideContent ? null : this.props.render();
    }
  }], [{
    key: "getDerivedStateFromProps",
    value: function getDerivedStateFromProps(nextProps, prevState) {
      var isBecomingVisible = isComponentBecomingVisible(prevState.hide, nextProps.hide);
      return {
        hide: nextProps.hide,
        countdownExpired: isBecomingVisible ? false : prevState.countdownExpired
      };
    }
  }]);
  return EuiDelayHide;
}(_react.Component);

exports.EuiDelayHide = EuiDelayHide;
(0, _defineProperty2.default)(EuiDelayHide, "defaultProps", {
  hide: false,
  minimumDuration: 1000
});
EuiDelayHide.propTypes = {
  hide: _propTypes.default.bool.isRequired,
  minimumDuration: _propTypes.default.number.isRequired,
  render: _propTypes.default.func.isRequired
};