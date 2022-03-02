"use strict";

var _interopRequireDefault = require("@babel/runtime/helpers/interopRequireDefault");

Object.defineProperty(exports, "__esModule", {
  value: true
});
exports.EuiObserver = void 0;

var _classCallCheck2 = _interopRequireDefault(require("@babel/runtime/helpers/classCallCheck"));

var _createClass2 = _interopRequireDefault(require("@babel/runtime/helpers/createClass"));

var _assertThisInitialized2 = _interopRequireDefault(require("@babel/runtime/helpers/assertThisInitialized"));

var _inherits2 = _interopRequireDefault(require("@babel/runtime/helpers/inherits"));

var _possibleConstructorReturn2 = _interopRequireDefault(require("@babel/runtime/helpers/possibleConstructorReturn"));

var _getPrototypeOf2 = _interopRequireDefault(require("@babel/runtime/helpers/getPrototypeOf"));

var _defineProperty2 = _interopRequireDefault(require("@babel/runtime/helpers/defineProperty"));

var _react = require("react");

function _createSuper(Derived) { var hasNativeReflectConstruct = _isNativeReflectConstruct(); return function _createSuperInternal() { var Super = (0, _getPrototypeOf2.default)(Derived), result; if (hasNativeReflectConstruct) { var NewTarget = (0, _getPrototypeOf2.default)(this).constructor; result = Reflect.construct(Super, arguments, NewTarget); } else { result = Super.apply(this, arguments); } return (0, _possibleConstructorReturn2.default)(this, result); }; }

function _isNativeReflectConstruct() { if (typeof Reflect === "undefined" || !Reflect.construct) return false; if (Reflect.construct.sham) return false; if (typeof Proxy === "function") return true; try { Date.prototype.toString.call(Reflect.construct(Date, [], function () {})); return true; } catch (e) { return false; } }

var EuiObserver = /*#__PURE__*/function (_Component) {
  (0, _inherits2.default)(EuiObserver, _Component);

  var _super = _createSuper(EuiObserver);

  function EuiObserver() {
    var _this;

    (0, _classCallCheck2.default)(this, EuiObserver);

    for (var _len = arguments.length, args = new Array(_len), _key = 0; _key < _len; _key++) {
      args[_key] = arguments[_key];
    }

    _this = _super.call.apply(_super, [this].concat(args));
    (0, _defineProperty2.default)((0, _assertThisInitialized2.default)(_this), "name", 'EuiObserver');
    (0, _defineProperty2.default)((0, _assertThisInitialized2.default)(_this), "childNode", null);
    (0, _defineProperty2.default)((0, _assertThisInitialized2.default)(_this), "observer", null);
    (0, _defineProperty2.default)((0, _assertThisInitialized2.default)(_this), "updateChildNode", function (ref) {
      if (_this.childNode === ref) return; // node hasn't changed
      // if there's an existing observer disconnect it

      if (_this.observer != null) {
        _this.observer.disconnect();

        _this.observer = null;
      }

      _this.childNode = ref;

      if (_this.childNode != null) {
        _this.beginObserve();
      }
    });
    (0, _defineProperty2.default)((0, _assertThisInitialized2.default)(_this), "beginObserve", function () {
      throw new Error('EuiObserver has no default observation method');
    });
    return _this;
  }

  (0, _createClass2.default)(EuiObserver, [{
    key: "componentDidMount",
    value: function componentDidMount() {
      if (this.childNode == null) {
        throw new Error("".concat(this.name, " did not receive a ref"));
      }
    }
  }, {
    key: "componentWillUnmount",
    value: function componentWillUnmount() {
      if (this.observer != null) {
        this.observer.disconnect();
      }
    }
  }, {
    key: "render",
    value: function render() {
      var props = this.props;
      return props.children(this.updateChildNode);
    }
  }]);
  return EuiObserver;
}(_react.Component);

exports.EuiObserver = EuiObserver;