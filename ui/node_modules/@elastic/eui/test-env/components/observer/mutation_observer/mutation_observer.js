"use strict";

var _interopRequireDefault = require("@babel/runtime/helpers/interopRequireDefault");

Object.defineProperty(exports, "__esModule", {
  value: true
});
exports.useMutationObserver = exports.EuiMutationObserver = void 0;

var _classCallCheck2 = _interopRequireDefault(require("@babel/runtime/helpers/classCallCheck"));

var _assertThisInitialized2 = _interopRequireDefault(require("@babel/runtime/helpers/assertThisInitialized"));

var _inherits2 = _interopRequireDefault(require("@babel/runtime/helpers/inherits"));

var _possibleConstructorReturn2 = _interopRequireDefault(require("@babel/runtime/helpers/possibleConstructorReturn"));

var _getPrototypeOf2 = _interopRequireDefault(require("@babel/runtime/helpers/getPrototypeOf"));

var _defineProperty2 = _interopRequireDefault(require("@babel/runtime/helpers/defineProperty"));

var _react = require("react");

var _observer = require("../observer");

function ownKeys(object, enumerableOnly) { var keys = Object.keys(object); if (Object.getOwnPropertySymbols) { var symbols = Object.getOwnPropertySymbols(object); if (enumerableOnly) symbols = symbols.filter(function (sym) { return Object.getOwnPropertyDescriptor(object, sym).enumerable; }); keys.push.apply(keys, symbols); } return keys; }

function _objectSpread(target) { for (var i = 1; i < arguments.length; i++) { var source = arguments[i] != null ? arguments[i] : {}; if (i % 2) { ownKeys(Object(source), true).forEach(function (key) { (0, _defineProperty2.default)(target, key, source[key]); }); } else if (Object.getOwnPropertyDescriptors) { Object.defineProperties(target, Object.getOwnPropertyDescriptors(source)); } else { ownKeys(Object(source)).forEach(function (key) { Object.defineProperty(target, key, Object.getOwnPropertyDescriptor(source, key)); }); } } return target; }

function _createSuper(Derived) { var hasNativeReflectConstruct = _isNativeReflectConstruct(); return function _createSuperInternal() { var Super = (0, _getPrototypeOf2.default)(Derived), result; if (hasNativeReflectConstruct) { var NewTarget = (0, _getPrototypeOf2.default)(this).constructor; result = Reflect.construct(Super, arguments, NewTarget); } else { result = Super.apply(this, arguments); } return (0, _possibleConstructorReturn2.default)(this, result); }; }

function _isNativeReflectConstruct() { if (typeof Reflect === "undefined" || !Reflect.construct) return false; if (Reflect.construct.sham) return false; if (typeof Proxy === "function") return true; try { Date.prototype.toString.call(Reflect.construct(Date, [], function () {})); return true; } catch (e) { return false; } }

var EuiMutationObserver = /*#__PURE__*/function (_EuiObserver) {
  (0, _inherits2.default)(EuiMutationObserver, _EuiObserver);

  var _super = _createSuper(EuiMutationObserver);

  function EuiMutationObserver() {
    var _this;

    (0, _classCallCheck2.default)(this, EuiMutationObserver);

    for (var _len = arguments.length, args = new Array(_len), _key = 0; _key < _len; _key++) {
      args[_key] = arguments[_key];
    }

    _this = _super.call.apply(_super, [this].concat(args));
    (0, _defineProperty2.default)((0, _assertThisInitialized2.default)(_this), "name", 'EuiMutationObserver');
    (0, _defineProperty2.default)((0, _assertThisInitialized2.default)(_this), "onMutation", function (records, observer) {
      _this.props.onMutation(records, observer);
    });
    (0, _defineProperty2.default)((0, _assertThisInitialized2.default)(_this), "beginObserve", function () {
      var childNode = _this.childNode;
      _this.observer = makeMutationObserver(childNode, _this.props.observerOptions, _this.onMutation);
    });
    return _this;
  }

  return EuiMutationObserver;
}(_observer.EuiObserver);

exports.EuiMutationObserver = EuiMutationObserver;

var makeMutationObserver = function makeMutationObserver(node, _observerOptions, callback) {
  // IE11 and the MutationObserver polyfill used in Kibana (for Jest) implement
  // an older spec in which specifying `attributeOldValue` or `attributeFilter`
  // without specifying `attributes` results in a `SyntaxError`.
  // The following logic patches the newer spec in which `attributes: true` can be
  // implied when appropriate (`attributeOldValue` or `attributeFilter` is specified).
  var observerOptions = _objectSpread({}, _observerOptions);

  var needsAttributes = observerOptions.hasOwnProperty('attributeOldValue') || observerOptions.hasOwnProperty('attributeFilter');

  if (needsAttributes && !observerOptions.hasOwnProperty('attributes')) {
    observerOptions.attributes = true;
  }

  var observer = new MutationObserver(callback);
  observer.observe(node, observerOptions);
  return observer;
};

var useMutationObserver = function useMutationObserver(container, callback, observerOptions) {
  (0, _react.useEffect)(function () {
    if (container != null) {
      var observer = makeMutationObserver(container, observerOptions, callback);
      return function () {
        return observer.disconnect();
      };
    }
  }, // ignore changing observerOptions
  // eslint-disable-next-line
  [container, callback]);
};

exports.useMutationObserver = useMutationObserver;