function _typeof(obj) { "@babel/helpers - typeof"; if (typeof Symbol === "function" && typeof Symbol.iterator === "symbol") { _typeof = function _typeof(obj) { return typeof obj; }; } else { _typeof = function _typeof(obj) { return obj && typeof Symbol === "function" && obj.constructor === Symbol && obj !== Symbol.prototype ? "symbol" : typeof obj; }; } return _typeof(obj); }

function ownKeys(object, enumerableOnly) { var keys = Object.keys(object); if (Object.getOwnPropertySymbols) { var symbols = Object.getOwnPropertySymbols(object); if (enumerableOnly) symbols = symbols.filter(function (sym) { return Object.getOwnPropertyDescriptor(object, sym).enumerable; }); keys.push.apply(keys, symbols); } return keys; }

function _objectSpread(target) { for (var i = 1; i < arguments.length; i++) { var source = arguments[i] != null ? arguments[i] : {}; if (i % 2) { ownKeys(Object(source), true).forEach(function (key) { _defineProperty(target, key, source[key]); }); } else if (Object.getOwnPropertyDescriptors) { Object.defineProperties(target, Object.getOwnPropertyDescriptors(source)); } else { ownKeys(Object(source)).forEach(function (key) { Object.defineProperty(target, key, Object.getOwnPropertyDescriptor(source, key)); }); } } return target; }

function _classCallCheck(instance, Constructor) { if (!(instance instanceof Constructor)) { throw new TypeError("Cannot call a class as a function"); } }

function _inherits(subClass, superClass) { if (typeof superClass !== "function" && superClass !== null) { throw new TypeError("Super expression must either be null or a function"); } subClass.prototype = Object.create(superClass && superClass.prototype, { constructor: { value: subClass, writable: true, configurable: true } }); if (superClass) _setPrototypeOf(subClass, superClass); }

function _setPrototypeOf(o, p) { _setPrototypeOf = Object.setPrototypeOf || function _setPrototypeOf(o, p) { o.__proto__ = p; return o; }; return _setPrototypeOf(o, p); }

function _createSuper(Derived) { var hasNativeReflectConstruct = _isNativeReflectConstruct(); return function _createSuperInternal() { var Super = _getPrototypeOf(Derived), result; if (hasNativeReflectConstruct) { var NewTarget = _getPrototypeOf(this).constructor; result = Reflect.construct(Super, arguments, NewTarget); } else { result = Super.apply(this, arguments); } return _possibleConstructorReturn(this, result); }; }

function _possibleConstructorReturn(self, call) { if (call && (_typeof(call) === "object" || typeof call === "function")) { return call; } return _assertThisInitialized(self); }

function _assertThisInitialized(self) { if (self === void 0) { throw new ReferenceError("this hasn't been initialised - super() hasn't been called"); } return self; }

function _isNativeReflectConstruct() { if (typeof Reflect === "undefined" || !Reflect.construct) return false; if (Reflect.construct.sham) return false; if (typeof Proxy === "function") return true; try { Date.prototype.toString.call(Reflect.construct(Date, [], function () {})); return true; } catch (e) { return false; } }

function _getPrototypeOf(o) { _getPrototypeOf = Object.setPrototypeOf ? Object.getPrototypeOf : function _getPrototypeOf(o) { return o.__proto__ || Object.getPrototypeOf(o); }; return _getPrototypeOf(o); }

function _defineProperty(obj, key, value) { if (key in obj) { Object.defineProperty(obj, key, { value: value, enumerable: true, configurable: true, writable: true }); } else { obj[key] = value; } return obj; }

/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */
import { useEffect } from 'react';
import { EuiObserver } from '../observer';
export var EuiMutationObserver = /*#__PURE__*/function (_EuiObserver) {
  _inherits(EuiMutationObserver, _EuiObserver);

  var _super = _createSuper(EuiMutationObserver);

  function EuiMutationObserver() {
    var _this;

    _classCallCheck(this, EuiMutationObserver);

    for (var _len = arguments.length, args = new Array(_len), _key = 0; _key < _len; _key++) {
      args[_key] = arguments[_key];
    }

    _this = _super.call.apply(_super, [this].concat(args));

    _defineProperty(_assertThisInitialized(_this), "name", 'EuiMutationObserver');

    _defineProperty(_assertThisInitialized(_this), "onMutation", function (records, observer) {
      _this.props.onMutation(records, observer);
    });

    _defineProperty(_assertThisInitialized(_this), "beginObserve", function () {
      var childNode = _this.childNode;
      _this.observer = makeMutationObserver(childNode, _this.props.observerOptions, _this.onMutation);
    });

    return _this;
  }

  return EuiMutationObserver;
}(EuiObserver);

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

export var useMutationObserver = function useMutationObserver(container, callback, observerOptions) {
  useEffect(function () {
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