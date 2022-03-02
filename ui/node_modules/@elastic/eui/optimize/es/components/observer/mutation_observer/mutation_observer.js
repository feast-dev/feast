import _classCallCheck from "@babel/runtime/helpers/classCallCheck";
import _assertThisInitialized from "@babel/runtime/helpers/assertThisInitialized";
import _inherits from "@babel/runtime/helpers/inherits";
import _possibleConstructorReturn from "@babel/runtime/helpers/possibleConstructorReturn";
import _getPrototypeOf from "@babel/runtime/helpers/getPrototypeOf";
import _defineProperty from "@babel/runtime/helpers/defineProperty";

function ownKeys(object, enumerableOnly) { var keys = Object.keys(object); if (Object.getOwnPropertySymbols) { var symbols = Object.getOwnPropertySymbols(object); if (enumerableOnly) symbols = symbols.filter(function (sym) { return Object.getOwnPropertyDescriptor(object, sym).enumerable; }); keys.push.apply(keys, symbols); } return keys; }

function _objectSpread(target) { for (var i = 1; i < arguments.length; i++) { var source = arguments[i] != null ? arguments[i] : {}; if (i % 2) { ownKeys(Object(source), true).forEach(function (key) { _defineProperty(target, key, source[key]); }); } else if (Object.getOwnPropertyDescriptors) { Object.defineProperties(target, Object.getOwnPropertyDescriptors(source)); } else { ownKeys(Object(source)).forEach(function (key) { Object.defineProperty(target, key, Object.getOwnPropertyDescriptor(source, key)); }); } } return target; }

function _createSuper(Derived) { var hasNativeReflectConstruct = _isNativeReflectConstruct(); return function _createSuperInternal() { var Super = _getPrototypeOf(Derived), result; if (hasNativeReflectConstruct) { var NewTarget = _getPrototypeOf(this).constructor; result = Reflect.construct(Super, arguments, NewTarget); } else { result = Super.apply(this, arguments); } return _possibleConstructorReturn(this, result); }; }

function _isNativeReflectConstruct() { if (typeof Reflect === "undefined" || !Reflect.construct) return false; if (Reflect.construct.sham) return false; if (typeof Proxy === "function") return true; try { Date.prototype.toString.call(Reflect.construct(Date, [], function () {})); return true; } catch (e) { return false; } }

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