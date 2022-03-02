import _classCallCheck from "@babel/runtime/helpers/classCallCheck";
import _createClass from "@babel/runtime/helpers/createClass";
import _assertThisInitialized from "@babel/runtime/helpers/assertThisInitialized";
import _inherits from "@babel/runtime/helpers/inherits";
import _possibleConstructorReturn from "@babel/runtime/helpers/possibleConstructorReturn";
import _getPrototypeOf from "@babel/runtime/helpers/getPrototypeOf";
import _defineProperty from "@babel/runtime/helpers/defineProperty";

function _createSuper(Derived) { var hasNativeReflectConstruct = _isNativeReflectConstruct(); return function _createSuperInternal() { var Super = _getPrototypeOf(Derived), result; if (hasNativeReflectConstruct) { var NewTarget = _getPrototypeOf(this).constructor; result = Reflect.construct(Super, arguments, NewTarget); } else { result = Super.apply(this, arguments); } return _possibleConstructorReturn(this, result); }; }

function _isNativeReflectConstruct() { if (typeof Reflect === "undefined" || !Reflect.construct) return false; if (Reflect.construct.sham) return false; if (typeof Proxy === "function") return true; try { Date.prototype.toString.call(Reflect.construct(Date, [], function () {})); return true; } catch (e) { return false; } }

/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */
import { Component } from 'react';
export var EuiObserver = /*#__PURE__*/function (_Component) {
  _inherits(EuiObserver, _Component);

  var _super = _createSuper(EuiObserver);

  function EuiObserver() {
    var _this;

    _classCallCheck(this, EuiObserver);

    for (var _len = arguments.length, args = new Array(_len), _key = 0; _key < _len; _key++) {
      args[_key] = arguments[_key];
    }

    _this = _super.call.apply(_super, [this].concat(args));

    _defineProperty(_assertThisInitialized(_this), "name", 'EuiObserver');

    _defineProperty(_assertThisInitialized(_this), "childNode", null);

    _defineProperty(_assertThisInitialized(_this), "observer", null);

    _defineProperty(_assertThisInitialized(_this), "updateChildNode", function (ref) {
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

    _defineProperty(_assertThisInitialized(_this), "beginObserve", function () {
      throw new Error('EuiObserver has no default observation method');
    });

    return _this;
  }

  _createClass(EuiObserver, [{
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
}(Component);