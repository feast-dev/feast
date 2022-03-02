import _classCallCheck from "@babel/runtime/helpers/classCallCheck";
import _createClass from "@babel/runtime/helpers/createClass";
import _inherits from "@babel/runtime/helpers/inherits";
import _possibleConstructorReturn from "@babel/runtime/helpers/possibleConstructorReturn";
import _getPrototypeOf from "@babel/runtime/helpers/getPrototypeOf";

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
export var EuiWindowEvent = /*#__PURE__*/function (_Component) {
  _inherits(EuiWindowEvent, _Component);

  var _super = _createSuper(EuiWindowEvent);

  function EuiWindowEvent() {
    _classCallCheck(this, EuiWindowEvent);

    return _super.apply(this, arguments);
  }

  _createClass(EuiWindowEvent, [{
    key: "componentDidMount",
    value: function componentDidMount() {
      this.addEvent(this.props);
    }
  }, {
    key: "componentDidUpdate",
    value: function componentDidUpdate(prevProps) {
      if (prevProps.event !== this.props.event || prevProps.handler !== this.props.handler) {
        this.removeEvent(prevProps);
        this.addEvent(this.props);
      }
    }
  }, {
    key: "componentWillUnmount",
    value: function componentWillUnmount() {
      this.removeEvent(this.props);
    }
  }, {
    key: "addEvent",
    value: function addEvent(_ref) {
      var event = _ref.event,
          handler = _ref.handler;
      window.addEventListener(event, handler);
    }
  }, {
    key: "removeEvent",
    value: function removeEvent(_ref2) {
      var event = _ref2.event,
          handler = _ref2.handler;
      window.removeEventListener(event, handler);
    }
  }, {
    key: "render",
    value: function render() {
      return null;
    }
  }]);

  return EuiWindowEvent;
}(Component);