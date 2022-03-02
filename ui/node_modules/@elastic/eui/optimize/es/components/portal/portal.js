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

/**
 * NOTE: We can't test this component because Enzyme doesn't support rendering
 * into portals.
 */
import { Component } from 'react';
import { createPortal } from 'react-dom';
import { keysOf } from '../common';
export var insertPositions = {
  after: 'afterend',
  before: 'beforebegin'
};
export var INSERT_POSITIONS = keysOf(insertPositions);
export var EuiPortal = /*#__PURE__*/function (_Component) {
  _inherits(EuiPortal, _Component);

  var _super = _createSuper(EuiPortal);

  function EuiPortal(props) {
    var _this;

    _classCallCheck(this, EuiPortal);

    _this = _super.call(this, props);

    _defineProperty(_assertThisInitialized(_this), "portalNode", void 0);

    var insert = _this.props.insert;
    _this.portalNode = document.createElement('div');

    if (insert == null) {
      // no insertion defined, append to body
      document.body.appendChild(_this.portalNode);
    } else {
      // inserting before or after an element
      var sibling = insert.sibling,
          position = insert.position;
      sibling.insertAdjacentElement(insertPositions[position], _this.portalNode);
    }

    return _this;
  }

  _createClass(EuiPortal, [{
    key: "componentDidMount",
    value: function componentDidMount() {
      this.updatePortalRef(this.portalNode);
    }
  }, {
    key: "componentWillUnmount",
    value: function componentWillUnmount() {
      if (this.portalNode.parentNode) {
        this.portalNode.parentNode.removeChild(this.portalNode);
      }

      this.updatePortalRef(null);
    }
  }, {
    key: "updatePortalRef",
    value: function updatePortalRef(ref) {
      if (this.props.portalRef) {
        this.props.portalRef(ref);
      }
    }
  }, {
    key: "render",
    value: function render() {
      return /*#__PURE__*/createPortal(this.props.children, this.portalNode);
    }
  }]);

  return EuiPortal;
}(Component);