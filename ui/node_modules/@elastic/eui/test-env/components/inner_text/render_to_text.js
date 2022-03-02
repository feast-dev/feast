"use strict";

var _interopRequireWildcard = require("@babel/runtime/helpers/interopRequireWildcard");

var _interopRequireDefault = require("@babel/runtime/helpers/interopRequireDefault");

Object.defineProperty(exports, "__esModule", {
  value: true
});
exports.useRenderToText = useRenderToText;

var _slicedToArray2 = _interopRequireDefault(require("@babel/runtime/helpers/slicedToArray"));

var _react = _interopRequireWildcard(require("react"));

var _reactDom = require("react-dom");

var _inner_text = require("./inner_text");

var _react2 = require("@emotion/react");

/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */
function useRenderToText(node) {
  var placeholder = arguments.length > 1 && arguments[1] !== undefined ? arguments[1] : '';

  var _useInnerText = (0, _inner_text.useInnerText)(placeholder),
      _useInnerText2 = (0, _slicedToArray2.default)(_useInnerText, 2),
      ref = _useInnerText2[0],
      text = _useInnerText2[1];

  var hostNode = (0, _react.useRef)(null);

  var onUnmount = function onUnmount() {
    if (hostNode.current) {
      (0, _reactDom.unmountComponentAtNode)(hostNode.current);
      hostNode.current = null;
    }
  };

  var setRef = (0, _react.useCallback)(function (node) {
    if (hostNode.current) {
      ref(node);
    }
  }, [ref]);
  (0, _react.useEffect)(function () {
    hostNode.current = document.createDocumentFragment();
    (0, _reactDom.render)((0, _react2.jsx)("div", {
      ref: setRef
    }, node), hostNode.current);
    return function () {
      onUnmount();
    };
  }, [node, setRef]);
  return text || placeholder;
}