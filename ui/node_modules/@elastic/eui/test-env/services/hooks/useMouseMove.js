"use strict";

var _interopRequireDefault = require("@babel/runtime/helpers/interopRequireDefault");

Object.defineProperty(exports, "__esModule", {
  value: true
});
exports.isMouseEvent = isMouseEvent;
exports.useMouseMove = useMouseMove;

var _typeof2 = _interopRequireDefault(require("@babel/runtime/helpers/typeof"));

var _react = require("react");

var _throttle = require("../../services/throttle");

/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */
function isMouseEvent(event) {
  return (0, _typeof2.default)(event) === 'object' && 'pageX' in event && 'pageY' in event;
}

function useMouseMove(handleChange) {
  var interactionConditional = arguments.length > 1 && arguments[1] !== undefined ? arguments[1] : true;
  (0, _react.useEffect)(function () {
    return unbindEventListeners;
  }, []); // eslint-disable-line react-hooks/exhaustive-deps

  var handleInteraction = function handleInteraction(e, isFirstInteraction) {
    if (e) {
      if (interactionConditional) {
        var x = isMouseEvent(e) ? e.pageX : e.touches[0].pageX;
        var y = isMouseEvent(e) ? e.pageY : e.touches[0].pageY;
        handleChange({
          x: x,
          y: y
        }, isFirstInteraction);
      }
    }
  };

  var handleMouseMove = (0, _throttle.throttle)(function (e) {
    handleChange({
      x: e.pageX,
      y: e.pageY
    }, false);
  });

  var handleMouseDown = function handleMouseDown(e) {
    handleInteraction(e, true);
    document.addEventListener('mousemove', handleMouseMove);
    document.addEventListener('mouseup', unbindEventListeners);
  };

  var unbindEventListeners = function unbindEventListeners() {
    document.removeEventListener('mousemove', handleMouseMove);
    document.removeEventListener('mouseup', unbindEventListeners);
  };

  return [handleMouseDown, handleInteraction];
}