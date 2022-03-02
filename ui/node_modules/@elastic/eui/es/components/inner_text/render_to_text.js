function _slicedToArray(arr, i) { return _arrayWithHoles(arr) || _iterableToArrayLimit(arr, i) || _unsupportedIterableToArray(arr, i) || _nonIterableRest(); }

function _nonIterableRest() { throw new TypeError("Invalid attempt to destructure non-iterable instance.\nIn order to be iterable, non-array objects must have a [Symbol.iterator]() method."); }

function _unsupportedIterableToArray(o, minLen) { if (!o) return; if (typeof o === "string") return _arrayLikeToArray(o, minLen); var n = Object.prototype.toString.call(o).slice(8, -1); if (n === "Object" && o.constructor) n = o.constructor.name; if (n === "Map" || n === "Set") return Array.from(o); if (n === "Arguments" || /^(?:Ui|I)nt(?:8|16|32)(?:Clamped)?Array$/.test(n)) return _arrayLikeToArray(o, minLen); }

function _arrayLikeToArray(arr, len) { if (len == null || len > arr.length) len = arr.length; for (var i = 0, arr2 = new Array(len); i < len; i++) { arr2[i] = arr[i]; } return arr2; }

function _iterableToArrayLimit(arr, i) { if (typeof Symbol === "undefined" || !(Symbol.iterator in Object(arr))) return; var _arr = []; var _n = true; var _d = false; var _e = undefined; try { for (var _i = arr[Symbol.iterator](), _s; !(_n = (_s = _i.next()).done); _n = true) { _arr.push(_s.value); if (i && _arr.length === i) break; } } catch (err) { _d = true; _e = err; } finally { try { if (!_n && _i["return"] != null) _i["return"](); } finally { if (_d) throw _e; } } return _arr; }

function _arrayWithHoles(arr) { if (Array.isArray(arr)) return arr; }

/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */
import React, { useCallback, useEffect, useRef } from 'react';
import { render, unmountComponentAtNode } from 'react-dom';
import { useInnerText } from './inner_text';
import { jsx as ___EmotionJSX } from "@emotion/react";
export function useRenderToText(node) {
  var placeholder = arguments.length > 1 && arguments[1] !== undefined ? arguments[1] : '';

  var _useInnerText = useInnerText(placeholder),
      _useInnerText2 = _slicedToArray(_useInnerText, 2),
      ref = _useInnerText2[0],
      text = _useInnerText2[1];

  var hostNode = useRef(null);

  var onUnmount = function onUnmount() {
    if (hostNode.current) {
      unmountComponentAtNode(hostNode.current);
      hostNode.current = null;
    }
  };

  var setRef = useCallback(function (node) {
    if (hostNode.current) {
      ref(node);
    }
  }, [ref]);
  useEffect(function () {
    hostNode.current = document.createDocumentFragment();
    render(___EmotionJSX("div", {
      ref: setRef
    }, node), hostNode.current);
    return function () {
      onUnmount();
    };
  }, [node, setRef]);
  return text || placeholder;
}