import _slicedToArray from "@babel/runtime/helpers/slicedToArray";

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