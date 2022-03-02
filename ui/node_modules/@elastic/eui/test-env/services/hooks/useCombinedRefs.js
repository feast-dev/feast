"use strict";

Object.defineProperty(exports, "__esModule", {
  value: true
});
exports.useCombinedRefs = void 0;

var _react = require("react");

/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

/*
 * For use when a component needs to set `ref` objects from multiple sources.
 * For instance, if a component accepts a `ref` prop but also needs its own
 * local reference for calculations, etc.
 * This hook handles setting multiple `ref`s of any available `ref` type
 * in a single callback function.
 */
var useCombinedRefs = function useCombinedRefs(refs) {
  return (0, _react.useCallback)(function (node) {
    return refs.forEach(function (ref) {
      if (!ref) return;

      if (typeof ref === 'function') {
        ref(node);
      } else {
        ref.current = node;
      }
    });
  }, [refs]);
};

exports.useCombinedRefs = useCombinedRefs;