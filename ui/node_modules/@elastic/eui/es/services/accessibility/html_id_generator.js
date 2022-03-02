/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */
import { v1 as uuidv1 } from 'uuid';
import { useMemo } from 'react';
/**
 * This function returns a function to generate ids.
 * This can be used to generate unique, but predictable ids to pair labels
 * with their inputs. It takes an optional prefix as a parameter. If you don't
 * specify it, it generates a random id prefix. If you specify a custom prefix
 * it should begin with an letter to be HTML4 compliant.
 */

export function htmlIdGenerator() {
  var idPrefix = arguments.length > 0 && arguments[0] !== undefined ? arguments[0] : '';
  var staticUuid = uuidv1();
  return function () {
    var idSuffix = arguments.length > 0 && arguments[0] !== undefined ? arguments[0] : '';
    var prefix = "".concat(idPrefix).concat(idPrefix !== '' ? '_' : 'i');
    var suffix = idSuffix ? "_".concat(idSuffix) : '';
    return "".concat(prefix).concat(suffix ? staticUuid : uuidv1()).concat(suffix);
  };
}
/**
 * Generates a memoized ID that remains static until component unmount.
 * This prevents IDs from being re-randomized on every component update.
 */

export var useGeneratedHtmlId = function useGeneratedHtmlId() {
  var _ref = arguments.length > 0 && arguments[0] !== undefined ? arguments[0] : {},
      prefix = _ref.prefix,
      suffix = _ref.suffix,
      conditionalId = _ref.conditionalId;

  return useMemo(function () {
    return conditionalId || htmlIdGenerator(prefix)(suffix);
  }, [conditionalId, prefix, suffix]);
};