import _slicedToArray from "@babel/runtime/helpers/slicedToArray";

/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */
import { computed, lineHeightFromBaseline } from '../../services/theme';
import { SCALES } from './_typography';
/**
 * NOTE: These were quick conversions of their Sass counterparts.
 *       They have yet to be used/tested.
 */

export var fontSize = SCALES.reduce(function (acc, elem) {
  acc[elem] = {
    fontSize: computed(function (_ref) {
      var _ref2 = _slicedToArray(_ref, 1),
          scale = _ref2[0];

      return "".concat(scale, "rem");
    }, ["font.scale.".concat(elem)]),
    lineHeight: computed(function (_ref3) {
      var _ref4 = _slicedToArray(_ref3, 2),
          base = _ref4[0],
          font = _ref4[1];

      return lineHeightFromBaseline(base, font, font.scale[elem]);
    }, ['base', 'font'])
  };
  return acc;
}, {});