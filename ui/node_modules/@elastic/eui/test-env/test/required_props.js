"use strict";

Object.defineProperty(exports, "__esModule", {
  value: true
});
exports.requiredProps = void 0;

/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */
// We expect all React components to be able to support these props,
// which will be rendered as HTML attributes.
var requiredProps = {
  'aria-label': 'aria-label',
  className: 'testClass1 testClass2',
  'data-test-subj': 'test subject string'
};
exports.requiredProps = requiredProps;