/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */
import PropTypes from 'prop-types';
var ASC = 'asc';
var DESC = 'desc';
export var SortDirection = Object.freeze({
  ASC: ASC,
  DESC: DESC,
  isAsc: function isAsc(direction) {
    return direction === ASC;
  },
  reverse: function reverse(direction) {
    return this.isAsc(direction) ? DESC : ASC;
  }
});
export var SortDirectionType = PropTypes.oneOf([ASC, DESC]);