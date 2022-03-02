/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */
import PropTypes from 'prop-types';
import { SortDirectionType } from './sort_direction';
export var PropertySortType = PropTypes.shape({
  field: PropTypes.string.isRequired,
  direction: SortDirectionType.isRequired
});