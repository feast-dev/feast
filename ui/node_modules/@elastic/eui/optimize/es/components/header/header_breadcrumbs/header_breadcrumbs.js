import _extends from "@babel/runtime/helpers/extends";
import _objectWithoutProperties from "@babel/runtime/helpers/objectWithoutProperties";

/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */
import React from 'react';
import classNames from 'classnames';
import { EuiBreadcrumbs } from '../../breadcrumbs';
import { jsx as ___EmotionJSX } from "@emotion/react";
export var EuiHeaderBreadcrumbs = function EuiHeaderBreadcrumbs(_ref) {
  var className = _ref.className,
      breadcrumbs = _ref.breadcrumbs,
      rest = _objectWithoutProperties(_ref, ["className", "breadcrumbs"]);

  var classes = classNames('euiHeaderBreadcrumbs', className);
  return ___EmotionJSX(EuiBreadcrumbs, _extends({
    max: 4,
    truncate: true,
    breadcrumbs: breadcrumbs,
    className: classes
  }, rest));
};