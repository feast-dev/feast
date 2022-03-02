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
import classnames from 'classnames';
import { jsx as ___EmotionJSX } from "@emotion/react";
export var EuiModalHeaderTitle = function EuiModalHeaderTitle(_ref) {
  var className = _ref.className,
      children = _ref.children,
      rest = _objectWithoutProperties(_ref, ["className", "children"]);

  var classes = classnames('euiModalHeader__title', className);
  return ___EmotionJSX("div", _extends({
    className: classes
  }, rest), /*#__PURE__*/React.isValidElement(children) ? children : ___EmotionJSX("h1", null, children));
};