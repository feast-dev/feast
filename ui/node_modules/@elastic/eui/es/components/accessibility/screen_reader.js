function ownKeys(object, enumerableOnly) { var keys = Object.keys(object); if (Object.getOwnPropertySymbols) { var symbols = Object.getOwnPropertySymbols(object); if (enumerableOnly) symbols = symbols.filter(function (sym) { return Object.getOwnPropertyDescriptor(object, sym).enumerable; }); keys.push.apply(keys, symbols); } return keys; }

function _objectSpread(target) { for (var i = 1; i < arguments.length; i++) { var source = arguments[i] != null ? arguments[i] : {}; if (i % 2) { ownKeys(Object(source), true).forEach(function (key) { _defineProperty(target, key, source[key]); }); } else if (Object.getOwnPropertyDescriptors) { Object.defineProperties(target, Object.getOwnPropertyDescriptors(source)); } else { ownKeys(Object(source)).forEach(function (key) { Object.defineProperty(target, key, Object.getOwnPropertyDescriptor(source, key)); }); } } return target; }

function _defineProperty(obj, key, value) { if (key in obj) { Object.defineProperty(obj, key, { value: value, enumerable: true, configurable: true, writable: true }); } else { obj[key] = value; } return obj; }

/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */
import { cloneElement } from 'react';
import PropTypes from "prop-types";
import classNames from 'classnames';
export var EuiScreenReaderOnly = function EuiScreenReaderOnly(_ref) {
  var children = _ref.children,
      showOnFocus = _ref.showOnFocus;
  var classes = classNames({
    euiScreenReaderOnly: !showOnFocus,
    'euiScreenReaderOnly--showOnFocus': showOnFocus
  }, children.props.className);

  var props = _objectSpread(_objectSpread({}, children.props), {}, {
    className: classes
  });

  return /*#__PURE__*/cloneElement(children, props);
};
EuiScreenReaderOnly.propTypes = {
  /**
     * ReactElement to render as this component's content
     */
  children: PropTypes.element.isRequired,

  /**
     * For keyboard navigation, force content to display visually upon focus.
     */
  showOnFocus: PropTypes.bool
};