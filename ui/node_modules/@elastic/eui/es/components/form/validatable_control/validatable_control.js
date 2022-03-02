/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */
import { Children, cloneElement, useRef, useEffect, useCallback } from 'react';
import PropTypes from "prop-types";

function isMutableRef(ref) {
  return ref != null && ref.hasOwnProperty('current');
}

export var EuiValidatableControl = function EuiValidatableControl(_ref) {
  var isInvalid = _ref.isInvalid,
      children = _ref.children;
  var control = useRef(null);
  var child = Children.only(children);
  var childRef = child.ref;
  var replacedRef = useCallback(function (element) {
    control.current = element; // Call the original ref, if any

    if (typeof childRef === 'function') {
      childRef(element);
    } else if (isMutableRef(childRef)) {
      childRef.current = element;
    }
  }, [childRef]);
  useEffect(function () {
    if (control.current === null || typeof control.current.setCustomValidity !== 'function') {
      return; // jsdom doesn't polyfill this for the server-side
    }

    if (isInvalid) {
      control.current.setCustomValidity('Invalid');
    } else {
      control.current.setCustomValidity('');
    }
  });
  return /*#__PURE__*/cloneElement(child, {
    ref: replacedRef
  });
};
EuiValidatableControl.propTypes = {
  className: PropTypes.string,
  "aria-label": PropTypes.string,
  "data-test-subj": PropTypes.string,
  isInvalid: PropTypes.bool,

  /**
     * ReactNode to render as this component's content
     */
  children: PropTypes.shape({
    ref: PropTypes.any
  }).isRequired
};