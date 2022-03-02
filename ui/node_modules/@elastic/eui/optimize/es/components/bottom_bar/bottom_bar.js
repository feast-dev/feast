import _extends from "@babel/runtime/helpers/extends";
import _defineProperty from "@babel/runtime/helpers/defineProperty";
import _slicedToArray from "@babel/runtime/helpers/slicedToArray";
import _objectWithoutProperties from "@babel/runtime/helpers/objectWithoutProperties";

function ownKeys(object, enumerableOnly) { var keys = Object.keys(object); if (Object.getOwnPropertySymbols) { var symbols = Object.getOwnPropertySymbols(object); if (enumerableOnly) symbols = symbols.filter(function (sym) { return Object.getOwnPropertyDescriptor(object, sym).enumerable; }); keys.push.apply(keys, symbols); } return keys; }

function _objectSpread(target) { for (var i = 1; i < arguments.length; i++) { var source = arguments[i] != null ? arguments[i] : {}; if (i % 2) { ownKeys(Object(source), true).forEach(function (key) { _defineProperty(target, key, source[key]); }); } else if (Object.getOwnPropertyDescriptors) { Object.defineProperties(target, Object.getOwnPropertyDescriptors(source)); } else { ownKeys(Object(source)).forEach(function (key) { Object.defineProperty(target, key, Object.getOwnPropertyDescriptor(source, key)); }); } } return target; }

/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */
import classNames from 'classnames';
import React, { forwardRef, useEffect, useState } from 'react';
import { useCombinedRefs } from '../../services';
import { EuiScreenReaderOnly } from '../accessibility';
import { EuiI18n } from '../i18n';
import { useResizeObserver } from '../observer/resize_observer';
import { EuiPortal } from '../portal';
import { jsx as ___EmotionJSX } from "@emotion/react";
// Exported for testing
export var paddingSizeToClassNameMap = {
  none: null,
  s: 'euiBottomBar--paddingSmall',
  m: 'euiBottomBar--paddingMedium',
  l: 'euiBottomBar--paddingLarge'
};
export var POSITIONS = ['static', 'fixed', 'sticky'];
export var EuiBottomBar = /*#__PURE__*/forwardRef(function (_ref, ref) {
  var _ref$position = _ref.position,
      position = _ref$position === void 0 ? 'fixed' : _ref$position,
      _ref$paddingSize = _ref.paddingSize,
      paddingSize = _ref$paddingSize === void 0 ? 'm' : _ref$paddingSize,
      _ref$affordForDisplac = _ref.affordForDisplacement,
      affordForDisplacement = _ref$affordForDisplac === void 0 ? true : _ref$affordForDisplac,
      children = _ref.children,
      className = _ref.className,
      bodyClassName = _ref.bodyClassName,
      landmarkHeading = _ref.landmarkHeading,
      _ref$usePortal = _ref.usePortal,
      usePortal = _ref$usePortal === void 0 ? true : _ref$usePortal,
      _ref$left = _ref.left,
      left = _ref$left === void 0 ? 0 : _ref$left,
      _ref$right = _ref.right,
      right = _ref$right === void 0 ? 0 : _ref$right,
      _ref$bottom = _ref.bottom,
      bottom = _ref$bottom === void 0 ? 0 : _ref$bottom,
      top = _ref.top,
      style = _ref.style,
      rest = _objectWithoutProperties(_ref, ["position", "paddingSize", "affordForDisplacement", "children", "className", "bodyClassName", "landmarkHeading", "usePortal", "left", "right", "bottom", "top", "style"]);

  // Force some props if `fixed` position, but not if the user has supplied these
  affordForDisplacement = position !== 'fixed' ? false : affordForDisplacement;
  usePortal = position !== 'fixed' ? false : usePortal;

  var _useState = useState(null),
      _useState2 = _slicedToArray(_useState, 2),
      resizeRef = _useState2[0],
      setResizeRef = _useState2[1];

  var setRef = useCombinedRefs([setResizeRef, ref]); // TODO: Allow this hooke to be conditional

  var dimensions = useResizeObserver(resizeRef);
  useEffect(function () {
    if (affordForDisplacement && usePortal) {
      document.body.style.paddingBottom = "".concat(dimensions.height, "px");
    }

    if (bodyClassName) {
      document.body.classList.add(bodyClassName);
    }

    return function () {
      if (affordForDisplacement && usePortal) {
        document.body.style.paddingBottom = '';
      }

      if (bodyClassName) {
        document.body.classList.remove(bodyClassName);
      }
    };
  }, [affordForDisplacement, usePortal, dimensions, bodyClassName]);
  var classes = classNames('euiBottomBar', "euiBottomBar--".concat(position), paddingSizeToClassNameMap[paddingSize], className);

  var newStyle = _objectSpread({
    left: left,
    right: right,
    bottom: bottom,
    top: top
  }, style);

  var bar = ___EmotionJSX(React.Fragment, null, ___EmotionJSX(EuiI18n, {
    token: "euiBottomBar.screenReaderHeading",
    default: "Page level controls"
  }, function (screenReaderHeading) {
    return (// Though it would be better to use aria-labelledby than aria-label and not repeat the same string twice
      // A bug in voiceover won't list some landmarks in the rotor without an aria-label
      ___EmotionJSX("section", _extends({
        "aria-label": landmarkHeading ? landmarkHeading : screenReaderHeading,
        className: classes,
        ref: setRef,
        style: newStyle
      }, rest), ___EmotionJSX(EuiScreenReaderOnly, null, ___EmotionJSX("h2", null, landmarkHeading ? landmarkHeading : screenReaderHeading)), children)
    );
  }), ___EmotionJSX(EuiScreenReaderOnly, null, ___EmotionJSX("p", {
    "aria-live": "assertive"
  }, landmarkHeading ? ___EmotionJSX(EuiI18n, {
    token: "euiBottomBar.customScreenReaderAnnouncement",
    default: "There is a new region landmark called {landmarkHeading} with page level controls at the end of the document.",
    values: {
      landmarkHeading: landmarkHeading
    }
  }) : ___EmotionJSX(EuiI18n, {
    token: "euiBottomBar.screenReaderAnnouncement",
    default: "There is a new region landmark with page level controls at the end of the document."
  }))));

  return usePortal ? ___EmotionJSX(EuiPortal, null, bar) : bar;
});
EuiBottomBar.displayName = 'EuiBottomBar';