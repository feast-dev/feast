import _slicedToArray from "@babel/runtime/helpers/slicedToArray";
import _objectWithoutProperties from "@babel/runtime/helpers/objectWithoutProperties";

/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

/**
 * NOTE: We can't test this component because Enzyme doesn't support rendering
 * into portals.
 */
import React, { useEffect, useRef, useState } from 'react';
import { createPortal } from 'react-dom';
import classNames from 'classnames';
import { keysOf } from '../common';
import { jsx as ___EmotionJSX } from "@emotion/react";
export var EuiOverlayMask = function EuiOverlayMask(_ref) {
  var className = _ref.className,
      children = _ref.children,
      onClick = _ref.onClick,
      _ref$headerZindexLoca = _ref.headerZindexLocation,
      headerZindexLocation = _ref$headerZindexLoca === void 0 ? 'above' : _ref$headerZindexLoca,
      rest = _objectWithoutProperties(_ref, ["className", "children", "onClick", "headerZindexLocation"]);

  var overlayMaskNode = useRef();

  var _useState = useState(false),
      _useState2 = _slicedToArray(_useState, 2),
      isPortalTargetReady = _useState2[0],
      setIsPortalTargetReady = _useState2[1];

  useEffect(function () {
    document.body.classList.add('euiBody-hasOverlayMask');
    return function () {
      document.body.classList.remove('euiBody-hasOverlayMask');
    };
  }, []);
  useEffect(function () {
    if (typeof document !== 'undefined') {
      overlayMaskNode.current = document.createElement('div');
    }
  }, []);
  useEffect(function () {
    var portalTarget = overlayMaskNode.current;

    if (portalTarget) {
      document.body.appendChild(portalTarget);
    }

    setIsPortalTargetReady(true);
    return function () {
      if (portalTarget) {
        document.body.removeChild(portalTarget);
      }
    };
  }, []);
  useEffect(function () {
    if (!overlayMaskNode.current) return;
    keysOf(rest).forEach(function (key) {
      if (typeof rest[key] !== 'string') {
        throw new Error("Unhandled property type. EuiOverlayMask property ".concat(key, " is not a string."));
      }

      if (overlayMaskNode.current) {
        overlayMaskNode.current.setAttribute(key, rest[key]);
      }
    });
  }, []); // eslint-disable-line react-hooks/exhaustive-deps

  useEffect(function () {
    if (!overlayMaskNode.current) return;
    overlayMaskNode.current.className = classNames('euiOverlayMask', "euiOverlayMask--".concat(headerZindexLocation, "Header"), className);
  }, [className, headerZindexLocation]);
  useEffect(function () {
    if (!overlayMaskNode.current || !onClick) return;
    var portalTarget = overlayMaskNode.current;

    var listener = function listener(e) {
      if (e.target === overlayMaskNode.current) {
        onClick();
      }
    };

    overlayMaskNode.current.addEventListener('click', listener);
    return function () {
      if (portalTarget && onClick) {
        portalTarget.removeEventListener('click', listener);
      }
    };
  }, [onClick]);
  return isPortalTargetReady ? ___EmotionJSX(React.Fragment, null, /*#__PURE__*/createPortal(children, overlayMaskNode.current)) : null;
};