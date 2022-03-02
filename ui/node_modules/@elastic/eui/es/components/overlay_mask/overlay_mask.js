function _slicedToArray(arr, i) { return _arrayWithHoles(arr) || _iterableToArrayLimit(arr, i) || _unsupportedIterableToArray(arr, i) || _nonIterableRest(); }

function _nonIterableRest() { throw new TypeError("Invalid attempt to destructure non-iterable instance.\nIn order to be iterable, non-array objects must have a [Symbol.iterator]() method."); }

function _unsupportedIterableToArray(o, minLen) { if (!o) return; if (typeof o === "string") return _arrayLikeToArray(o, minLen); var n = Object.prototype.toString.call(o).slice(8, -1); if (n === "Object" && o.constructor) n = o.constructor.name; if (n === "Map" || n === "Set") return Array.from(o); if (n === "Arguments" || /^(?:Ui|I)nt(?:8|16|32)(?:Clamped)?Array$/.test(n)) return _arrayLikeToArray(o, minLen); }

function _arrayLikeToArray(arr, len) { if (len == null || len > arr.length) len = arr.length; for (var i = 0, arr2 = new Array(len); i < len; i++) { arr2[i] = arr[i]; } return arr2; }

function _iterableToArrayLimit(arr, i) { if (typeof Symbol === "undefined" || !(Symbol.iterator in Object(arr))) return; var _arr = []; var _n = true; var _d = false; var _e = undefined; try { for (var _i = arr[Symbol.iterator](), _s; !(_n = (_s = _i.next()).done); _n = true) { _arr.push(_s.value); if (i && _arr.length === i) break; } } catch (err) { _d = true; _e = err; } finally { try { if (!_n && _i["return"] != null) _i["return"](); } finally { if (_d) throw _e; } } return _arr; }

function _arrayWithHoles(arr) { if (Array.isArray(arr)) return arr; }

function _objectWithoutProperties(source, excluded) { if (source == null) return {}; var target = _objectWithoutPropertiesLoose(source, excluded); var key, i; if (Object.getOwnPropertySymbols) { var sourceSymbolKeys = Object.getOwnPropertySymbols(source); for (i = 0; i < sourceSymbolKeys.length; i++) { key = sourceSymbolKeys[i]; if (excluded.indexOf(key) >= 0) continue; if (!Object.prototype.propertyIsEnumerable.call(source, key)) continue; target[key] = source[key]; } } return target; }

function _objectWithoutPropertiesLoose(source, excluded) { if (source == null) return {}; var target = {}; var sourceKeys = Object.keys(source); var key, i; for (i = 0; i < sourceKeys.length; i++) { key = sourceKeys[i]; if (excluded.indexOf(key) >= 0) continue; target[key] = source[key]; } return target; }

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
import PropTypes from "prop-types";
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
EuiOverlayMask.propTypes = {
  className: PropTypes.string,
  "aria-label": PropTypes.string,
  "data-test-subj": PropTypes.string,

  /**
     * Function that applies to clicking the mask itself and not the children
     */
  onClick: PropTypes.func,

  /**
     * ReactNode to render as this component's content
     */
  children: PropTypes.node,

  /**
     * Should the mask visually sit above or below the EuiHeader (controlled by z-index)
     */
  headerZindexLocation: PropTypes.oneOf(["above", "below"])
};