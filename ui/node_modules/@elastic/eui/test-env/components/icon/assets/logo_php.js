"use strict";

var _interopRequireWildcard = require("@babel/runtime/helpers/interopRequireWildcard");

var _interopRequireDefault = require("@babel/runtime/helpers/interopRequireDefault");

Object.defineProperty(exports, "__esModule", {
  value: true
});
exports.icon = void 0;

var _extends2 = _interopRequireDefault(require("@babel/runtime/helpers/extends"));

var _objectWithoutProperties2 = _interopRequireDefault(require("@babel/runtime/helpers/objectWithoutProperties"));

var React = _interopRequireWildcard(require("react"));

var _services = require("../../../services");

var _react2 = require("@emotion/react");

/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */
// THIS IS A GENERATED FILE. DO NOT MODIFY MANUALLY. @see scripts/compile-icons.js
var EuiIconLogoPhp = function EuiIconLogoPhp(_ref) {
  var title = _ref.title,
      titleId = _ref.titleId,
      props = (0, _objectWithoutProperties2.default)(_ref, ["title", "titleId"]);
  var generateId = (0, _services.htmlIdGenerator)('logo_php');
  return (0, _react2.jsx)("svg", (0, _extends2.default)({
    xmlns: "http://www.w3.org/2000/svg",
    xmlnsXlink: "http://www.w3.org/1999/xlink",
    width: 32,
    height: 32,
    viewBox: "0 0 32 32",
    "aria-labelledby": titleId
  }, props), title ? (0, _react2.jsx)("title", {
    id: titleId
  }, title) : null, (0, _react2.jsx)("defs", null, (0, _react2.jsx)("path", {
    id: generateId('a'),
    d: "M.028 7.854C.028 3.534 6.688.03 14.902.03s14.874 3.503 14.874 7.824c0 4.322-6.66 7.825-14.874 7.825-8.215 0-14.874-3.503-14.874-7.825"
  }), (0, _react2.jsx)("path", {
    id: generateId('d'),
    d: "M0 17.43h30.987V0H0z"
  }), (0, _react2.jsx)("path", {
    id: generateId('f'),
    d: "M0 17.43h30.987V0H0z"
  }), (0, _react2.jsx)("radialGradient", {
    id: generateId('b'),
    cx: "30.02%",
    cy: "82.422%",
    r: "124.814%",
    fx: "30.02%",
    fy: "82.422%",
    gradientTransform: "matrix(.52604 0 0 1 .142 0)"
  }, (0, _react2.jsx)("stop", {
    offset: "0%",
    stopColor: "#AEB2D5"
  }), (0, _react2.jsx)("stop", {
    offset: "30%",
    stopColor: "#AEB2D5"
  }), (0, _react2.jsx)("stop", {
    offset: "75%",
    stopColor: "#484C89"
  }), (0, _react2.jsx)("stop", {
    offset: "100%",
    stopColor: "#484C89"
  }))), (0, _react2.jsx)("g", {
    fill: "none",
    fillRule: "evenodd",
    transform: "matrix(1 0 0 -1 .026 25.413)"
  }, (0, _react2.jsx)("g", {
    transform: "translate(.592 .86)"
  }, (0, _react2.jsx)("mask", {
    id: generateId('c'),
    fill: "#fff"
  }, (0, _react2.jsx)("use", {
    xlinkHref: "#".concat(generateId('a'))
  })), (0, _react2.jsx)("g", {
    fill: "url(#".concat(generateId('b'), ")"),
    fillRule: "nonzero",
    mask: "url(#".concat(generateId('c'), ")")
  }, (0, _react2.jsx)("path", {
    d: "M.028 7.854C.028 3.534 6.688.03 14.902.03s14.874 3.503 14.874 7.824c0 4.322-6.66 7.825-14.874 7.825-8.215 0-14.874-3.503-14.874-7.825"
  }))), (0, _react2.jsx)("mask", {
    id: generateId('e'),
    fill: "#fff"
  }, (0, _react2.jsx)("use", {
    xlinkHref: "#".concat(generateId('d'))
  })), (0, _react2.jsx)("g", {
    mask: "url(#".concat(generateId('e'), ")")
  }, (0, _react2.jsx)("path", {
    fill: "#777BB3",
    fillRule: "nonzero",
    d: "M15.494 1.472c7.894 0 14.293 3.243 14.293 7.243s-6.4 7.244-14.293 7.244c-7.894 0-14.293-3.243-14.293-7.244 0-4 6.4-7.243 14.293-7.243"
  })), (0, _react2.jsx)("mask", {
    id: generateId('g'),
    fill: "#fff"
  }, (0, _react2.jsx)("use", {
    xlinkHref: "#".concat(generateId('f'))
  })), (0, _react2.jsx)("g", {
    fillRule: "nonzero",
    mask: "url(#".concat(generateId('g'), ")")
  }, (0, _react2.jsx)("path", {
    fill: "#000",
    d: "M8.701 7.817c.65 0 1.134.12 1.44.356.304.233.513.638.623 1.202.103.528.064.896-.116 1.095-.183.203-.58.306-1.177.306H8.433l-.575-2.959h.843zM5.31 4.172a.161.161 0 00-.159.193l1.524 7.84c.015.076.082.131.159.131h3.284c1.033 0 1.801-.28 2.284-.833.485-.556.635-1.333.446-2.309a3.686 3.686 0 00-.395-1.098 3.676 3.676 0 00-.727-.91 3.189 3.189 0 00-1.197-.718c-.43-.14-.983-.21-1.643-.21h-1.33l-.38-1.955a.161.161 0 00-.158-.13H5.309z"
  }), (0, _react2.jsx)("path", {
    fill: "#FFF",
    d: "M8.566 10.615h.905c.722 0 .972-.158 1.058-.253.141-.157.168-.487.077-.956-.102-.524-.291-.896-.563-1.105-.277-.214-.729-.322-1.341-.322h-.648l.512 2.636zm1.551 1.883H6.833a.323.323 0 01-.317-.261L4.992 4.396a.323.323 0 01.317-.385h1.708c.155 0 .287.11.317.262l.354 1.823h1.197c.677 0 1.247.074 1.694.22.46.149.882.402 1.256.752.31.284.564.604.758.95.193.345.331.731.412 1.146.2 1.027.038 1.85-.482 2.446-.515.59-1.325.888-2.406.888zM7.663 7.656h1.039c.688 0 1.201.13 1.538.39.337.259.565.692.683 1.299.113.583.061.994-.155 1.233-.216.24-.648.36-1.297.36H8.3l-.638-3.282zm2.454 4.52c.989 0 1.71-.26 2.162-.779.453-.518.59-1.242.41-2.172a3.51 3.51 0 00-.378-1.05 3.502 3.502 0 00-.695-.87 3.017 3.017 0 00-1.137-.683c-.417-.135-.948-.203-1.594-.203H7.422l-.405-2.085H5.309l1.524 7.841h3.284z"
  }), (0, _react2.jsx)("path", {
    fill: "#000",
    d: "M16.763 6.257a.161.161 0 00-.159.192l.674 3.47c.064.33.049.566-.044.666-.057.061-.228.164-.732.164H15.28l-.847-4.361a.161.161 0 00-.159-.131H12.58a.161.161 0 00-.159.192l1.524 7.841c.015.076.082.13.159.13h1.694a.161.161 0 00.159-.191l-.368-1.893h1.314c1 0 1.68-.176 2.075-.54.403-.37.529-.962.374-1.76l-.71-3.648a.161.161 0 00-.158-.131h-1.721z"
  }), (0, _react2.jsx)("path", {
    fill: "#FFF",
    d: "M15.799 14.582h-1.695a.323.323 0 01-.317-.261L12.263 6.48a.323.323 0 01.317-.385h1.695c.154 0 .287.11.317.262l.822 4.23h1.088c.504 0 .61-.107.614-.112.03-.033.07-.185.004-.526l-.674-3.47a.323.323 0 01.317-.384h1.721c.155 0 .288.11.317.262l.71 3.648c.166.857.023 1.5-.424 1.91-.426.392-1.14.582-2.184.582h-1.118l.33 1.7a.323.323 0 01-.316.385m0-.323l-.406-2.084h1.51c.95 0 1.605-.166 1.966-.497.36-.332.468-.869.324-1.611l-.709-3.649h-1.721l.674 3.47c.077.394.048.663-.085.807-.133.143-.416.215-.85.215h-1.354l-.873-4.492H12.58l1.524 7.841h1.695"
  }), (0, _react2.jsx)("path", {
    fill: "#000",
    d: "M22.033 7.817c.65 0 1.134.12 1.44.356.304.233.513.638.623 1.202.103.528.064.896-.116 1.095-.183.203-.58.306-1.177.306h-1.038l-.575-2.959h.843zm-3.392-3.645a.161.161 0 00-.159.193l1.524 7.84c.015.076.082.131.159.131h3.284c1.033 0 1.801-.28 2.284-.833.485-.556.635-1.333.446-2.309a3.689 3.689 0 00-.395-1.098 3.676 3.676 0 00-.727-.91 3.189 3.189 0 00-1.197-.718c-.43-.14-.983-.21-1.643-.21h-1.33l-.38-1.955a.161.161 0 00-.158-.13H18.64z"
  }), (0, _react2.jsx)("path", {
    fill: "#FFF",
    d: "M21.898 10.615h.905c.722 0 .972-.158 1.058-.253.141-.157.168-.487.077-.956-.102-.524-.292-.896-.563-1.105-.277-.214-.729-.322-1.341-.322h-.648l.512 2.636zm1.551 1.883h-3.284a.323.323 0 01-.317-.261l-1.524-7.841a.323.323 0 01.317-.385h1.708c.155 0 .287.11.317.262l.354 1.823h1.197c.677 0 1.247.074 1.694.22.46.149.882.402 1.256.752.31.284.564.604.758.95.193.345.331.731.412 1.146.2 1.027.038 1.85-.482 2.446-.515.59-1.325.888-2.406.888zm-2.454-4.842h1.039c.688 0 1.201.13 1.538.39.337.259.565.692.683 1.299.113.583.061.994-.155 1.233-.216.24-.648.36-1.297.36h-1.17l-.638-3.282zm2.454 4.52c.989 0 1.71-.26 2.162-.779.454-.518.59-1.242.41-2.172a3.51 3.51 0 00-.378-1.05 3.502 3.502 0 00-.695-.87 3.017 3.017 0 00-1.137-.683c-.417-.135-.948-.203-1.594-.203h-1.463l-.405-2.085h-1.708l1.524 7.841h3.284z"
  }))));
};

var icon = EuiIconLogoPhp;
exports.icon = icon;