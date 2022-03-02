function _extends() { _extends = Object.assign || function (target) { for (var i = 1; i < arguments.length; i++) { var source = arguments[i]; for (var key in source) { if (Object.prototype.hasOwnProperty.call(source, key)) { target[key] = source[key]; } } } return target; }; return _extends.apply(this, arguments); }

function _objectWithoutProperties(source, excluded) { if (source == null) return {}; var target = _objectWithoutPropertiesLoose(source, excluded); var key, i; if (Object.getOwnPropertySymbols) { var sourceSymbolKeys = Object.getOwnPropertySymbols(source); for (i = 0; i < sourceSymbolKeys.length; i++) { key = sourceSymbolKeys[i]; if (excluded.indexOf(key) >= 0) continue; if (!Object.prototype.propertyIsEnumerable.call(source, key)) continue; target[key] = source[key]; } } return target; }

function _objectWithoutPropertiesLoose(source, excluded) { if (source == null) return {}; var target = {}; var sourceKeys = Object.keys(source); var key, i; for (i = 0; i < sourceKeys.length; i++) { key = sourceKeys[i]; if (excluded.indexOf(key) >= 0) continue; target[key] = source[key]; } return target; }

/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */
// THIS IS A GENERATED FILE. DO NOT MODIFY MANUALLY. @see scripts/compile-icons.js
import * as React from 'react';
import { jsx as ___EmotionJSX } from "@emotion/react";

var EuiIconVisTimelion = function EuiIconVisTimelion(_ref) {
  var title = _ref.title,
      titleId = _ref.titleId,
      props = _objectWithoutProperties(_ref, ["title", "titleId"]);

  return ___EmotionJSX("svg", _extends({
    xmlns: "http://www.w3.org/2000/svg",
    width: 16,
    height: 16,
    viewBox: "0 0 16 16",
    "aria-labelledby": titleId
  }, props), title ? ___EmotionJSX("title", {
    id: titleId
  }, title) : null, ___EmotionJSX("path", {
    d: "M10.884 1.802c1.171-.309 2.22-.24 3.103.07.295.104.507.212.631.292l.164.106.048.188c.278 1.066.2 2.175-.091 3.23a6.728 6.728 0 01-.194.598c.035.12.074.273.112.454.227 1.065.247 2.193-.08 3.267a4.855 4.855 0 01-.534 1.175c-.324.514-.7.954-1.117 1.323-.806.712-1.616 1.071-2.235 1.206-.14.179-.346.381-.629.578-.643.447-1.493.711-2.561.711-1.07 0-1.92-.264-2.563-.711a2.874 2.874 0 01-.629-.578c-.618-.134-1.428-.493-2.232-1.203-.419-.37-.796-.81-1.12-1.326a4.855 4.855 0 01-.534-1.175C.096 8.933.116 7.805.343 6.74a6.74 6.74 0 01.112-.454 6.728 6.728 0 01-.194-.598C-.03 4.633-.108 3.524.17 2.458l.048-.188.164-.106c.124-.08.336-.188.63-.291.885-.31 1.933-.38 3.104-.07.184-.108.434-.23.748-.348C5.616 1.17 6.497 1 7.501 1c1.002 0 1.883.17 2.634.455.315.119.565.24.75.347zm3.027 1.116a3.13 3.13 0 00-.255-.102c-.749-.263-1.66-.309-2.708.007l-.238.072-.201-.147c-.11-.08-.354-.217-.728-.358C9.14 2.147 8.38 2 7.501 2c-.88 0-1.641.147-2.283.39-.373.141-.617.278-.727.358l-.2.147-.24-.072c-1.048-.316-1.958-.27-2.707-.007a3.13 3.13 0 00-.255.102c-.167.812-.094 1.674.136 2.503.084.303.166.524.217.637l.082.182-.066.19c-.032.09-.084.27-.137.518-.195.914-.212 1.88.059 2.767.1.333.241.645.423.935.273.434.588.801.936 1.108.434.383.898.655 1.36.834.27.105.474.155.575.17l.248.034.12.22c.051.091.199.266.466.452.476.33 1.127.532 1.993.532.865 0 1.516-.202 1.99-.532.268-.186.416-.36.466-.452l.121-.22.248-.034c.101-.015.305-.065.576-.17.462-.18.927-.452 1.362-.836.347-.307.66-.674.933-1.107.182-.29.322-.601.423-.934.271-.887.254-1.853.06-2.767a4.597 4.597 0 00-.138-.519l-.066-.189.082-.182c.05-.113.133-.334.217-.637.23-.829.303-1.691.136-2.503zM5 5.008c.004.181-.048.364-.264.616-.428.498-.985.488-1.431.049-.326-.321-.325-.732-.29-1.047.085-.76.748-.722 1.262-.44.486.27.717.616.723.823zm5 0c.006-.206.237-.553.724-.821.514-.283 1.176-.321 1.26.44.036.314.038.725-.288 1.046-.446.44-1.005.449-1.433-.05-.217-.25-.267-.434-.263-.614zM7.5 8c-.503 0-.976-.24-1.375-.672-.398-.432-.592-1.126 0-1.528.384-.259 1.082-.3 1.375-.3.294 0 .991.041 1.375.3.593.402.397 1.096 0 1.528C8.477 7.76 8.002 8 7.5 8zm0 1c.808 0 2.656.994 2.49 2.074-.153.993-2.114.925-2.49.925-.374 0-2.336.068-2.49-.925C4.845 9.994 6.693 9 7.5 9z"
  }));
};

export var icon = EuiIconVisTimelion;