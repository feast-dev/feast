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

var EuiIconCheer = function EuiIconCheer(_ref) {
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
    d: "M4.934 3.063a1.5 1.5 0 01.547.321l.112.115 6.07 6.915a1.5 1.5 0 01-.646 2.41l-.142.04-9.031 2.097A1.5 1.5 0 01.037 13.19l.043-.159L3.04 4.02a1.5 1.5 0 011.893-.957zM4.027 4.25l-.036.083-2.961 9.011a.5.5 0 00.498.656l.09-.013 2.937-.681-1.399-1.508a.5.5 0 01.666-.74l.067.06 1.788 1.927 2.634-.611-3.198-3.601a.5.5 0 01.682-.726l.066.062 3.559 4.007 1.229-.284a.5.5 0 00.15-.063l.067-.049a.5.5 0 00.099-.632l-.053-.073-6.07-6.916a.5.5 0 00-.138-.11l-.082-.035-.088-.02a.5.5 0 00-.507.256zm11.66 5.039a2.5 2.5 0 01-.975 3.399.5.5 0 01-.485-.875 1.5 1.5 0 00-1.454-2.624.5.5 0 01-.485-.875 2.5 2.5 0 013.399.975zm-5.03-6.206a.5.5 0 01.338.544l-.02.088-.677 2.035 2.068-.721a.5.5 0 01.6.225l.036.082a.5.5 0 01-.225.6l-.082.037L9.67 7.028a.5.5 0 01-.659-.55l.02-.08.995-3a.5.5 0 01.632-.316zM14.5 4a.5.5 0 110 1 .5.5 0 010-1zM7.862.403a2.5 2.5 0 01.735 3.459.5.5 0 01-.839-.545 1.5 1.5 0 10-2.516-1.634.5.5 0 01-.839-.545A2.5 2.5 0 017.862.403zM13.5 2a.5.5 0 110 1 .5.5 0 010-1zm-3-1a.5.5 0 110 1 .5.5 0 010-1zm4-1a.5.5 0 110 1 .5.5 0 010-1z"
  }));
};

export var icon = EuiIconCheer;