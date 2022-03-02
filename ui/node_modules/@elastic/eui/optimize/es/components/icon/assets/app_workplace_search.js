import _extends from "@babel/runtime/helpers/extends";
import _objectWithoutProperties from "@babel/runtime/helpers/objectWithoutProperties";

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

var EuiIconAppWorkplaceSearch = function EuiIconAppWorkplaceSearch(_ref) {
  var title = _ref.title,
      titleId = _ref.titleId,
      props = _objectWithoutProperties(_ref, ["title", "titleId"]);

  return ___EmotionJSX("svg", _extends({
    xmlns: "http://www.w3.org/2000/svg",
    width: 32,
    height: 32,
    viewBox: "0 0 32 32",
    "aria-labelledby": titleId
  }, props), title ? ___EmotionJSX("title", {
    id: titleId
  }, title) : null, ___EmotionJSX("path", {
    fillRule: "evenodd",
    d: "M7.81 1H2v30h5.815c1.705 0 3.343-.783 4.38-2.094l3.182-4.023.006.006 2.607-3.137.01-.014a9.42 9.42 0 001.953-4.826 9.112 9.112 0 00-1.891-6.522l-.01-.012-2.603-3.156L12.197 3.1C11.16 1.786 9.52 1 7.81 1zm6.118 7.523l-3.31-4.195C9.968 3.512 8.917 3 7.81 3H4v26h3.815c1.1 0 2.15-.508 2.8-1.32l3.33-4.21-1.796-1.77-.027-.033c-2.84-3.317-2.83-8.195.028-11.287l.004-.004a98.893 98.893 0 011.774-1.853zm4.043 8.099c-.12 1.279-.616 2.537-1.49 3.64l-1.288 1.63-1.578-1.556c-2.19-2.59-2.118-6.3.001-8.596.6-.64 1.119-1.183 1.561-1.635l1.293 1.638v.002l.014.016c.942 1.192 1.446 2.564 1.51 3.949a7.09 7.09 0 01-.023.912z"
  }), ___EmotionJSX("path", {
    fillRule: "evenodd",
    className: "euiIcon__fillSecondary",
    d: "M23.801 3c-.382 0-1.052.231-1.925.812-.636.422-1.228.928-1.677 1.357l1.771 2.245.005.007.024.03.008.01c4.003 5.099 3.99 12.057-.035 17.145l-1.793 2.267a8.206 8.206 0 001.769 1.498c.698.427 1.337.629 1.852.629H28V3h-4.199zm-6.148 23.842l.765 1.053.023.027c.717.851 1.566 1.607 2.464 2.156.89.544 1.89.922 2.895.922H30V1h-6.199c-.987 0-2.066.505-3.032 1.146a14.934 14.934 0 00-2.352 1.974l-.276.288-.595.628 2.855 3.619.006.006c3.459 4.375 3.457 10.329-.004 14.704l-2.75 3.477z"
  }));
};

export var icon = EuiIconAppWorkplaceSearch;