/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

/**
 * Most of the style combinations for tokens are semi-arbitrary. However, there was an effort
 * to use the square shape for more common token types like string and number. Reserving the
 * circle shape for more uncommon token types so they grab attention.
 */
export var TOKEN_MAP = {
  tokenClass: {
    shape: 'circle',
    color: 'euiColorVis1'
  },
  tokenProperty: {
    shape: 'circle',
    color: 'euiColorVis2'
  },
  tokenEnum: {
    shape: 'circle',
    color: 'euiColorVis3'
  },
  tokenVariable: {
    shape: 'circle',
    color: 'euiColorVis7'
  },
  tokenMethod: {
    shape: 'square',
    color: 'euiColorVis2'
  },
  tokenAnnotation: {
    shape: 'square',
    color: 'euiColorVis5'
  },
  tokenException: {
    shape: 'circle',
    color: 'euiColorVis0'
  },
  tokenInterface: {
    shape: 'circle',
    color: 'euiColorVis9'
  },
  tokenParameter: {
    shape: 'square',
    color: 'euiColorVis4'
  },
  tokenField: {
    shape: 'circle',
    color: 'euiColorVis0'
  },
  tokenElement: {
    shape: 'square',
    color: 'euiColorVis3'
  },
  tokenFunction: {
    shape: 'circle',
    color: 'euiColorVis2'
  },
  tokenBoolean: {
    shape: 'square',
    color: 'euiColorVis7'
  },
  tokenString: {
    shape: 'square',
    color: 'euiColorVis1'
  },
  tokenArray: {
    shape: 'square',
    color: 'euiColorVis7'
  },
  tokenNumber: {
    shape: 'square',
    color: 'euiColorVis0'
  },
  tokenConstant: {
    shape: 'circle',
    color: 'euiColorVis0'
  },
  tokenObject: {
    shape: 'circle',
    color: 'euiColorVis3'
  },
  tokenEvent: {
    shape: 'circle',
    color: 'euiColorVis4'
  },
  tokenKey: {
    shape: 'circle',
    color: 'euiColorVis5'
  },
  tokenNull: {
    shape: 'square',
    color: 'euiColorVis2'
  },
  tokenStruct: {
    shape: 'square',
    color: 'euiColorVis0'
  },
  tokenPackage: {
    shape: 'square',
    color: 'euiColorVis0'
  },
  tokenOperator: {
    shape: 'circle',
    color: 'euiColorVis4'
  },
  tokenEnumMember: {
    shape: 'square',
    color: 'euiColorVis7'
  },
  tokenRepo: {
    shape: 'rectangle',
    color: 'euiColorVis1',
    fill: 'dark'
  },
  tokenSymbol: {
    shape: 'rectangle',
    color: 'euiColorVis0',
    fill: 'dark'
  },
  tokenFile: {
    shape: 'rectangle',
    color: 'gray',
    fill: 'dark'
  },
  tokenNamespace: {
    shape: 'square',
    color: 'euiColorVis1'
  },
  tokenModule: {
    shape: 'square',
    color: 'euiColorVis4'
  },
  tokenDate: {
    shape: 'square',
    color: 'euiColorVis6'
  },
  tokenGeo: {
    shape: 'square',
    color: 'euiColorVis5'
  },
  tokenIP: {
    shape: 'square',
    color: 'euiColorVis9'
  },
  tokenShape: {
    shape: 'circle',
    color: 'euiColorVis8'
  },
  tokenRange: {
    shape: 'circle',
    color: 'euiColorVis4'
  },
  tokenNested: {
    shape: 'circle',
    color: 'euiColorVis2'
  },
  tokenAlias: {
    shape: 'circle',
    color: 'euiColorVis3'
  },
  tokenBinary: {
    shape: 'square',
    color: 'euiColorVis4'
  },
  tokenJoin: {
    shape: 'square',
    color: 'euiColorVis5'
  },
  tokenPercolator: {
    shape: 'square',
    color: 'euiColorVis6'
  },
  tokenFlattened: {
    shape: 'square',
    color: 'euiColorVis7'
  },
  tokenRankFeature: {
    shape: 'square',
    color: 'euiColorVis8'
  },
  tokenRankFeatures: {
    shape: 'square',
    color: 'euiColorVis3'
  },
  tokenTag: {
    shape: 'square',
    color: 'euiColorVis9'
  },
  tokenKeyword: {
    shape: 'square',
    color: 'euiColorVis1'
  },
  tokenCompletionSuggester: {
    shape: 'square',
    color: 'euiColorVis1'
  },
  tokenDenseVector: {
    shape: 'square',
    color: 'euiColorVis2'
  },
  tokenText: {
    shape: 'square',
    color: 'euiColorVis3'
  },
  tokenTokenCount: {
    shape: 'square',
    color: 'euiColorVis4'
  },
  tokenSearchType: {
    shape: 'square',
    color: 'euiColorVis5'
  },
  tokenHistogram: {
    shape: 'square',
    color: 'euiColorVis6'
  }
};