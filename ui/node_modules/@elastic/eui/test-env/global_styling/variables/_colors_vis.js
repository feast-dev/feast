"use strict";

Object.defineProperty(exports, "__esModule", {
  value: true
});
exports.colorVis = void 0;

/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

/**
 * NOTE: These were quick conversions of their Sass counterparts.
 *       They have yet to be used/tested.
 */
// Maps allow for easier JSON usage
// Use map_merge(euiColorVisColors, $yourMap) to change individual colors after importing ths file
// The `behindText` variant is a direct copy of the hex output by the JS euiPaletteColorBlindBehindText() function
var euiPaletteColorBlind = {
  euiColorVis0: {
    graphic: '#54B399',
    behindText: '#6DCCB1'
  },
  euiColorVis1: {
    graphic: '#6092C0',
    behindText: '#79AAD9'
  },
  euiColorVis2: {
    graphic: '#D36086',
    behindText: '#EE789D'
  },
  euiColorVis3: {
    graphic: '#9170B8',
    behindText: '#A987D1'
  },
  euiColorVis4: {
    graphic: '#CA8EAE',
    behindText: '#E4A6C7'
  },
  euiColorVis5: {
    graphic: '#D6BF57',
    behindText: '#F1D86F'
  },
  euiColorVis6: {
    graphic: '#B9A888',
    behindText: '#D2C0A0'
  },
  euiColorVis7: {
    graphic: '#DA8B45',
    behindText: '#F5A35C'
  },
  euiColorVis8: {
    graphic: '#AA6556',
    behindText: '#C47C6C'
  },
  euiColorVis9: {
    graphic: '#E7664C',
    behindText: '#FF7E62'
  }
};
var colorVis = {
  euiColorVis0: euiPaletteColorBlind.euiColorVis0.graphic,
  euiColorVis1: euiPaletteColorBlind.euiColorVis1.graphic,
  euiColorVis2: euiPaletteColorBlind.euiColorVis2.graphic,
  euiColorVis3: euiPaletteColorBlind.euiColorVis3.graphic,
  euiColorVis4: euiPaletteColorBlind.euiColorVis4.graphic,
  euiColorVis5: euiPaletteColorBlind.euiColorVis5.graphic,
  euiColorVis6: euiPaletteColorBlind.euiColorVis6.graphic,
  euiColorVis7: euiPaletteColorBlind.euiColorVis7.graphic,
  euiColorVis8: euiPaletteColorBlind.euiColorVis8.graphic,
  euiColorVis9: euiPaletteColorBlind.euiColorVis9.graphic,
  euiColorVis0_behindText: euiPaletteColorBlind.euiColorVis0.behindText,
  euiColorVis1_behindText: euiPaletteColorBlind.euiColorVis1.behindText,
  euiColorVis2_behindText: euiPaletteColorBlind.euiColorVis2.behindText,
  euiColorVis3_behindText: euiPaletteColorBlind.euiColorVis3.behindText,
  euiColorVis4_behindText: euiPaletteColorBlind.euiColorVis4.behindText,
  euiColorVis5_behindText: euiPaletteColorBlind.euiColorVis5.behindText,
  euiColorVis6_behindText: euiPaletteColorBlind.euiColorVis6.behindText,
  euiColorVis7_behindText: euiPaletteColorBlind.euiColorVis7.behindText,
  euiColorVis8_behindText: euiPaletteColorBlind.euiColorVis8.behindText,
  euiColorVis9_behindText: euiPaletteColorBlind.euiColorVis9.behindText
};
exports.colorVis = colorVis;