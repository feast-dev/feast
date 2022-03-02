/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */
import { euiPaletteColorBlind } from '../../services/color/eui_palettes';
import { DEFAULT_VISUALIZATION_COLOR } from '../../services/color/visualization_colors'; // @ts-ignore typescript doesn't understand the webpack loader

import lightColors from '!!sass-vars-to-js-loader!../../themes/amsterdam/_colors_light.scss'; // @ts-ignore typescript doesn't understand the webpack loader

import darkColors from '!!sass-vars-to-js-loader!../../themes/amsterdam/_colors_dark.scss';
var fontFamily = "'Inter', 'Inter UI', -apple-system, BlinkMacSystemFont,\n  'Segoe UI', Helvetica, Arial, sans-serif, 'Apple Color Emoji', 'Segoe UI Emoji', 'Segoe UI Symbol'";

function createTheme(colors) {
  return {
    lineAnnotation: {
      line: {
        strokeWidth: 1,
        stroke: colors.euiColorDarkShade.rgba,
        opacity: 1
      },
      details: {
        fontSize: 10,
        fontFamily: fontFamily,
        fill: colors.euiTextColor.rgba,
        padding: 0
      }
    },
    partition: {
      fontFamily: fontFamily,
      minFontSize: 8,
      maxFontSize: 16,
      fillLabel: {
        valueFont: {
          fontWeight: 700
        }
      },
      linkLabel: {
        maxCount: 5,
        fontSize: 11,
        textColor: colors.euiTextColor.rgba
      },
      outerSizeRatio: 1,
      circlePadding: 4,
      sectorLineStroke: colors.euiColorEmptyShade.rgba,
      sectorLineWidth: 1.5
    },
    theme: {
      background: {
        color: colors.euiColorEmptyShade.rgba
      },
      chartMargins: {
        left: 0,
        right: 0,
        top: 0,
        bottom: 0
      },
      lineSeriesStyle: {
        line: {
          strokeWidth: 2
        },
        point: {
          fill: colors.euiColorEmptyShade.rgba,
          strokeWidth: 2,
          radius: 3
        }
      },
      areaSeriesStyle: {
        area: {
          opacity: 0.3
        },
        line: {
          strokeWidth: 2
        },
        point: {
          visible: false,
          fill: colors.euiColorEmptyShade.rgba,
          strokeWidth: 2,
          radius: 3
        }
      },
      barSeriesStyle: {
        displayValue: {
          fontSize: 10,
          fontFamily: fontFamily,
          fill: {
            textBorder: 0
          },
          alignment: {
            horizontal: 'center',
            vertical: 'middle'
          }
        }
      },
      scales: {
        barsPadding: 0.25,
        histogramPadding: 0.05
      },
      axes: {
        axisTitle: {
          fontSize: 12,
          fontFamily: fontFamily,
          fill: colors.euiTextColor.rgba,
          padding: {
            inner: 10,
            outer: 0
          }
        },
        axisLine: {
          stroke: colors.euiColorChartLines.rgba
        },
        tickLabel: {
          fontSize: 10,
          fontFamily: fontFamily,
          fill: colors.euiTextSubduedColor.rgba,
          padding: {
            outer: 8,
            inner: 10
          }
        },
        tickLine: {
          visible: false,
          stroke: colors.euiColorChartLines.rgba,
          strokeWidth: 1
        },
        gridLine: {
          horizontal: {
            visible: true,
            stroke: colors.euiColorChartLines.rgba,
            strokeWidth: 1,
            opacity: 1,
            dash: [0, 0]
          },
          vertical: {
            visible: true,
            stroke: colors.euiColorChartLines.rgba,
            strokeWidth: 1,
            opacity: 1,
            dash: [4, 4]
          }
        }
      },
      colors: {
        vizColors: euiPaletteColorBlind({
          sortBy: 'natural'
        }),
        defaultVizColor: DEFAULT_VISUALIZATION_COLOR
      },
      crosshair: {
        band: {
          fill: colors.euiColorChartBand.rgba
        },
        line: {
          stroke: colors.euiColorDarkShade.rgba,
          strokeWidth: 1,
          dash: [4, 4]
        },
        crossLine: {
          stroke: colors.euiColorDarkShade.rgba,
          strokeWidth: 1,
          dash: [4, 4]
        }
      },
      goal: {
        tickLabel: {
          fontFamily: fontFamily,
          fill: colors.euiTextSubduedColor.rgba
        },
        majorLabel: {
          fontFamily: fontFamily,
          fill: colors.euiTextColor.rgba
        },
        minorLabel: {
          fontFamily: fontFamily,
          fill: colors.euiTextSubduedColor.rgba
        },
        majorCenterLabel: {
          fontFamily: fontFamily,
          fill: colors.euiTextColor.rgba
        },
        minorCenterLabel: {
          fontFamily: fontFamily,
          fill: colors.euiTextSubduedColor.rgba
        },
        targetLine: {
          stroke: colors.euiColorDarkestShade.rgba
        },
        tickLine: {
          stroke: colors.euiColorMediumShade.rgba
        },
        progressLine: {
          stroke: colors.euiColorDarkestShade.rgba
        }
      },
      partition: {
        fontFamily: fontFamily,
        minFontSize: 8,
        maxFontSize: 16,
        fillLabel: {
          valueFont: {
            fontWeight: 700
          }
        },
        linkLabel: {
          maxCount: 5,
          fontSize: 11,
          textColor: colors.euiTextColor.rgba
        },
        outerSizeRatio: 1,
        circlePadding: 4,
        sectorLineStroke: colors.euiColorEmptyShade.rgba,
        sectorLineWidth: 1.5
      }
    }
  };
}

export var EUI_CHARTS_THEME_LIGHT = createTheme(lightColors);
export var EUI_CHARTS_THEME_DARK = createTheme(darkColors);
export var EUI_SPARKLINE_THEME_PARTIAL = {
  lineSeriesStyle: {
    point: {
      visible: false,
      strokeWidth: 1,
      radius: 1
    }
  },
  areaSeriesStyle: {
    point: {
      visible: false,
      strokeWidth: 1,
      radius: 1
    }
  }
};