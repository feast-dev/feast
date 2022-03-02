function _extends() { _extends = Object.assign || function (target) { for (var i = 1; i < arguments.length; i++) { var source = arguments[i]; for (var key in source) { if (Object.prototype.hasOwnProperty.call(source, key)) { target[key] = source[key]; } } } return target; }; return _extends.apply(this, arguments); }

function _slicedToArray(arr, i) { return _arrayWithHoles(arr) || _iterableToArrayLimit(arr, i) || _unsupportedIterableToArray(arr, i) || _nonIterableRest(); }

function _nonIterableRest() { throw new TypeError("Invalid attempt to destructure non-iterable instance.\nIn order to be iterable, non-array objects must have a [Symbol.iterator]() method."); }

function _unsupportedIterableToArray(o, minLen) { if (!o) return; if (typeof o === "string") return _arrayLikeToArray(o, minLen); var n = Object.prototype.toString.call(o).slice(8, -1); if (n === "Object" && o.constructor) n = o.constructor.name; if (n === "Map" || n === "Set") return Array.from(o); if (n === "Arguments" || /^(?:Ui|I)nt(?:8|16|32)(?:Clamped)?Array$/.test(n)) return _arrayLikeToArray(o, minLen); }

function _arrayLikeToArray(arr, len) { if (len == null || len > arr.length) len = arr.length; for (var i = 0, arr2 = new Array(len); i < len; i++) { arr2[i] = arr[i]; } return arr2; }

function _iterableToArrayLimit(arr, i) { if (typeof Symbol === "undefined" || !(Symbol.iterator in Object(arr))) return; var _arr = []; var _n = true; var _d = false; var _e = undefined; try { for (var _i = arr[Symbol.iterator](), _s; !(_n = (_s = _i.next()).done); _n = true) { _arr.push(_s.value); if (i && _arr.length === i) break; } } catch (err) { _d = true; _e = err; } finally { try { if (!_n && _i["return"] != null) _i["return"](); } finally { if (_d) throw _e; } } return _arr; }

function _arrayWithHoles(arr) { if (Array.isArray(arr)) return arr; }

function ownKeys(object, enumerableOnly) { var keys = Object.keys(object); if (Object.getOwnPropertySymbols) { var symbols = Object.getOwnPropertySymbols(object); if (enumerableOnly) symbols = symbols.filter(function (sym) { return Object.getOwnPropertyDescriptor(object, sym).enumerable; }); keys.push.apply(keys, symbols); } return keys; }

function _objectSpread(target) { for (var i = 1; i < arguments.length; i++) { var source = arguments[i] != null ? arguments[i] : {}; if (i % 2) { ownKeys(Object(source), true).forEach(function (key) { _defineProperty(target, key, source[key]); }); } else if (Object.getOwnPropertyDescriptors) { Object.defineProperties(target, Object.getOwnPropertyDescriptors(source)); } else { ownKeys(Object(source)).forEach(function (key) { Object.defineProperty(target, key, Object.getOwnPropertyDescriptor(source, key)); }); } } return target; }

function _defineProperty(obj, key, value) { if (key in obj) { Object.defineProperty(obj, key, { value: value, enumerable: true, configurable: true, writable: true }); } else { obj[key] = value; } return obj; }

function _objectWithoutProperties(source, excluded) { if (source == null) return {}; var target = _objectWithoutPropertiesLoose(source, excluded); var key, i; if (Object.getOwnPropertySymbols) { var sourceSymbolKeys = Object.getOwnPropertySymbols(source); for (i = 0; i < sourceSymbolKeys.length; i++) { key = sourceSymbolKeys[i]; if (excluded.indexOf(key) >= 0) continue; if (!Object.prototype.propertyIsEnumerable.call(source, key)) continue; target[key] = source[key]; } } return target; }

function _objectWithoutPropertiesLoose(source, excluded) { if (source == null) return {}; var target = {}; var sourceKeys = Object.keys(source); var key, i; for (i = 0; i < sourceKeys.length; i++) { key = sourceKeys[i]; if (excluded.indexOf(key) >= 0) continue; target[key] = source[key]; } return target; }

/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */
import React, { useState, useEffect } from 'react';
import PropTypes from "prop-types";
import classNames from 'classnames';
import { keysOf } from '../../common';
import { EuiIcon } from '../../icon';
import { EuiPopover } from '../../popover';
import { EuiI18n } from '../../i18n';
import { EuiHeaderSectionItemButton } from '../header_section';
import { EuiHideFor, EuiShowFor } from '../../responsive';
import { jsx as ___EmotionJSX } from "@emotion/react";
var gutterSizeToClassNameMap = {
  xs: '--gutterXS',
  s: '--gutterS',
  m: '--gutterM',
  l: '--gutterL'
};
export var GUTTER_SIZES = keysOf(gutterSizeToClassNameMap);
export var EuiHeaderLinks = function EuiHeaderLinks(_ref) {
  var children = _ref.children,
      className = _ref.className,
      _ref$gutterSize = _ref.gutterSize,
      gutterSize = _ref$gutterSize === void 0 ? 's' : _ref$gutterSize,
      _ref$popoverBreakpoin = _ref.popoverBreakpoints,
      popoverBreakpoints = _ref$popoverBreakpoin === void 0 ? ['xs', 's'] : _ref$popoverBreakpoin,
      popoverButtonProps = _ref.popoverButtonProps,
      popoverProps = _ref.popoverProps,
      rest = _objectWithoutProperties(_ref, ["children", "className", "gutterSize", "popoverBreakpoints", "popoverButtonProps", "popoverProps"]);

  var _popoverButtonProps = _objectSpread({}, popoverButtonProps),
      _onClick = _popoverButtonProps.onClick,
      _popoverButtonProps$i = _popoverButtonProps.iconType,
      iconType = _popoverButtonProps$i === void 0 ? 'apps' : _popoverButtonProps$i,
      popoverButtonRest = _objectWithoutProperties(_popoverButtonProps, ["onClick", "iconType"]);

  var _useState = useState(false),
      _useState2 = _slicedToArray(_useState, 2),
      mobileMenuIsOpen = _useState2[0],
      setMobileMenuIsOpen = _useState2[1];

  var onMenuButtonClick = function onMenuButtonClick(e) {
    _onClick && _onClick(e);
    setMobileMenuIsOpen(!mobileMenuIsOpen);
  };

  var closeMenu = function closeMenu() {
    setMobileMenuIsOpen(false);
  };

  useEffect(function () {
    window.addEventListener('resize', closeMenu);
    return function () {
      window.removeEventListener('resize', closeMenu);
    };
  });
  var classes = classNames('euiHeaderLinks', className);

  var button = ___EmotionJSX(EuiI18n, {
    token: "euiHeaderLinks.openNavigationMenu",
    default: "Open menu"
  }, function (openNavigationMenu) {
    return ___EmotionJSX(EuiHeaderSectionItemButton, _extends({
      "aria-label": openNavigationMenu,
      onClick: onMenuButtonClick
    }, popoverButtonRest), ___EmotionJSX(EuiIcon, {
      type: iconType,
      size: "m"
    }));
  });

  return ___EmotionJSX(EuiI18n, {
    token: "euiHeaderLinks.appNavigation",
    default: "App menu"
  }, function (appNavigation) {
    return ___EmotionJSX("nav", _extends({
      className: classes,
      "aria-label": appNavigation
    }, rest), ___EmotionJSX(EuiHideFor, {
      sizes: popoverBreakpoints
    }, ___EmotionJSX("div", {
      className: classNames('euiHeaderLinks__list', ["euiHeaderLinks__list".concat(gutterSizeToClassNameMap[gutterSize])])
    }, children)), ___EmotionJSX(EuiShowFor, {
      sizes: popoverBreakpoints
    }, ___EmotionJSX(EuiPopover, _extends({
      button: button,
      isOpen: mobileMenuIsOpen,
      anchorPosition: "downRight",
      closePopover: closeMenu,
      panelPaddingSize: "none"
    }, popoverProps), ___EmotionJSX("div", {
      className: classNames('euiHeaderLinks__mobileList', ["euiHeaderLinks__mobileList".concat(gutterSizeToClassNameMap[gutterSize])])
    }, children))));
  });
};
EuiHeaderLinks.propTypes = {
  className: PropTypes.string,
  "aria-label": PropTypes.string,
  "data-test-subj": PropTypes.string,

  /**
       * Spacing between direct children
       */
  gutterSize: PropTypes.oneOf(["xs", "s", "m", "l"]),

  /**
       * A list of named breakpoints at which to show the popover version
       */
  popoverBreakpoints: PropTypes.oneOfType([PropTypes.arrayOf(PropTypes.oneOf(["xs", "s", "m", "l", "xl"]).isRequired).isRequired, PropTypes.oneOf(["all", "none"])]),

  /**
       * Extend the functionality of the EuiPopover.button which is a EuiHeaderSectionItemButton.
       * With the addition of `iconType` to change the display icon which defaults to `apps`
       */
  popoverButtonProps: PropTypes.shape({
    iconType: PropTypes.oneOfType([PropTypes.oneOf(["accessibility", "addDataApp", "advancedSettingsApp", "agentApp", "aggregate", "alert", "analyzeEvent", "annotation", "apmApp", "apmTrace", "appSearchApp", "apps", "arrowDown", "arrowLeft", "arrowRight", "arrowUp", "arrowStart", "arrowEnd", "asterisk", "auditbeatApp", "beaker", "bell", "bellSlash", "bolt", "boxesHorizontal", "boxesVertical", "branch", "broom", "brush", "bug", "bullseye", "calendar", "canvasApp", "check", "checkInCircleFilled", "cheer", "classificationJob", "clock", "cloudDrizzle", "cloudStormy", "cloudSunny", "codeApp", "color", "compute", "console", "consoleApp", "continuityAbove", "continuityAboveBelow", "continuityBelow", "continuityWithin", "controlsHorizontal", "controlsVertical", "copy", "copyClipboard", "createAdvancedJob", "createMultiMetricJob", "createPopulationJob", "createSingleMetricJob", "cross", "crossClusterReplicationApp", "crossInACircleFilled", "crosshairs", "currency", "cut", "dashboardApp", "dataVisualizer", "database", "devToolsApp", "discoverApp", "document", "documentEdit", "documentation", "documents", "dot", "doubleArrowLeft", "doubleArrowRight", "download", "editorAlignCenter", "editorAlignLeft", "editorAlignRight", "editorBold", "editorCodeBlock", "editorComment", "editorDistributeHorizontal", "editorDistributeVertical", "editorHeading", "editorItalic", "editorItemAlignBottom", "editorItemAlignCenter", "editorItemAlignLeft", "editorItemAlignMiddle", "editorItemAlignRight", "editorItemAlignTop", "editorLink", "editorOrderedList", "editorPositionBottomLeft", "editorPositionBottomRight", "editorPositionTopLeft", "editorPositionTopRight", "editorRedo", "editorStrike", "editorTable", "editorUnderline", "editorUndo", "editorUnorderedList", "email", "empty", "emsApp", "eql", "eraser", "exit", "expand", "expandMini", "exportAction", "eye", "eyeClosed", "faceHappy", "faceNeutral", "faceSad", "filebeatApp", "filter", "flag", "fleetApp", "fold", "folderCheck", "folderClosed", "folderExclamation", "folderOpen", "frameNext", "framePrevious", "fullScreen", "fullScreenExit", "function", "gear", "gisApp", "glasses", "globe", "grab", "grabHorizontal", "graphApp", "grid", "grokApp", "heart", "heartbeatApp", "heatmap", "help", "home", "iInCircle", "image", "importAction", "indexClose", "indexEdit", "indexFlush", "indexManagementApp", "indexMapping", "indexOpen", "indexPatternApp", "indexRollupApp", "indexRuntime", "indexSettings", "inputOutput", "inspect", "invert", "ip", "keyboardShortcut", "kqlField", "kqlFunction", "kqlOperand", "kqlSelector", "kqlValue", "layers", "lensApp", "lettering", "link", "list", "listAdd", "lock", "lockOpen", "logoAWS", "logoAWSMono", "logoAerospike", "logoApache", "logoAppSearch", "logoAzure", "logoAzureMono", "logoBeats", "logoBusinessAnalytics", "logoCeph", "logoCloud", "logoCloudEnterprise", "logoCode", "logoCodesandbox", "logoCouchbase", "logoDocker", "logoDropwizard", "logoElastic", "logoElasticStack", "logoElasticsearch", "logoEnterpriseSearch", "logoEtcd", "logoGCP", "logoGCPMono", "logoGithub", "logoGmail", "logoGolang", "logoGoogleG", "logoHAproxy", "logoIBM", "logoIBMMono", "logoKafka", "logoKibana", "logoKubernetes", "logoLogging", "logoLogstash", "logoMaps", "logoMemcached", "logoMetrics", "logoMongodb", "logoMySQL", "logoNginx", "logoObservability", "logoOsquery", "logoPhp", "logoPostgres", "logoPrometheus", "logoRabbitmq", "logoRedis", "logoSecurity", "logoSiteSearch", "logoSketch", "logoSlack", "logoUptime", "logoWebhook", "logoWindows", "logoWorkplaceSearch", "logsApp", "logstashFilter", "logstashIf", "logstashInput", "logstashOutput", "logstashQueue", "machineLearningApp", "magnet", "magnifyWithExclamation", "magnifyWithMinus", "magnifyWithPlus", "managementApp", "mapMarker", "memory", "menu", "menuDown", "menuLeft", "menuRight", "menuUp", "merge", "metricbeatApp", "metricsApp", "minimize", "minus", "minusInCircle", "minusInCircleFilled", "mobile", "monitoringApp", "moon", "nested", "node", "notebookApp", "number", "offline", "online", "outlierDetectionJob", "package", "packetbeatApp", "pageSelect", "pagesSelect", "paperClip", "partial", "pause", "payment", "pencil", "percent", "pin", "pinFilled", "pipelineApp", "play", "playFilled", "plus", "plusInCircle", "plusInCircleFilled", "popout", "push", "questionInCircle", "quote", "recentlyViewedApp", "refresh", "regressionJob", "reporter", "reportingApp", "returnKey", "save", "savedObjectsApp", "scale", "search", "searchProfilerApp", "securityAnalyticsApp", "securityApp", "securitySignal", "securitySignalDetected", "securitySignalResolved", "shard", "share", "snowflake", "sortDown", "sortLeft", "sortRight", "sortUp", "sortable", "spacesApp", "sqlApp", "starEmpty", "starEmptySpace", "starFilled", "starFilledSpace", "starMinusEmpty", "starMinusFilled", "starPlusEmpty", "starPlusFilled", "stats", "stop", "stopFilled", "stopSlash", "storage", "string", "submodule", "sun", "swatchInput", "symlink", "tableDensityCompact", "tableDensityExpanded", "tableDensityNormal", "tableOfContents", "tag", "tear", "temperature", "timeline", "timelionApp", "timeRefresh", "timeslider", "training", "trash", "unfold", "unlink", "upgradeAssistantApp", "uptimeApp", "user", "users", "usersRolesApp", "vector", "videoPlayer", "visArea", "visAreaStacked", "visBarHorizontal", "visBarHorizontalStacked", "visBarVertical", "visBarVerticalStacked", "visGauge", "visGoal", "visLine", "visMapCoordinate", "visMapRegion", "visMetric", "visPie", "visTable", "visTagCloud", "visText", "visTimelion", "visVega", "visVisualBuilder", "visualizeApp", "watchesApp", "wordWrap", "wordWrapDisabled", "workplaceSearchApp", "wrench", "tokenClass", "tokenProperty", "tokenEnum", "tokenVariable", "tokenMethod", "tokenAnnotation", "tokenException", "tokenInterface", "tokenParameter", "tokenField", "tokenElement", "tokenFunction", "tokenBoolean", "tokenString", "tokenArray", "tokenNumber", "tokenConstant", "tokenObject", "tokenEvent", "tokenKey", "tokenNull", "tokenStruct", "tokenPackage", "tokenOperator", "tokenEnumMember", "tokenRepo", "tokenSymbol", "tokenFile", "tokenModule", "tokenNamespace", "tokenDate", "tokenIP", "tokenNested", "tokenAlias", "tokenShape", "tokenGeo", "tokenRange", "tokenBinary", "tokenJoin", "tokenPercolator", "tokenFlattened", "tokenRankFeature", "tokenRankFeatures", "tokenKeyword", "tokenTag", "tokenCompletionSuggester", "tokenDenseVector", "tokenText", "tokenTokenCount", "tokenSearchType", "tokenHistogram"]).isRequired, PropTypes.string.isRequired, PropTypes.elementType.isRequired])
  }),

  /**
       * Extend the functionality of the EuiPopover
       */
  popoverProps: PropTypes.shape({
    /**
       * Class name passed to the direct parent of the button
       */
    anchorClassName: PropTypes.string,

    /**
       * Alignment of the popover and arrow relative to the button
       */
    anchorPosition: PropTypes.any,

    /**
       * Style and position alteration for arrow-less, left-aligned
       * attachment. Intended for use with inputs as anchors, e.g.
       * EuiInputPopover
       */
    attachToAnchor: PropTypes.bool,
    buttonRef: PropTypes.any,

    /**
       * Restrict the popover's position within this element
       */
    container: PropTypes.any,

    /**
       * CSS display type for both the popover and anchor
       */
    display: PropTypes.oneOf(["inlineBlock", "block"]),

    /**
       * Object of props passed to EuiFocusTrap
       */
    focusTrapProps: PropTypes.any,

    /**
       * Show arrow indicating to originating button
       */
    hasArrow: PropTypes.bool,

    /**
       * Specifies what element should initially have focus; Can be a DOM
       * node, or a selector string (which will be passed to
       * document.querySelector() to find the DOM node), or a function that
       * returns a DOM node
       * Set to `false` to prevent initial auto-focus. Use only
       * when your app handles setting initial focus state.
       */
    initialFocus: PropTypes.oneOfType([PropTypes.oneOfType([PropTypes.any.isRequired, PropTypes.string.isRequired, PropTypes.func.isRequired]).isRequired, PropTypes.oneOf([false])]),

    /**
       * Passed directly to EuiPortal for DOM positioning. Both properties are
       * required if prop is specified
       */
    insert: PropTypes.shape({
      sibling: PropTypes.any.isRequired,
      position: PropTypes.oneOf(["before", "after"]).isRequired
    }),

    /**
       * Visibility state of the popover
       */
    isOpen: PropTypes.bool,

    /**
       * Traps tab focus within the popover contents
       */
    ownFocus: PropTypes.bool,

    /**
       * Custom class added to the EuiPanel containing the popover contents
       */
    panelClassName: PropTypes.string,

    /**
       * EuiPanel padding on all sides
       */
    panelPaddingSize: PropTypes.any,

    /**
       * Standard DOM `style` attribute. Passed to the EuiPanel
       */
    panelStyle: PropTypes.any,

    /**
       * Object of props passed to EuiPanel
       */
    panelProps: PropTypes.any,
    panelRef: PropTypes.any,
    popoverRef: PropTypes.any,

    /**
       * When `true`, the popover's position is re-calculated when the user
       * scrolls, this supports having fixed-position popover anchors. When nesting
       * an `EuiPopover` in a scrollable container, `repositionOnScroll` should be `true`
       */
    repositionOnScroll: PropTypes.bool,

    /**
       * By default, popover content inherits the z-index of the anchor
       * component; pass `zIndex` to override
       */
    zIndex: PropTypes.number,

    /**
       * Function callback for when the focus trap is deactivated
       */
    onTrapDeactivation: PropTypes.any,

    /**
       * Distance away from the anchor that the popover will render
       */
    offset: PropTypes.number,

    /**
       * Minimum distance between the popover and the bounding container;
       * Pass an array of 4 values to adjust each side differently: `[top, right, bottom, left]`
       * Default is 16
       */
    buffer: PropTypes.oneOfType([PropTypes.number.isRequired, PropTypes.any.isRequired]),

    /**
       * Element to pass as the child element of the arrow;
       * Use case is typically limited to an accompanying `EuiBeacon`
       */
    arrowChildren: PropTypes.node,

    /**
       * Provide a name to the popover panel
       */
    "aria-label": PropTypes.string,

    /**
       * Alternative option to `aria-label` that takes an `id`.
       * Usually takes the `id` of the popover title
       */
    "aria-labelledby": PropTypes.string
  })
};