function _extends() { _extends = Object.assign || function (target) { for (var i = 1; i < arguments.length; i++) { var source = arguments[i]; for (var key in source) { if (Object.prototype.hasOwnProperty.call(source, key)) { target[key] = source[key]; } } } return target; }; return _extends.apply(this, arguments); }

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
import React from 'react';
import PropTypes from "prop-types";
import classNames from 'classnames';
import { keysOf } from '../../common';
import { EuiPageHeaderContent } from './page_header_content';
import { setPropsForRestrictedPageWidth } from '../_restrict_width';
import { jsx as ___EmotionJSX } from "@emotion/react";
var paddingSizeToClassNameMap = {
  none: null,
  s: 'euiPageHeader--paddingSmall',
  m: 'euiPageHeader--paddingMedium',
  l: 'euiPageHeader--paddingLarge'
};
export var PADDING_SIZES = keysOf(paddingSizeToClassNameMap);
export var EuiPageHeader = function EuiPageHeader(_ref) {
  var _classNames;

  var className = _ref.className,
      _ref$restrictWidth = _ref.restrictWidth,
      restrictWidth = _ref$restrictWidth === void 0 ? false : _ref$restrictWidth,
      _ref$paddingSize = _ref.paddingSize,
      paddingSize = _ref$paddingSize === void 0 ? 'none' : _ref$paddingSize,
      bottomBorder = _ref.bottomBorder,
      style = _ref.style,
      alignItems = _ref.alignItems,
      _ref$responsive = _ref.responsive,
      responsive = _ref$responsive === void 0 ? true : _ref$responsive,
      children = _ref.children,
      pageTitle = _ref.pageTitle,
      pageTitleProps = _ref.pageTitleProps,
      iconType = _ref.iconType,
      iconProps = _ref.iconProps,
      tabs = _ref.tabs,
      tabsProps = _ref.tabsProps,
      description = _ref.description,
      rightSideItems = _ref.rightSideItems,
      rightSideGroupProps = _ref.rightSideGroupProps,
      rest = _objectWithoutProperties(_ref, ["className", "restrictWidth", "paddingSize", "bottomBorder", "style", "alignItems", "responsive", "children", "pageTitle", "pageTitleProps", "iconType", "iconProps", "tabs", "tabsProps", "description", "rightSideItems", "rightSideGroupProps"]);

  var _setPropsForRestricte = setPropsForRestrictedPageWidth(restrictWidth, style),
      widthClassName = _setPropsForRestricte.widthClassName,
      newStyle = _setPropsForRestricte.newStyle;

  var classes = classNames('euiPageHeader', paddingSizeToClassNameMap[paddingSize], (_classNames = {}, _defineProperty(_classNames, "euiPageHeader--".concat(widthClassName), widthClassName), _defineProperty(_classNames, 'euiPageHeader--bottomBorder', bottomBorder), _defineProperty(_classNames, 'euiPageHeader--responsive', responsive === true), _defineProperty(_classNames, 'euiPageHeader--responsiveReverse', responsive === 'reverse'), _defineProperty(_classNames, 'euiPageHeader--tabsAtBottom', pageTitle && tabs), _defineProperty(_classNames, 'euiPageHeader--onlyTabs', tabs && !pageTitle && !rightSideItems && !description && !children), _classNames), "euiPageHeader--".concat(alignItems !== null && alignItems !== void 0 ? alignItems : 'center'), className);

  if (!pageTitle && !tabs && !description && !rightSideItems) {
    return ___EmotionJSX("header", _extends({
      className: classes,
      style: newStyle || style
    }, rest), children);
  }

  return ___EmotionJSX("header", _extends({
    className: classes,
    style: newStyle || style
  }, rest), ___EmotionJSX(EuiPageHeaderContent, {
    alignItems: alignItems,
    responsive: responsive,
    pageTitle: pageTitle,
    pageTitleProps: pageTitleProps,
    iconType: iconType,
    iconProps: iconProps,
    tabs: tabs,
    tabsProps: tabsProps,
    description: description,
    rightSideItems: rightSideItems,
    rightSideGroupProps: rightSideGroupProps
  }, children));
};
EuiPageHeader.propTypes = {
  className: PropTypes.string,
  "aria-label": PropTypes.string,
  "data-test-subj": PropTypes.string,

  /**
     * Wrapped in an `H1` so choose appropriately.
     * A simple string is best
     */
  pageTitle: PropTypes.node,

  /**
     * Additional props to pass to the EuiTitle
     */
  pageTitleProps: PropTypes.shape({
    className: PropTypes.string,
    "aria-label": PropTypes.string,
    "data-test-subj": PropTypes.string,
    textTransform: PropTypes.oneOf(["uppercase"]),
    id: PropTypes.string
  }),

  /**
     * Optional icon to place to the left of the title
     */
  iconType: PropTypes.oneOfType([PropTypes.oneOf(["accessibility", "addDataApp", "advancedSettingsApp", "agentApp", "aggregate", "alert", "analyzeEvent", "annotation", "apmApp", "apmTrace", "appSearchApp", "apps", "arrowDown", "arrowLeft", "arrowRight", "arrowUp", "arrowStart", "arrowEnd", "asterisk", "auditbeatApp", "beaker", "bell", "bellSlash", "bolt", "boxesHorizontal", "boxesVertical", "branch", "broom", "brush", "bug", "bullseye", "calendar", "canvasApp", "check", "checkInCircleFilled", "cheer", "classificationJob", "clock", "cloudDrizzle", "cloudStormy", "cloudSunny", "codeApp", "color", "compute", "console", "consoleApp", "continuityAbove", "continuityAboveBelow", "continuityBelow", "continuityWithin", "controlsHorizontal", "controlsVertical", "copy", "copyClipboard", "createAdvancedJob", "createMultiMetricJob", "createPopulationJob", "createSingleMetricJob", "cross", "crossClusterReplicationApp", "crossInACircleFilled", "crosshairs", "currency", "cut", "dashboardApp", "dataVisualizer", "database", "devToolsApp", "discoverApp", "document", "documentEdit", "documentation", "documents", "dot", "doubleArrowLeft", "doubleArrowRight", "download", "editorAlignCenter", "editorAlignLeft", "editorAlignRight", "editorBold", "editorCodeBlock", "editorComment", "editorDistributeHorizontal", "editorDistributeVertical", "editorHeading", "editorItalic", "editorItemAlignBottom", "editorItemAlignCenter", "editorItemAlignLeft", "editorItemAlignMiddle", "editorItemAlignRight", "editorItemAlignTop", "editorLink", "editorOrderedList", "editorPositionBottomLeft", "editorPositionBottomRight", "editorPositionTopLeft", "editorPositionTopRight", "editorRedo", "editorStrike", "editorTable", "editorUnderline", "editorUndo", "editorUnorderedList", "email", "empty", "emsApp", "eql", "eraser", "exit", "expand", "expandMini", "exportAction", "eye", "eyeClosed", "faceHappy", "faceNeutral", "faceSad", "filebeatApp", "filter", "flag", "fleetApp", "fold", "folderCheck", "folderClosed", "folderExclamation", "folderOpen", "frameNext", "framePrevious", "fullScreen", "fullScreenExit", "function", "gear", "gisApp", "glasses", "globe", "grab", "grabHorizontal", "graphApp", "grid", "grokApp", "heart", "heartbeatApp", "heatmap", "help", "home", "iInCircle", "image", "importAction", "indexClose", "indexEdit", "indexFlush", "indexManagementApp", "indexMapping", "indexOpen", "indexPatternApp", "indexRollupApp", "indexRuntime", "indexSettings", "inputOutput", "inspect", "invert", "ip", "keyboardShortcut", "kqlField", "kqlFunction", "kqlOperand", "kqlSelector", "kqlValue", "layers", "lensApp", "lettering", "link", "list", "listAdd", "lock", "lockOpen", "logoAWS", "logoAWSMono", "logoAerospike", "logoApache", "logoAppSearch", "logoAzure", "logoAzureMono", "logoBeats", "logoBusinessAnalytics", "logoCeph", "logoCloud", "logoCloudEnterprise", "logoCode", "logoCodesandbox", "logoCouchbase", "logoDocker", "logoDropwizard", "logoElastic", "logoElasticStack", "logoElasticsearch", "logoEnterpriseSearch", "logoEtcd", "logoGCP", "logoGCPMono", "logoGithub", "logoGmail", "logoGolang", "logoGoogleG", "logoHAproxy", "logoIBM", "logoIBMMono", "logoKafka", "logoKibana", "logoKubernetes", "logoLogging", "logoLogstash", "logoMaps", "logoMemcached", "logoMetrics", "logoMongodb", "logoMySQL", "logoNginx", "logoObservability", "logoOsquery", "logoPhp", "logoPostgres", "logoPrometheus", "logoRabbitmq", "logoRedis", "logoSecurity", "logoSiteSearch", "logoSketch", "logoSlack", "logoUptime", "logoWebhook", "logoWindows", "logoWorkplaceSearch", "logsApp", "logstashFilter", "logstashIf", "logstashInput", "logstashOutput", "logstashQueue", "machineLearningApp", "magnet", "magnifyWithExclamation", "magnifyWithMinus", "magnifyWithPlus", "managementApp", "mapMarker", "memory", "menu", "menuDown", "menuLeft", "menuRight", "menuUp", "merge", "metricbeatApp", "metricsApp", "minimize", "minus", "minusInCircle", "minusInCircleFilled", "mobile", "monitoringApp", "moon", "nested", "node", "notebookApp", "number", "offline", "online", "outlierDetectionJob", "package", "packetbeatApp", "pageSelect", "pagesSelect", "paperClip", "partial", "pause", "payment", "pencil", "percent", "pin", "pinFilled", "pipelineApp", "play", "playFilled", "plus", "plusInCircle", "plusInCircleFilled", "popout", "push", "questionInCircle", "quote", "recentlyViewedApp", "refresh", "regressionJob", "reporter", "reportingApp", "returnKey", "save", "savedObjectsApp", "scale", "search", "searchProfilerApp", "securityAnalyticsApp", "securityApp", "securitySignal", "securitySignalDetected", "securitySignalResolved", "shard", "share", "snowflake", "sortDown", "sortLeft", "sortRight", "sortUp", "sortable", "spacesApp", "sqlApp", "starEmpty", "starEmptySpace", "starFilled", "starFilledSpace", "starMinusEmpty", "starMinusFilled", "starPlusEmpty", "starPlusFilled", "stats", "stop", "stopFilled", "stopSlash", "storage", "string", "submodule", "sun", "swatchInput", "symlink", "tableDensityCompact", "tableDensityExpanded", "tableDensityNormal", "tableOfContents", "tag", "tear", "temperature", "timeline", "timelionApp", "timeRefresh", "timeslider", "training", "trash", "unfold", "unlink", "upgradeAssistantApp", "uptimeApp", "user", "users", "usersRolesApp", "vector", "videoPlayer", "visArea", "visAreaStacked", "visBarHorizontal", "visBarHorizontalStacked", "visBarVertical", "visBarVerticalStacked", "visGauge", "visGoal", "visLine", "visMapCoordinate", "visMapRegion", "visMetric", "visPie", "visTable", "visTagCloud", "visText", "visTimelion", "visVega", "visVisualBuilder", "visualizeApp", "watchesApp", "wordWrap", "wordWrapDisabled", "workplaceSearchApp", "wrench", "tokenClass", "tokenProperty", "tokenEnum", "tokenVariable", "tokenMethod", "tokenAnnotation", "tokenException", "tokenInterface", "tokenParameter", "tokenField", "tokenElement", "tokenFunction", "tokenBoolean", "tokenString", "tokenArray", "tokenNumber", "tokenConstant", "tokenObject", "tokenEvent", "tokenKey", "tokenNull", "tokenStruct", "tokenPackage", "tokenOperator", "tokenEnumMember", "tokenRepo", "tokenSymbol", "tokenFile", "tokenModule", "tokenNamespace", "tokenDate", "tokenIP", "tokenNested", "tokenAlias", "tokenShape", "tokenGeo", "tokenRange", "tokenBinary", "tokenJoin", "tokenPercolator", "tokenFlattened", "tokenRankFeature", "tokenRankFeatures", "tokenKeyword", "tokenTag", "tokenCompletionSuggester", "tokenDenseVector", "tokenText", "tokenTokenCount", "tokenSearchType", "tokenHistogram"]).isRequired, PropTypes.string.isRequired, PropTypes.elementType.isRequired]),

  /**
     * Additional EuiIcon props to apply to the optional icon
     */
  iconProps: PropTypes.any,

  /**
     * In-app navigation presented as large borderless tabs.
     * Accepts an array of `EuiTab` objects;
     */
  tabs: PropTypes.arrayOf(PropTypes.shape({
    /**
       * Visible text of the tab
       */
    label: PropTypes.node.isRequired
  }).isRequired),

  /**
     * Any extras to apply to the outer tabs container.
     * Extends `EuiTabs`
     */
  tabsProps: PropTypes.shape({
    className: PropTypes.string,
    "aria-label": PropTypes.string,
    "data-test-subj": PropTypes.string,

    /**
         * ReactNode to render as this component's content
         */
    children: PropTypes.node,

    /**
         * Adds a bottom border to separate it from the content after
         */
    bottomBorder: PropTypes.bool
  }),

  /**
       * Position is dependent on existing with a `pageTitle` or `tabs`
       * Automatically get wrapped in a single paragraph tag inside an EuiText block
       */
  description: PropTypes.oneOfType([PropTypes.string.isRequired, PropTypes.node.isRequired]),

  /**
       * Set to false if you don't want the children to stack at small screen sizes.
       * Set to `reverse` to display the right side content first for the sake of hierarchy (like global time)
       */
  responsive: PropTypes.oneOfType([PropTypes.bool.isRequired, PropTypes.oneOf(["reverse"])]),

  /**
       * Vertical alignment of the left and right side content;
       * Default is `middle` for custom content, but `top` for when `pageTitle` or `tabs` are included
       */
  alignItems: PropTypes.any,

  /**
       * Pass custom an array of content to this side usually up to 3 buttons.
       * The first button should be primary, usually with `fill` and will be visually displayed as the last item,
       * but first in the tab order
       */
  rightSideItems: PropTypes.arrayOf(PropTypes.node.isRequired),

  /**
       * Additional EuiFlexGroup props to pass to the container of the `rightSideItems`
       */
  rightSideGroupProps: PropTypes.any,

  /**
       * Custom children will be rendered before the `tabs` unless no `pageTitle` is present, then it will be the last item
       */
  children: PropTypes.node,

  /**
     * Sets the max-width of the page,
     * set to `true` to use the default size of `1000px (1200 for Amsterdam)`,
     * set to `false` to not restrict the width,
     * set to a number for a custom width in px,
     * set to a string for a custom width in custom measurement.
     */
  restrictWidth: PropTypes.oneOfType([PropTypes.bool.isRequired, PropTypes.number.isRequired, PropTypes.string.isRequired]),

  /**
       * Adjust the padding.
       * When using this setting it's best to be consistent throughout all similar usages
       */
  paddingSize: PropTypes.any,

  /**
       * Adds a bottom border to separate it from the content after
       */
  bottomBorder: PropTypes.bool
};