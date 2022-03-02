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
import React from 'react';
import PropTypes from "prop-types";
import classNames from 'classnames';
import { EuiIcon } from '../../icon';
import { EuiTab, EuiTabs } from '../../tabs';
import { EuiFlexGroup, EuiFlexItem } from '../../flex';
import { EuiSpacer } from '../../spacer';
import { EuiTitle } from '../../title';
import { EuiText } from '../../text';
import { useIsWithinBreakpoints } from '../../../services/hooks';
import { EuiScreenReaderOnly } from '../../accessibility';
import { jsx as ___EmotionJSX } from "@emotion/react";
export var ALIGN_ITEMS = ['top', 'bottom', 'center', 'stretch']; // Gets all the tab props including the button or link props

export var EuiPageHeaderContent = function EuiPageHeaderContent(_ref) {
  var className = _ref.className,
      pageTitle = _ref.pageTitle,
      pageTitleProps = _ref.pageTitleProps,
      iconType = _ref.iconType,
      iconProps = _ref.iconProps,
      tabs = _ref.tabs,
      tabsProps = _ref.tabsProps,
      description = _ref.description,
      _ref$alignItems = _ref.alignItems,
      alignItems = _ref$alignItems === void 0 ? 'top' : _ref$alignItems,
      _ref$responsive = _ref.responsive,
      responsive = _ref$responsive === void 0 ? true : _ref$responsive,
      rightSideItems = _ref.rightSideItems,
      rightSideGroupProps = _ref.rightSideGroupProps,
      children = _ref.children,
      rest = _objectWithoutProperties(_ref, ["className", "pageTitle", "pageTitleProps", "iconType", "iconProps", "tabs", "tabsProps", "description", "alignItems", "responsive", "rightSideItems", "rightSideGroupProps", "children"]);

  var isResponsiveBreakpoint = useIsWithinBreakpoints(['xs', 's'], !!responsive);
  var classes = classNames('euiPageHeaderContent');
  var descriptionNode;

  if (description) {
    descriptionNode = ___EmotionJSX(React.Fragment, null, (pageTitle || tabs) && ___EmotionJSX(EuiSpacer, null), ___EmotionJSX(EuiText, {
      grow: false
    }, ___EmotionJSX("p", null, description)));
  }

  var pageTitleNode;

  if (pageTitle) {
    var icon = iconType ? ___EmotionJSX(EuiIcon, _extends({
      size: "xl"
    }, iconProps, {
      type: iconType,
      className: classNames('euiPageHeaderContent__titleIcon', iconProps === null || iconProps === void 0 ? void 0 : iconProps.className)
    })) : undefined;
    pageTitleNode = ___EmotionJSX(EuiTitle, _extends({}, pageTitleProps, {
      size: "l"
    }), ___EmotionJSX("h1", null, icon, pageTitle));
  }

  var tabsNode;

  if (tabs) {
    var _tabs$find;

    var tabsSize = pageTitle ? 'l' : 'xl';

    var renderTabs = function renderTabs() {
      return tabs.map(function (tab, index) {
        var label = tab.label,
            tabRest = _objectWithoutProperties(tab, ["label"]);

        return ___EmotionJSX(EuiTab, _extends({
          key: index
        }, tabRest), label);
      });
    }; // When tabs exist without a pageTitle, we need to recreate an h1 based on the currently selected tab and visually hide it


    var screenReaderPageTitle = !pageTitle && ___EmotionJSX(EuiScreenReaderOnly, null, ___EmotionJSX("h1", null, (_tabs$find = tabs.find(function (obj) {
      return obj.isSelected === true;
    })) === null || _tabs$find === void 0 ? void 0 : _tabs$find.label));

    tabsNode = ___EmotionJSX(React.Fragment, null, pageTitleNode && ___EmotionJSX(EuiSpacer, null), screenReaderPageTitle, ___EmotionJSX(EuiTabs, _extends({}, tabsProps, {
      display: "condensed",
      bottomBorder: false,
      size: tabsSize
    }), renderTabs()));
  }

  var childrenNode = children && ___EmotionJSX(React.Fragment, null, ___EmotionJSX(EuiSpacer, null), children);

  var bottomContentNode;

  if (childrenNode || tabsNode && pageTitleNode) {
    bottomContentNode = ___EmotionJSX("div", {
      className: "euiPageHeaderContent__bottom"
    }, childrenNode, pageTitleNode && tabsNode);
  }
  /**
   * The left side order depends on if a `pageTitle` was supplied.
   * If not, but there are `tabs`, then the tabs become the page title
   */


  var leftSideOrder;

  if (tabsNode && !pageTitleNode) {
    leftSideOrder = ___EmotionJSX(React.Fragment, null, tabsNode, descriptionNode);
  } else {
    leftSideOrder = ___EmotionJSX(React.Fragment, null, pageTitleNode, descriptionNode);
  }

  var rightSideFlexItem;

  if (rightSideItems && rightSideItems.length) {
    var wrapWithFlex = function wrapWithFlex() {
      return rightSideItems.map(function (item, index) {
        return ___EmotionJSX(EuiFlexItem, {
          grow: false,
          key: index
        }, item);
      });
    };

    rightSideFlexItem = ___EmotionJSX(EuiFlexItem, {
      grow: false
    }, ___EmotionJSX(EuiFlexGroup, _extends({
      wrap: true,
      responsive: false
    }, rightSideGroupProps, {
      className: classNames('euiPageHeaderContent__rightSideItems', rightSideGroupProps === null || rightSideGroupProps === void 0 ? void 0 : rightSideGroupProps.className)
    }), wrapWithFlex()));
  }

  return alignItems === 'top' || isResponsiveBreakpoint ? ___EmotionJSX("div", _extends({
    className: classes
  }, rest), ___EmotionJSX(EuiFlexGroup, {
    responsive: !!responsive,
    className: "euiPageHeaderContent__top",
    alignItems: pageTitle ? 'flexStart' : 'baseline',
    gutterSize: "l"
  }, isResponsiveBreakpoint && responsive === 'reverse' ? ___EmotionJSX(React.Fragment, null, rightSideFlexItem, ___EmotionJSX(EuiFlexItem, null, leftSideOrder)) : ___EmotionJSX(React.Fragment, null, ___EmotionJSX(EuiFlexItem, null, leftSideOrder), rightSideFlexItem)), bottomContentNode) : ___EmotionJSX("div", _extends({
    className: classes
  }, rest), ___EmotionJSX(EuiFlexGroup, {
    responsive: !!responsive,
    className: "euiPageHeaderContent__top",
    alignItems: alignItems === 'bottom' ? 'flexEnd' : alignItems,
    gutterSize: "l"
  }, ___EmotionJSX(EuiFlexItem, null, leftSideOrder, bottomContentNode), rightSideFlexItem));
};
EuiPageHeaderContent.propTypes = {
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
  children: PropTypes.node
};