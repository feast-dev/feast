function _extends() { _extends = Object.assign || function (target) { for (var i = 1; i < arguments.length; i++) { var source = arguments[i]; for (var key in source) { if (Object.prototype.hasOwnProperty.call(source, key)) { target[key] = source[key]; } } } return target; }; return _extends.apply(this, arguments); }

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
import React from 'react';
import PropTypes from "prop-types";
import classNames from 'classnames';
import { EuiPage } from './page';
import { EuiPageSideBar } from './page_side_bar';
import { EuiPageBody } from './page_body';
import { EuiPageHeader } from './page_header';
import { EuiPageContent, EuiPageContentBody } from './page_content';
import { EuiBottomBar } from '../bottom_bar';
import { useIsWithinBreakpoints } from '../../services';
import { EuiFlexGroup, EuiFlexItem } from '../flex';
import { jsx as ___EmotionJSX } from "@emotion/react";
export var TEMPLATES = ['default', 'centeredBody', 'centeredContent', 'empty'];
export var EuiPageTemplate = function EuiPageTemplate(_ref) {
  var _pageBodyProps2;

  var _ref$template = _ref.template,
      template = _ref$template === void 0 ? 'default' : _ref$template,
      _ref$restrictWidth = _ref.restrictWidth,
      restrictWidth = _ref$restrictWidth === void 0 ? true : _ref$restrictWidth,
      _ref$grow = _ref.grow,
      grow = _ref$grow === void 0 ? true : _ref$grow,
      _ref$paddingSize = _ref.paddingSize,
      paddingSize = _ref$paddingSize === void 0 ? 'l' : _ref$paddingSize,
      fullHeight = _ref.fullHeight,
      children = _ref.children,
      className = _ref.className,
      pageSideBar = _ref.pageSideBar,
      pageSideBarProps = _ref.pageSideBarProps,
      pageHeader = _ref.pageHeader,
      pageBodyProps = _ref.pageBodyProps,
      pageContentProps = _ref.pageContentProps,
      pageContentBodyProps = _ref.pageContentBodyProps,
      bottomBar = _ref.bottomBar,
      bottomBarProps = _ref.bottomBarProps,
      _ref$minHeight = _ref.minHeight,
      minHeight = _ref$minHeight === void 0 ? 460 : _ref$minHeight,
      rest = _objectWithoutProperties(_ref, ["template", "restrictWidth", "grow", "paddingSize", "fullHeight", "children", "className", "pageSideBar", "pageSideBarProps", "pageHeader", "pageBodyProps", "pageContentProps", "pageContentBodyProps", "bottomBar", "bottomBarProps", "minHeight"]);

  /**
   * Full height ~madness~ logic
   */
  var canFullHeight = useIsWithinBreakpoints(['m', 'l', 'xl']) && (template === 'default' || template === 'empty');
  var fullHeightClass = {
    'eui-fullHeight': fullHeight && canFullHeight
  };
  var yScrollClass = {
    'eui-yScroll': fullHeight && canFullHeight
  };

  if (canFullHeight && fullHeight) {
    var _pageBodyProps, _pageContentProps, _pageContentBodyProps;

    // By using flex group it will also fix the negative margin issues for nested flex groups
    children = ___EmotionJSX(EuiFlexGroup, {
      className: "eui-fullHeight",
      gutterSize: "none",
      direction: "column",
      responsive: false
    }, ___EmotionJSX(EuiFlexItem, {
      className: classNames({
        'eui-yScroll': fullHeight === true,
        'eui-fullHeight': fullHeight === 'noscroll'
      }),
      grow: true
    }, children));
    pageBodyProps = _objectSpread(_objectSpread({}, pageBodyProps), {}, {
      className: classNames(fullHeightClass, (_pageBodyProps = pageBodyProps) === null || _pageBodyProps === void 0 ? void 0 : _pageBodyProps.className)
    });
    pageContentProps = _objectSpread(_objectSpread({}, pageContentProps), {}, {
      className: classNames(yScrollClass, (_pageContentProps = pageContentProps) === null || _pageContentProps === void 0 ? void 0 : _pageContentProps.className)
    });
    pageContentBodyProps = _objectSpread(_objectSpread({}, pageContentBodyProps), {}, {
      className: classNames(fullHeightClass, (_pageContentBodyProps = pageContentBodyProps) === null || _pageContentBodyProps === void 0 ? void 0 : _pageContentBodyProps.className)
    });
  }

  var classes = classNames('euiPageTemplate', fullHeightClass, className);

  var pageStyle = _objectSpread({
    minHeight: minHeight
  }, rest.style);
  /**
   * This seems very repetitious but it's the most readable, scalable, and maintainable
   */


  switch (template) {
    /**
     * CENTERED BODY
     * The panelled content is centered
     */
    case 'centeredBody':
      return pageSideBar ? ___EmotionJSX(EuiPage, _extends({
        className: classes,
        paddingSize: "none",
        grow: grow
      }, rest, {
        style: pageStyle
      }), ___EmotionJSX(EuiPageSideBar, _extends({
        sticky: true,
        paddingSize: paddingSize
      }, pageSideBarProps), pageSideBar), ___EmotionJSX(EuiPageBody, _extends({
        paddingSize: paddingSize
      }, pageBodyProps), pageHeader && ___EmotionJSX(EuiPageHeader, _extends({
        restrictWidth: restrictWidth
      }, pageHeader)), ___EmotionJSX(EuiPageContent, _extends({
        verticalPosition: "center",
        horizontalPosition: "center",
        paddingSize: paddingSize
      }, pageContentProps), ___EmotionJSX(EuiPageContentBody, _extends({
        restrictWidth: restrictWidth
      }, pageContentBodyProps), children)))) : ___EmotionJSX(EuiPage, _extends({
        className: classes,
        paddingSize: paddingSize,
        grow: grow
      }, rest, {
        style: pageStyle
      }), ___EmotionJSX(EuiPageBody, _extends({
        restrictWidth: restrictWidth
      }, pageBodyProps), pageHeader && ___EmotionJSX(EuiPageHeader, _extends({
        paddingSize: "none",
        restrictWidth: false,
        bottomBorder: true
      }, pageHeader)), ___EmotionJSX(EuiPageBody, null, ___EmotionJSX(EuiPageContent, _extends({
        verticalPosition: "center",
        horizontalPosition: "center",
        paddingSize: paddingSize
      }, pageContentProps), ___EmotionJSX(EuiPageContentBody, _extends({
        paddingSize: "none",
        restrictWidth: restrictWidth
      }, pageContentBodyProps), children)))));

    /**
     * CENTERED CONTENT
     * The content inside the panel is centered
     */

    case 'centeredContent':
      return pageSideBar ? ___EmotionJSX(EuiPage, _extends({
        className: classes,
        paddingSize: "none",
        grow: grow
      }, rest, {
        style: pageStyle
      }), ___EmotionJSX(EuiPageSideBar, _extends({
        sticky: true,
        paddingSize: paddingSize
      }, pageSideBarProps), pageSideBar), ___EmotionJSX(EuiPageBody, _extends({
        panelled: true,
        paddingSize: paddingSize
      }, pageBodyProps), pageHeader && ___EmotionJSX(EuiPageHeader, _extends({
        restrictWidth: restrictWidth
      }, pageHeader)), ___EmotionJSX(EuiPageContent, _extends({
        verticalPosition: "center",
        horizontalPosition: "center",
        hasShadow: false,
        color: "subdued",
        paddingSize: paddingSize
      }, pageContentProps), ___EmotionJSX(EuiPageContentBody, _extends({
        restrictWidth: restrictWidth
      }, pageContentBodyProps), children)))) : ___EmotionJSX(EuiPage, _extends({
        className: classes,
        paddingSize: "none",
        grow: grow
      }, rest, {
        style: pageStyle
      }), ___EmotionJSX(EuiPageBody, pageBodyProps, pageHeader && ___EmotionJSX(EuiPageHeader, _extends({
        paddingSize: paddingSize,
        restrictWidth: restrictWidth
      }, pageHeader)), ___EmotionJSX(EuiPageContent, {
        role: null,
        borderRadius: "none",
        hasShadow: false,
        paddingSize: paddingSize,
        style: {
          display: 'flex'
        }
      }, ___EmotionJSX(EuiPageContent, _extends({
        verticalPosition: "center",
        horizontalPosition: "center",
        hasShadow: false,
        color: "subdued",
        paddingSize: paddingSize
      }, pageContentProps), ___EmotionJSX(EuiPageContentBody, _extends({
        restrictWidth: restrictWidth
      }, pageContentBodyProps), children)))));

    /**
     * EMPTY
     * No panelling at all
     */

    case 'empty':
      return pageSideBar ? ___EmotionJSX(EuiPage, _extends({
        className: classes,
        paddingSize: "none",
        grow: grow
      }, rest, {
        style: pageStyle
      }), ___EmotionJSX(EuiPageSideBar, _extends({
        sticky: true,
        paddingSize: paddingSize
      }, pageSideBarProps), pageSideBar), ___EmotionJSX(EuiPageBody, _extends({
        paddingSize: paddingSize
      }, pageBodyProps), pageHeader && ___EmotionJSX(EuiPageHeader, _extends({
        restrictWidth: restrictWidth
      }, pageHeader)), ___EmotionJSX(EuiPageContent, _extends({
        hasBorder: false,
        hasShadow: false,
        paddingSize: 'none',
        color: 'transparent',
        borderRadius: 'none'
      }, pageContentProps), ___EmotionJSX(EuiPageContentBody, _extends({
        restrictWidth: restrictWidth
      }, pageContentBodyProps), children)))) : ___EmotionJSX(EuiPage, _extends({
        className: classes,
        paddingSize: paddingSize,
        grow: grow
      }, rest, {
        style: pageStyle
      }), ___EmotionJSX(EuiPageBody, _extends({
        restrictWidth: restrictWidth
      }, pageBodyProps), pageHeader && ___EmotionJSX(EuiPageHeader, _extends({
        paddingSize: "none",
        restrictWidth: false,
        bottomBorder: true
      }, pageHeader)), ___EmotionJSX(EuiPageContent, _extends({
        hasBorder: false,
        hasShadow: false,
        paddingSize: 'none',
        color: 'transparent',
        borderRadius: 'none'
      }, pageContentProps), ___EmotionJSX(EuiPageContentBody, _extends({
        paddingSize: "none"
      }, pageContentBodyProps), children))));

    /**
     * DEFAULT
     * Typical layout with nothing "centered"
     */

    default:
      // Only the default template can display a bottom bar
      var bottomBarNode = bottomBar ? ___EmotionJSX(EuiBottomBar, _extends({
        paddingSize: paddingSize,
        position: canFullHeight && fullHeight ? 'static' : 'sticky' // Using uknown here because of the possible conflict with overriding props and position `sticky`

      }, bottomBarProps), ___EmotionJSX(EuiPageContentBody, {
        paddingSize: 'none',
        restrictWidth: restrictWidth
      }, bottomBar)) : undefined;
      return pageSideBar ? ___EmotionJSX(EuiPage, _extends({
        className: classes,
        paddingSize: "none",
        grow: grow
      }, rest, {
        style: pageStyle
      }), ___EmotionJSX(EuiPageSideBar, _extends({
        sticky: true,
        paddingSize: paddingSize
      }, pageSideBarProps), pageSideBar), ___EmotionJSX(EuiPageBody, _extends({
        panelled: true,
        paddingSize: "none"
      }, pageBodyProps), ___EmotionJSX(EuiPageBody, {
        component: "div",
        paddingSize: paddingSize,
        className: (_pageBodyProps2 = pageBodyProps) === null || _pageBodyProps2 === void 0 ? void 0 : _pageBodyProps2.className
      }, pageHeader && ___EmotionJSX(EuiPageHeader, _extends({
        bottomBorder: true,
        restrictWidth: restrictWidth
      }, pageHeader)), ___EmotionJSX(EuiPageContent, _extends({
        hasShadow: false,
        hasBorder: false,
        color: 'transparent',
        borderRadius: 'none',
        paddingSize: "none"
      }, pageContentProps), ___EmotionJSX(EuiPageContentBody, _extends({
        restrictWidth: restrictWidth
      }, pageContentBodyProps), children))), bottomBarNode)) : ___EmotionJSX(EuiPage, _extends({
        className: classes,
        paddingSize: "none",
        grow: grow
      }, rest, {
        style: pageStyle
      }), ___EmotionJSX(EuiPageBody, pageBodyProps, pageHeader && ___EmotionJSX(EuiPageHeader, _extends({
        restrictWidth: restrictWidth,
        paddingSize: paddingSize
      }, pageHeader)), ___EmotionJSX(EuiPageContent, _extends({
        hasBorder: pageHeader === undefined ? false : undefined,
        hasShadow: false,
        paddingSize: 'none',
        color: 'plain',
        borderRadius: 'none'
      }, pageContentProps), ___EmotionJSX(EuiPageContentBody, _extends({
        restrictWidth: restrictWidth,
        paddingSize: paddingSize
      }, pageContentBodyProps), children)), bottomBarNode));
  }
};
EuiPageTemplate.propTypes = {
  /**
     * Choose between 3 types of templates.
     * `default`: Typical layout with nothing centered
     * `centeredBody`: The panelled content is centered
     * `centeredContent`: The content inside the panel is centered
     * `empty`: Removes the panneling of the page content
     */
  template: PropTypes.any,

  /**
     * Padding size will not get applied to the over-arching #EuiPage,
     * but will propogate through all the components to keep them in sync
     */
  paddingSize: PropTypes.any,

  /**
     * Optionally include #EuiPageSideBar content.
     * The inclusion of this will affect the whole layout
     */
  pageSideBar: PropTypes.node,

  /**
     * Gets passed along to the #EuiPageSideBar component
     */
  pageSideBarProps: PropTypes.shape({
    /**
       * Adds `position: sticky` and affords for any fixed position headers
       */
    sticky: PropTypes.bool,

    /**
       * Adds padding around the children
       */
    paddingSize: PropTypes.any,
    className: PropTypes.string,
    "aria-label": PropTypes.string,
    "data-test-subj": PropTypes.string
  }),

  /**
     * Optionally include an #EuiPageHeader by passing an object of its props
     */
  pageHeader: PropTypes.shape({
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
  }),

  /**
     * Gets passed along to the #EuiPageBody component
     */
  pageBodyProps: PropTypes.shape({
    className: PropTypes.string,
    "aria-label": PropTypes.string,
    "data-test-subj": PropTypes.string,

    /**
       * Sets the max-width of the page,
       * set to `true` to use the default size of `1000px (1200 for Amsterdam)`,
       * set to `false` to not restrict the width,
       * set to a number for a custom width in px,
       * set to a string for a custom width in custom measurement.
       */
    restrictWidth: PropTypes.oneOfType([PropTypes.bool.isRequired, PropTypes.number.isRequired, PropTypes.string.isRequired]),

    /**
         * Sets the HTML element for `EuiPageBody`.
         */
    component: PropTypes.any,

    /**
         * Uses an EuiPanel as the main component instead of a plain div
         */
    panelled: PropTypes.bool,

    /**
         * Extends any extra EuiPanel props if `panelled=true`
         */
    panelProps: PropTypes.any,

    /**
         * Adjusts the padding
         */
    paddingSize: PropTypes.any
  }),

  /**
     * Gets passed along to the #EuiPageContent component
     */
  pageContentProps: PropTypes.shape({
    className: PropTypes.string,
    "aria-label": PropTypes.string,
    "data-test-subj": PropTypes.string,

    /**
       * Adds a medium shadow to the panel;
       * Only works when `color="plain"`
       */

    /**
       * Adds a medium shadow to the panel;
       * Only works when `color="plain"`
       */
    hasShadow: PropTypes.bool,

    /**
       * Adds a slight 1px border on all edges.
       * Only works when `color="plain | transparent"`
       * Default is `undefined` and will default to that theme's panel style
       */

    /**
       * Adds a slight 1px border on all edges.
       * Only works when `color="plain | transparent"`
       * Default is `undefined` and will default to that theme's panel style
       */
    hasBorder: PropTypes.bool,

    /**
       * Padding for all four sides
       */

    /**
       * Padding for all four sides
       */
    paddingSize: PropTypes.any,

    /**
       * Corner border radius
       */

    /**
       * Corner border radius
       */
    borderRadius: PropTypes.any,

    /**
       * When true the panel will grow in height to match `EuiFlexItem`
       */

    /**
       * When true the panel will grow in height to match `EuiFlexItem`
       */
    grow: PropTypes.bool,
    panelRef: PropTypes.any,

    /**
       * Background color of the panel;
       * Usually a lightened form of the brand colors
       */

    /**
       * Background color of the panel;
       * Usually a lightened form of the brand colors
       */
    color: PropTypes.any,
    element: PropTypes.oneOf(["div"]),
    verticalPosition: PropTypes.oneOf(["center"]),
    horizontalPosition: PropTypes.oneOf(["center"]),

    /**
         * There should only be one EuiPageContent per page and should contain the main contents.
         * If this is untrue, set role = `null`, or change it to match your needed aria role
         */
    role: PropTypes.oneOfType([PropTypes.any.isRequired, PropTypes.oneOf([null])])
  }),

  /**
     * Gets passed along to the #EuiPageContentBody component
     */
  pageContentBodyProps: PropTypes.shape({
    /**
       * Adjust the padding.
       * When using this setting it's best to be consistent throughout all similar usages
       */
    paddingSize: PropTypes.any,
    className: PropTypes.string,
    "aria-label": PropTypes.string,
    "data-test-subj": PropTypes.string,

    /**
       * Sets the max-width of the page,
       * set to `true` to use the default size of `1000px (1200 for Amsterdam)`,
       * set to `false` to not restrict the width,
       * set to a number for a custom width in px,
       * set to a string for a custom width in custom measurement.
       */
    restrictWidth: PropTypes.oneOfType([PropTypes.bool.isRequired, PropTypes.number.isRequired, PropTypes.string.isRequired])
  }),

  /**
     * Adds contents inside of an EuiBottomBar.
     * Only works when `template = 'default'`
     */
  bottomBar: PropTypes.any,

  /**
     * Gets passed along to the #EuiBottomBar component if `bottomBar` has contents
     */
  bottomBarProps: PropTypes.shape({
    className: PropTypes.string,
    "aria-label": PropTypes.string,
    "data-test-subj": PropTypes.string,

    /**
         * How to position the bottom bar against its parent.
         */
    position: PropTypes.oneOfType([PropTypes.oneOf(["fixed"]), PropTypes.oneOf(["static", "sticky"]).isRequired]),

    /**
         * Whether to wrap in an EuiPortal which appends the component to the body element.
         * Only works if `position` is `fixed`.
         */
    usePortal: PropTypes.bool,

    /**
         * Whether the component should apply padding on the document body element to afford for its own displacement height.
         * Only works if `usePortal` is true and `position` is `fixed`.
         */
    affordForDisplacement: PropTypes.bool,

    /**
         * Padding applied to the bar. Default is 'm'.
         */
    paddingSize: PropTypes.oneOf(["none", "s", "m", "l"]),

    /**
         * Optional class applied to the body element on mount.
         */
    bodyClassName: PropTypes.string,

    /**
         * Customize the screen reader heading that helps users find this control. Default is 'Page level controls'.
         */
    landmarkHeading: PropTypes.string,

    /**
         * Starting vertical position when `fixed` position.
         * Offset from the top of the window when `sticky` position.
         * Has no affect on `static` positions.
         */
    top: PropTypes.any,

    /**
         * Ending horizontal position when `fixed` position.
         * Has no affect on `static` or `sticky` positions.
         */
    right: PropTypes.any,

    /**
         * Starting vertical position when `fixed` position.
         * Offset from the bottom of the window when `sticky` position.
         * Has no affect on `static` positions.
         */
    bottom: PropTypes.any,

    /**
         * Starting horizontal position when `fixed` position.
         * Has no affect on `static` or `sticky` positions.
         */
    left: PropTypes.any
  }),

  /**
     * Stretches or restricts the height to 100% of the parent;
     * `true`: scrolls the EuiPageContentBody;
     * `noscroll`: removes all scroll ability;
     * Only works when `template = 'default | empty'` and breakpoint is `m` and above
     */
  fullHeight: PropTypes.oneOfType([PropTypes.bool.isRequired, PropTypes.oneOf(["noscroll"])]),

  /**
     * Minimum height in which to enforce scrolling
     */
  minHeight: PropTypes.any
};