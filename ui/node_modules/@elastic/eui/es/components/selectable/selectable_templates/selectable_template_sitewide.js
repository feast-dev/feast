function _extends() { _extends = Object.assign || function (target) { for (var i = 1; i < arguments.length; i++) { var source = arguments[i]; for (var key in source) { if (Object.prototype.hasOwnProperty.call(source, key)) { target[key] = source[key]; } } } return target; }; return _extends.apply(this, arguments); }

function ownKeys(object, enumerableOnly) { var keys = Object.keys(object); if (Object.getOwnPropertySymbols) { var symbols = Object.getOwnPropertySymbols(object); if (enumerableOnly) symbols = symbols.filter(function (sym) { return Object.getOwnPropertyDescriptor(object, sym).enumerable; }); keys.push.apply(keys, symbols); } return keys; }

function _objectSpread(target) { for (var i = 1; i < arguments.length; i++) { var source = arguments[i] != null ? arguments[i] : {}; if (i % 2) { ownKeys(Object(source), true).forEach(function (key) { _defineProperty(target, key, source[key]); }); } else if (Object.getOwnPropertyDescriptors) { Object.defineProperties(target, Object.getOwnPropertyDescriptors(source)); } else { ownKeys(Object(source)).forEach(function (key) { Object.defineProperty(target, key, Object.getOwnPropertyDescriptor(source, key)); }); } } return target; }

function _defineProperty(obj, key, value) { if (key in obj) { Object.defineProperty(obj, key, { value: value, enumerable: true, configurable: true, writable: true }); } else { obj[key] = value; } return obj; }

function _slicedToArray(arr, i) { return _arrayWithHoles(arr) || _iterableToArrayLimit(arr, i) || _unsupportedIterableToArray(arr, i) || _nonIterableRest(); }

function _nonIterableRest() { throw new TypeError("Invalid attempt to destructure non-iterable instance.\nIn order to be iterable, non-array objects must have a [Symbol.iterator]() method."); }

function _unsupportedIterableToArray(o, minLen) { if (!o) return; if (typeof o === "string") return _arrayLikeToArray(o, minLen); var n = Object.prototype.toString.call(o).slice(8, -1); if (n === "Object" && o.constructor) n = o.constructor.name; if (n === "Map" || n === "Set") return Array.from(o); if (n === "Arguments" || /^(?:Ui|I)nt(?:8|16|32)(?:Clamped)?Array$/.test(n)) return _arrayLikeToArray(o, minLen); }

function _arrayLikeToArray(arr, len) { if (len == null || len > arr.length) len = arr.length; for (var i = 0, arr2 = new Array(len); i < len; i++) { arr2[i] = arr[i]; } return arr2; }

function _iterableToArrayLimit(arr, i) { if (typeof Symbol === "undefined" || !(Symbol.iterator in Object(arr))) return; var _arr = []; var _n = true; var _d = false; var _e = undefined; try { for (var _i = arr[Symbol.iterator](), _s; !(_n = (_s = _i.next()).done); _n = true) { _arr.push(_s.value); if (i && _arr.length === i) break; } } catch (err) { _d = true; _e = err; } finally { try { if (!_n && _i["return"] != null) _i["return"](); } finally { if (_d) throw _e; } } return _arr; }

function _arrayWithHoles(arr) { if (Array.isArray(arr)) return arr; }

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
import { useCombinedRefs, throttle } from '../../../services';
import { EuiSelectable } from '../selectable';
import { EuiPopoverTitle, EuiPopoverFooter } from '../../popover';
import { EuiPopover } from '../../popover/popover';
import { useEuiI18n, EuiI18n } from '../../i18n';
import { EuiSelectableMessage } from '../selectable_message';
import { EuiLoadingSpinner } from '../../loading';
import { euiSelectableTemplateSitewideFormatOptions, euiSelectableTemplateSitewideRenderOptions } from './selectable_template_sitewide_option';
import { isWithinBreakpoints } from '../../../services/breakpoint';
import { EuiSpacer } from '../../spacer';
import { jsx as ___EmotionJSX } from "@emotion/react";
export var EuiSelectableTemplateSitewide = function EuiSelectableTemplateSitewide(_ref) {
  var children = _ref.children,
      className = _ref.className,
      options = _ref.options,
      popoverProps = _ref.popoverProps,
      popoverTitle = _ref.popoverTitle,
      popoverFooter = _ref.popoverFooter,
      searchProps = _ref.searchProps,
      listProps = _ref.listProps,
      isLoading = _ref.isLoading,
      popoverButton = _ref.popoverButton,
      popoverButtonBreakpoints = _ref.popoverButtonBreakpoints,
      rest = _objectWithoutProperties(_ref, ["children", "className", "options", "popoverProps", "popoverTitle", "popoverFooter", "searchProps", "listProps", "isLoading", "popoverButton", "popoverButtonBreakpoints"]);

  /**
   * Breakpoint management
   */
  var _useState = useState(typeof window !== 'undefined' && popoverButtonBreakpoints ? isWithinBreakpoints(window.innerWidth, popoverButtonBreakpoints) : true),
      _useState2 = _slicedToArray(_useState, 2),
      canShowPopoverButton = _useState2[0],
      setCanShowPopoverButton = _useState2[1];

  var functionToCallOnWindowResize = throttle(function () {
    var newWidthIsWithinBreakpoint = popoverButtonBreakpoints ? isWithinBreakpoints(window.innerWidth, popoverButtonBreakpoints) : true;

    if (newWidthIsWithinBreakpoint !== canShowPopoverButton) {
      setCanShowPopoverButton(newWidthIsWithinBreakpoint);
    } // reacts every 50ms to resize changes and always gets the final update

  }, 50); // Add window resize handlers

  useEffect(function () {
    window.addEventListener('resize', functionToCallOnWindowResize);
    return function () {
      window.removeEventListener('resize', functionToCallOnWindowResize);
    };
  }, [functionToCallOnWindowResize]);
  /**
   * i18n text
   */

  var _useEuiI18n = useEuiI18n(['euiSelectableTemplateSitewide.searchPlaceholder'], ['Search for anything...']),
      _useEuiI18n2 = _slicedToArray(_useEuiI18n, 1),
      searchPlaceholder = _useEuiI18n2[0];
  /**
   * Popover helpers
   */


  var _useState3 = useState(null),
      _useState4 = _slicedToArray(_useState3, 2),
      popoverRef = _useState4[0],
      setPopoverRef = _useState4[1];

  var _useState5 = useState(false),
      _useState6 = _slicedToArray(_useState5, 2),
      popoverIsOpen = _useState6[0],
      setPopoverIsOpen = _useState6[1];

  var _popoverProps = _objectSpread({}, popoverProps),
      _closePopover = _popoverProps.closePopover,
      panelRef = _popoverProps.panelRef,
      width = _popoverProps.width,
      popoverRest = _objectWithoutProperties(_popoverProps, ["closePopover", "panelRef", "width"]);

  var closePopover = function closePopover() {
    setPopoverIsOpen(false);
    _closePopover && _closePopover();
  };

  var togglePopover = function togglePopover() {
    setPopoverIsOpen(!popoverIsOpen);
  }; // Width applied to the internal div


  var popoverWidth = width || 600;
  var setPanelRef = useCombinedRefs([setPopoverRef, panelRef]);
  /**
   * Search helpers
   */

  var searchOnFocus = function searchOnFocus(e) {
    searchProps && searchProps.onFocus && searchProps.onFocus(e);
    if (canShowPopoverButton) return;
    setPopoverIsOpen(true);
  };

  var onSearchInput = function onSearchInput(e) {
    searchProps && searchProps.onInput && searchProps.onInput(e);
    setPopoverIsOpen(true);
  };

  var searchOnBlur = function searchOnBlur(e) {
    searchProps && searchProps.onBlur && searchProps.onBlur(e);
    if (canShowPopoverButton) return;

    if (!(popoverRef === null || popoverRef === void 0 ? void 0 : popoverRef.contains(e.relatedTarget))) {
      setPopoverIsOpen(false);
    }
  };
  /**
   * Classes
   */


  var classes = classNames('euiSelectableTemplateSitewide', className);
  var searchClasses = classNames('euiSelectableTemplateSitewide__search', searchProps && searchProps.className);
  var listClasses = classNames('euiSelectableTemplateSitewide__list', listProps && listProps.className);
  /**
   * List options
   */

  var formattedOptions = euiSelectableTemplateSitewideFormatOptions(options);

  var loadingMessage = ___EmotionJSX(EuiSelectableMessage, {
    style: {
      minHeight: 300
    }
  }, ___EmotionJSX(EuiLoadingSpinner, {
    size: "l"
  }), ___EmotionJSX("br", null), ___EmotionJSX("p", null, ___EmotionJSX(EuiI18n, {
    token: "euiSelectableTemplateSitewide.loadingResults",
    default: "Loading results"
  })));

  var emptyMessage = ___EmotionJSX(EuiSelectableMessage, {
    style: {
      minHeight: 300
    }
  }, ___EmotionJSX("p", null, ___EmotionJSX(EuiI18n, {
    token: "euiSelectableTemplateSitewide.noResults",
    default: "No results available"
  })));
  /**
   * Changes based on showing the `popoverButton` if provided.
   * This will move the search input into the popover
   * and use the passed `popoverButton` as the popover trigger.
   */


  var popoverTrigger;

  if (popoverButton && canShowPopoverButton) {
    popoverTrigger = /*#__PURE__*/React.cloneElement(popoverButton, _objectSpread(_objectSpread({}, popoverButton.props), {}, {
      onClick: togglePopover,
      onKeyDown: function onKeyDown(e) {
        // Selectable preventsDefault on Enter which kills browser controls for pressing the button
        e.stopPropagation();
      }
    }));
  }

  return ___EmotionJSX(EuiSelectable, _extends({
    isLoading: isLoading,
    options: formattedOptions,
    renderOption: euiSelectableTemplateSitewideRenderOptions,
    singleSelection: true,
    searchProps: _objectSpread(_objectSpread({
      placeholder: searchPlaceholder,
      isClearable: true
    }, searchProps), {}, {
      onFocus: searchOnFocus,
      onBlur: searchOnBlur,
      onInput: onSearchInput,
      className: searchClasses
    }),
    listProps: _objectSpread(_objectSpread({
      rowHeight: 68,
      showIcons: false,
      onFocusBadge: {
        iconSide: 'right',
        children: ___EmotionJSX(EuiI18n, {
          token: "euiSelectableTemplateSitewide.onFocusBadgeGoTo",
          default: "Go to"
        })
      }
    }, listProps), {}, {
      className: listClasses
    }),
    loadingMessage: loadingMessage,
    emptyMessage: emptyMessage,
    noMatchesMessage: emptyMessage
  }, rest, {
    className: classes,
    searchable: true
  }), function (list, search) {
    return ___EmotionJSX(EuiPopover, _extends({
      panelPaddingSize: "none",
      isOpen: popoverIsOpen,
      ownFocus: !!popoverTrigger,
      display: popoverTrigger ? 'inlineBlock' : 'block'
    }, popoverRest, {
      panelRef: setPanelRef,
      button: popoverTrigger ? popoverTrigger : search,
      closePopover: closePopover
    }), ___EmotionJSX("div", {
      style: {
        width: popoverWidth,
        maxWidth: '100%'
      }
    }, popoverTitle || popoverTrigger ? ___EmotionJSX(EuiPopoverTitle, {
      paddingSize: "s"
    }, popoverTitle, popoverTitle && search && ___EmotionJSX(EuiSpacer, null), search) : undefined, list, popoverFooter && ___EmotionJSX(EuiPopoverFooter, {
      paddingSize: "s"
    }, popoverFooter)));
  });
};
EuiSelectableTemplateSitewide.propTypes = {
  /**
     * Extends the typical EuiSelectable #Options with the addition of pre-composed elements
     * such as `icon`, `avatar`and `meta`
     */
  options: PropTypes.arrayOf(PropTypes.shape({
    /**
       * Displayed on the left (`prepend`).
       * Object of `EuiIconProps` for display of the solution/application's logo
       */
    icon: PropTypes.shape({
      className: PropTypes.string,
      "aria-label": PropTypes.string,
      "data-test-subj": PropTypes.string,

      /**
           * Descriptive title for naming the icon based on its use
           */
      title: PropTypes.string,

      /**
           * A unique identifier for the title element
           */
      titleId: PropTypes.string,

      /**
           * Its value should be one or more element IDs
           */
      "aria-labelledby": PropTypes.string,

      /**
           * Callback when the icon has been loaded & rendered
           */
      onIconLoad: PropTypes.func
    }),

    /**
       * Displayed on the right (`append`).
       * Object of `EuiAvatarProps` for display of the space (default) or user
       */
    avatar: PropTypes.shape({
      className: PropTypes.string,
      "aria-label": PropTypes.string,
      "data-test-subj": PropTypes.string,

      /**
             * Custom initials (max 2 characters).
             * By default will take the first character (of each word).
             */
      initials: PropTypes.string,

      /**
             * Specify how many characters to show (1 or 2).
             * By default, will show based on number of words (max first 2).
             */
      initialsLength: PropTypes.oneOf([1, 2]),

      /**
             * Path to an image to display instead of initials
             */
      imageUrl: PropTypes.string,

      /**
           * Any EUI glyph, logo or custom icon to display instead of initials
           */
      iconType: PropTypes.oneOfType([PropTypes.oneOf(["accessibility", "addDataApp", "advancedSettingsApp", "agentApp", "aggregate", "alert", "analyzeEvent", "annotation", "apmApp", "apmTrace", "appSearchApp", "apps", "arrowDown", "arrowLeft", "arrowRight", "arrowUp", "arrowStart", "arrowEnd", "asterisk", "auditbeatApp", "beaker", "bell", "bellSlash", "bolt", "boxesHorizontal", "boxesVertical", "branch", "broom", "brush", "bug", "bullseye", "calendar", "canvasApp", "check", "checkInCircleFilled", "cheer", "classificationJob", "clock", "cloudDrizzle", "cloudStormy", "cloudSunny", "codeApp", "color", "compute", "console", "consoleApp", "continuityAbove", "continuityAboveBelow", "continuityBelow", "continuityWithin", "controlsHorizontal", "controlsVertical", "copy", "copyClipboard", "createAdvancedJob", "createMultiMetricJob", "createPopulationJob", "createSingleMetricJob", "cross", "crossClusterReplicationApp", "crossInACircleFilled", "crosshairs", "currency", "cut", "dashboardApp", "dataVisualizer", "database", "devToolsApp", "discoverApp", "document", "documentEdit", "documentation", "documents", "dot", "doubleArrowLeft", "doubleArrowRight", "download", "editorAlignCenter", "editorAlignLeft", "editorAlignRight", "editorBold", "editorCodeBlock", "editorComment", "editorDistributeHorizontal", "editorDistributeVertical", "editorHeading", "editorItalic", "editorItemAlignBottom", "editorItemAlignCenter", "editorItemAlignLeft", "editorItemAlignMiddle", "editorItemAlignRight", "editorItemAlignTop", "editorLink", "editorOrderedList", "editorPositionBottomLeft", "editorPositionBottomRight", "editorPositionTopLeft", "editorPositionTopRight", "editorRedo", "editorStrike", "editorTable", "editorUnderline", "editorUndo", "editorUnorderedList", "email", "empty", "emsApp", "eql", "eraser", "exit", "expand", "expandMini", "exportAction", "eye", "eyeClosed", "faceHappy", "faceNeutral", "faceSad", "filebeatApp", "filter", "flag", "fleetApp", "fold", "folderCheck", "folderClosed", "folderExclamation", "folderOpen", "frameNext", "framePrevious", "fullScreen", "fullScreenExit", "function", "gear", "gisApp", "glasses", "globe", "grab", "grabHorizontal", "graphApp", "grid", "grokApp", "heart", "heartbeatApp", "heatmap", "help", "home", "iInCircle", "image", "importAction", "indexClose", "indexEdit", "indexFlush", "indexManagementApp", "indexMapping", "indexOpen", "indexPatternApp", "indexRollupApp", "indexRuntime", "indexSettings", "inputOutput", "inspect", "invert", "ip", "keyboardShortcut", "kqlField", "kqlFunction", "kqlOperand", "kqlSelector", "kqlValue", "layers", "lensApp", "lettering", "link", "list", "listAdd", "lock", "lockOpen", "logoAWS", "logoAWSMono", "logoAerospike", "logoApache", "logoAppSearch", "logoAzure", "logoAzureMono", "logoBeats", "logoBusinessAnalytics", "logoCeph", "logoCloud", "logoCloudEnterprise", "logoCode", "logoCodesandbox", "logoCouchbase", "logoDocker", "logoDropwizard", "logoElastic", "logoElasticStack", "logoElasticsearch", "logoEnterpriseSearch", "logoEtcd", "logoGCP", "logoGCPMono", "logoGithub", "logoGmail", "logoGolang", "logoGoogleG", "logoHAproxy", "logoIBM", "logoIBMMono", "logoKafka", "logoKibana", "logoKubernetes", "logoLogging", "logoLogstash", "logoMaps", "logoMemcached", "logoMetrics", "logoMongodb", "logoMySQL", "logoNginx", "logoObservability", "logoOsquery", "logoPhp", "logoPostgres", "logoPrometheus", "logoRabbitmq", "logoRedis", "logoSecurity", "logoSiteSearch", "logoSketch", "logoSlack", "logoUptime", "logoWebhook", "logoWindows", "logoWorkplaceSearch", "logsApp", "logstashFilter", "logstashIf", "logstashInput", "logstashOutput", "logstashQueue", "machineLearningApp", "magnet", "magnifyWithExclamation", "magnifyWithMinus", "magnifyWithPlus", "managementApp", "mapMarker", "memory", "menu", "menuDown", "menuLeft", "menuRight", "menuUp", "merge", "metricbeatApp", "metricsApp", "minimize", "minus", "minusInCircle", "minusInCircleFilled", "mobile", "monitoringApp", "moon", "nested", "node", "notebookApp", "number", "offline", "online", "outlierDetectionJob", "package", "packetbeatApp", "pageSelect", "pagesSelect", "paperClip", "partial", "pause", "payment", "pencil", "percent", "pin", "pinFilled", "pipelineApp", "play", "playFilled", "plus", "plusInCircle", "plusInCircleFilled", "popout", "push", "questionInCircle", "quote", "recentlyViewedApp", "refresh", "regressionJob", "reporter", "reportingApp", "returnKey", "save", "savedObjectsApp", "scale", "search", "searchProfilerApp", "securityAnalyticsApp", "securityApp", "securitySignal", "securitySignalDetected", "securitySignalResolved", "shard", "share", "snowflake", "sortDown", "sortLeft", "sortRight", "sortUp", "sortable", "spacesApp", "sqlApp", "starEmpty", "starEmptySpace", "starFilled", "starFilledSpace", "starMinusEmpty", "starMinusFilled", "starPlusEmpty", "starPlusFilled", "stats", "stop", "stopFilled", "stopSlash", "storage", "string", "submodule", "sun", "swatchInput", "symlink", "tableDensityCompact", "tableDensityExpanded", "tableDensityNormal", "tableOfContents", "tag", "tear", "temperature", "timeline", "timelionApp", "timeRefresh", "timeslider", "training", "trash", "unfold", "unlink", "upgradeAssistantApp", "uptimeApp", "user", "users", "usersRolesApp", "vector", "videoPlayer", "visArea", "visAreaStacked", "visBarHorizontal", "visBarHorizontalStacked", "visBarVertical", "visBarVerticalStacked", "visGauge", "visGoal", "visLine", "visMapCoordinate", "visMapRegion", "visMetric", "visPie", "visTable", "visTagCloud", "visText", "visTimelion", "visVega", "visVisualBuilder", "visualizeApp", "watchesApp", "wordWrap", "wordWrapDisabled", "workplaceSearchApp", "wrench", "tokenClass", "tokenProperty", "tokenEnum", "tokenVariable", "tokenMethod", "tokenAnnotation", "tokenException", "tokenInterface", "tokenParameter", "tokenField", "tokenElement", "tokenFunction", "tokenBoolean", "tokenString", "tokenArray", "tokenNumber", "tokenConstant", "tokenObject", "tokenEvent", "tokenKey", "tokenNull", "tokenStruct", "tokenPackage", "tokenOperator", "tokenEnumMember", "tokenRepo", "tokenSymbol", "tokenFile", "tokenModule", "tokenNamespace", "tokenDate", "tokenIP", "tokenNested", "tokenAlias", "tokenShape", "tokenGeo", "tokenRange", "tokenBinary", "tokenJoin", "tokenPercolator", "tokenFlattened", "tokenRankFeature", "tokenRankFeatures", "tokenKeyword", "tokenTag", "tokenCompletionSuggester", "tokenDenseVector", "tokenText", "tokenTokenCount", "tokenSearchType", "tokenHistogram"]).isRequired, PropTypes.string.isRequired, PropTypes.elementType.isRequired]),

      /**
           * Manually change icon size
           */
      iconSize: PropTypes.oneOf(["original", "s", "m", "l", "xl", "xxl"]),

      /**
           * Manually change icon color
           */
      iconColor: PropTypes.oneOfType([PropTypes.oneOfType([PropTypes.string.isRequired, PropTypes.oneOf(["default", "primary", "success", "accent", "warning", "danger", "text", "subdued", "ghost", "inherit"]).isRequired]).isRequired, PropTypes.oneOf([null])]),

      /**
           * Full name of avatar for title attribute and calculating initial if not provided
           */
      name: PropTypes.string.isRequired,

      /**
           * Accepts hex values like `#FFFFFF`, `#000` otherwise a viz palette color will be assigned.
           * Or pass `'plain'` for an empty shade or `null` to remove entirely and the text/icon color will `inherit`
           */
      color: PropTypes.oneOfType([PropTypes.string.isRequired, PropTypes.oneOf(["plain", null])]),

      /**
           * The type of avatar mainly controlling the shape.
           * `user` = circle
           * `space` = rounded square
           */
      type: PropTypes.oneOf(["space", "user"]),
      size: PropTypes.oneOf(["s", "m", "l", "xl"]),

      /**
           * Grays out the avatar to simulate being disabled
           */
      isDisabled: PropTypes.bool
    }),

    /**
       * An array of inline #MetaData displayed beneath the label and separated by bullets.
       */
    meta: PropTypes.arrayOf(PropTypes.shape({
      /**
         * Required to display the metadata
         */
      text: PropTypes.string.isRequired,

      /**
         * Styles the metadata according to Elastic's schema.
         * Can be one of 'application', 'deployment', 'article', 'case', 'platform',
         * or a custom string to associate with your own schema.
         * Appends the string to the class name as `euiSelectableTemplateSitewide__optionMeta--[type]`
         */
      type: PropTypes.oneOfType([PropTypes.oneOf(["application", "deployment", "article", "case", "platform"]), PropTypes.string.isRequired]),

      /**
         * Will wrap the meta tag in EuiHighlight to mark the portions that match the search text
         */
      highlightSearchString: PropTypes.bool,
      className: PropTypes.string,
      "aria-label": PropTypes.string,
      "data-test-subj": PropTypes.string
    }).isRequired),

    /**
       * Optional `boolean`.
       * Set to `true` to indicate object is just a grouping label, not a selectable item
       */
    isGroupLabel: PropTypes.oneOfType([PropTypes.oneOf([true]).isRequired, PropTypes.oneOf([false])]),
    className: PropTypes.string,
    "aria-label": PropTypes.string,
    "data-test-subj": PropTypes.string,

    /**
       * Visible label of option.
       * Must be unique across items if `key` is not supplied
       */
    label: PropTypes.string,

    /**
       * Optionally change the searchable term by passing a different string other than the `label`.
       * Best used when creating a custom `optionRender` to separate the label from metadata but allowing to search on both
       */
    searchableLabel: PropTypes.string,

    /**
       * Must be unique across items.
       * Will be used to match options instead of `label`
       */
    key: PropTypes.string,

    /**
       * Leave `undefined` to indicate not selected,
       * 'on' to indicate inclusion and
       * 'off' to indicate exclusion
       */
    checked: PropTypes.oneOf(["on", "off", undefined]),
    disabled: PropTypes.bool,

    /**
       * Node to add between the selection icon and the label
       */
    prepend: PropTypes.node,

    /**
       * Node to add to the far right of the item
       */
    append: PropTypes.node,
    ref: PropTypes.func,

    /**
       * Option data to pass through to the `renderOptions` element.
       * Bypass `EuiSelectableItem` and avoid DOM attribute warnings.
       */
    data: PropTypes.shape({})
  }).isRequired).isRequired,

  /**
     * Override some of the EuiPopover props housing the list.
     * The default width is `600`
     */
  popoverProps: PropTypes.shape({
    width: PropTypes.any
  }),

  /**
     * Optionally provide a title for the popover
     */
  popoverTitle: PropTypes.node,

  /**
     * Optionally provide a footer for the popover
     */
  popoverFooter: PropTypes.node,

  /**
     * Optionally provide a separate button for toggling the display of the popover.
     */
  popoverButton: PropTypes.element,

  /**
     * Pass an array of named breakpoints for which to show the `popoverButton`.
     * If `undefined`, the `popoverButton` will always show (if provided)
     */
  popoverButtonBreakpoints: PropTypes.arrayOf(PropTypes.oneOf(["xs", "s", "m", "l", "xl"]).isRequired)
};