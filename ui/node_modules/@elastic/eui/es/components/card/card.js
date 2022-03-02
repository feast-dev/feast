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
import React, { isValidElement } from 'react';
import PropTypes from "prop-types";
import classNames from 'classnames';
import { keysOf } from '../common';
import { getSecureRelForTarget } from '../../services';
import { EuiText } from '../text';
import { EuiTitle } from '../title';
import { EuiBetaBadge } from '../badge/beta_badge';
import { EuiCardSelect, euiCardSelectableColor } from './card_select';
import { useGeneratedHtmlId } from '../../services/accessibility';
import { validateHref } from '../../services/security/href_validator';
import { EuiPanel } from '../panel';
import { jsx as ___EmotionJSX } from "@emotion/react";
var textAlignToClassNameMap = {
  left: 'euiCard--leftAligned',
  center: 'euiCard--centerAligned',
  right: 'euiCard--rightAligned'
};
export var ALIGNMENTS = keysOf(textAlignToClassNameMap);
var layoutToClassNameMap = {
  vertical: '',
  horizontal: 'euiCard--horizontal'
};
export var LAYOUT_ALIGNMENTS = keysOf(layoutToClassNameMap);
/**
 * Certain props are only allowed when the layout is vertical
 */

export var EuiCard = function EuiCard(_ref) {
  var className = _ref.className,
      description = _ref.description,
      _isDisabled = _ref.isDisabled,
      title = _ref.title,
      _ref$titleElement = _ref.titleElement,
      titleElement = _ref$titleElement === void 0 ? 'span' : _ref$titleElement,
      _ref$titleSize = _ref.titleSize,
      titleSize = _ref$titleSize === void 0 ? 's' : _ref$titleSize,
      icon = _ref.icon,
      image = _ref.image,
      children = _ref.children,
      footer = _ref.footer,
      onClick = _ref.onClick,
      href = _ref.href,
      rel = _ref.rel,
      target = _ref.target,
      _ref$textAlign = _ref.textAlign,
      textAlign = _ref$textAlign === void 0 ? 'center' : _ref$textAlign,
      betaBadgeProps = _ref.betaBadgeProps,
      _ref$layout = _ref.layout,
      layout = _ref$layout === void 0 ? 'vertical' : _ref$layout,
      selectable = _ref.selectable,
      display = _ref.display,
      paddingSize = _ref.paddingSize,
      rest = _objectWithoutProperties(_ref, ["className", "description", "isDisabled", "title", "titleElement", "titleSize", "icon", "image", "children", "footer", "onClick", "href", "rel", "target", "textAlign", "betaBadgeProps", "layout", "selectable", "display", "paddingSize"]);

  var isHrefValid = !href || validateHref(href);
  var isDisabled = _isDisabled || !isHrefValid;
  var isClickable = !isDisabled && (onClick || href || selectable && !selectable.isDisabled);
  /**
   * For a11y, we simulate the same click that's provided on the title when clicking the whole card
   * without having to make the whole card a button or anchor tag.
   * *Card Accessibility: The redundant click event https://inclusive-components.design/cards/*
   */

  var link = null;

  var outerOnClick = function outerOnClick(e) {
    if (link && link !== e.target) {
      link.click();
    }
  };

  if (layout === 'horizontal') {
    if (image || footer || textAlign !== 'center') {
      throw new Error('EuiCard: `layout="horizontal"` cannot be used in conjunction with `image`, `footer`, or `textAlign`.');
    }
  }

  var selectableColorClass = selectable ? "euiCard--isSelectable--".concat(euiCardSelectableColor(selectable.color, selectable.isSelected)) : undefined;
  var classes = classNames('euiCard', textAlignToClassNameMap[textAlign], layoutToClassNameMap[layout], {
    'euiCard--isClickable': isClickable,
    'euiCard--hasBetaBadge': betaBadgeProps === null || betaBadgeProps === void 0 ? void 0 : betaBadgeProps.label,
    'euiCard--hasIcon': icon,
    'euiCard--isSelectable': selectable,
    'euiCard-isSelected': selectable === null || selectable === void 0 ? void 0 : selectable.isSelected,
    'euiCard-isDisabled': isDisabled
  }, selectableColorClass, className);
  var ariaId = useGeneratedHtmlId();
  var ariaDesc = description ? "".concat(ariaId, "Description") : '';
  /**
   * Top area containing image, icon or both
   */

  var imageNode;

  if (image && layout === 'vertical') {
    if ( /*#__PURE__*/isValidElement(image) || typeof image === 'string') {
      imageNode = ___EmotionJSX("div", {
        className: "euiCard__image"
      }, /*#__PURE__*/isValidElement(image) ? image : ___EmotionJSX("img", {
        src: image,
        alt: ""
      }));
    } else {
      imageNode = null;
    }
  }

  var iconNode;

  if (icon) {
    iconNode = /*#__PURE__*/React.cloneElement(icon, {
      className: classNames(icon.props.className, 'euiCard__icon')
    });
  }

  var optionalCardTop;

  if (imageNode || iconNode) {
    optionalCardTop = ___EmotionJSX("div", {
      className: "euiCard__top"
    }, imageNode, iconNode);
  }
  /**
   * Optional EuiBetaBadge
   */


  var optionalBetaBadge;
  var optionalBetaBadgeID = '';

  if (betaBadgeProps === null || betaBadgeProps === void 0 ? void 0 : betaBadgeProps.label) {
    optionalBetaBadgeID = "".concat(ariaId, "BetaBadge");
    optionalBetaBadge = ___EmotionJSX("span", {
      className: "euiCard__betaBadgeWrapper"
    }, ___EmotionJSX(EuiBetaBadge, _extends({
      id: optionalBetaBadgeID
    }, betaBadgeProps, {
      className: classNames('euiCard__betaBadge', betaBadgeProps === null || betaBadgeProps === void 0 ? void 0 : betaBadgeProps.className)
    }))); // Increase padding size when there is a beta badge unless it's already determined

    paddingSize = paddingSize || 'l';
  }
  /**
   * Optional selectable button
   */


  if (selectable && isDisabled && selectable.isDisabled === undefined) {
    selectable.isDisabled = isDisabled;
  }

  var optionalSelectButton;

  if (selectable) {
    optionalSelectButton = ___EmotionJSX(EuiCardSelect, _extends({
      "aria-describedby": "".concat(ariaId, "Title ").concat(ariaDesc)
    }, selectable, {
      buttonRef: function buttonRef(node) {
        link = node;
      }
    }));
  }
  /**
   * Wraps the title with the link (<a>) or button.
   * This makes the title element a11y friendly and gets described by its content if its interactable.
   */


  var theTitle;

  if (!isDisabled && href) {
    theTitle = ___EmotionJSX("a", {
      className: "euiCard__titleAnchor",
      onClick: onClick,
      href: href,
      target: target,
      "aria-describedby": ariaDesc,
      rel: getSecureRelForTarget({
        href: href,
        target: target,
        rel: rel
      }),
      ref: function ref(node) {
        link = node;
      }
    }, title);
  } else if (isDisabled || onClick) {
    theTitle = ___EmotionJSX("button", {
      className: "euiCard__titleButton",
      onClick: onClick,
      disabled: isDisabled,
      "aria-describedby": "".concat(optionalBetaBadgeID, " ").concat(ariaDesc),
      ref: function ref(node) {
        link = node;
      }
    }, title);
  } else {
    theTitle = title;
  }
  /**
   * Convert titleElement to a capital TitleElement
   */


  var TitleElement = titleElement;
  return ___EmotionJSX(EuiPanel, _extends({
    element: "div",
    className: classes,
    onClick: isClickable ? outerOnClick : undefined,
    color: isDisabled ? 'subdued' : display,
    hasShadow: isDisabled || display ? false : true,
    hasBorder: display ? false : undefined,
    paddingSize: paddingSize
  }, rest), optionalCardTop, ___EmotionJSX("div", {
    className: "euiCard__content"
  }, ___EmotionJSX(EuiTitle, {
    id: "".concat(ariaId, "Title"),
    className: "euiCard__title",
    size: titleSize
  }, ___EmotionJSX(TitleElement, null, theTitle)), description && ___EmotionJSX(EuiText, {
    id: ariaDesc,
    size: "s",
    className: "euiCard__description"
  }, ___EmotionJSX("p", null, description)), children && ___EmotionJSX("div", {
    className: "euiCard__children"
  }, children)), optionalBetaBadge, layout === 'vertical' && footer && ___EmotionJSX("div", {
    className: "euiCard__footer"
  }, footer), optionalSelectButton);
};
EuiCard.propTypes = {
  /**
       * Change to "horizontal" if you need the icon to be left of the content.
       * Horizontal layouts cannot be used in conjunction with `image`, `footer`, or `textAlign`.
       */
  layout: PropTypes.oneOfType([PropTypes.oneOf(["vertical"]), PropTypes.oneOf(["horizontal"]).isRequired]),

  /**
       * Changes alignment of the title and description
       */
  textAlign: PropTypes.oneOf(["left", "center", "right"]),

  /**
       * Accepts any combination of elements
       */
  footer: PropTypes.node,

  /**
       * Accepts a url in string form or ReactElement for a custom image component
       */
  image: PropTypes.oneOfType([PropTypes.string.isRequired, PropTypes.element.isRequired]),

  /**
       * Cards are required to have at least a title and a description and/or children
       */
  title: PropTypes.any.isRequired,

  /**
       * Determines the title's heading element
       */
  titleElement: PropTypes.oneOf(["h2", "h3", "h4", "h5", "h6", "span"]),

  /**
       * Determines the title's size, matching that of EuiTitle.
       * Though, card titles can't be too large or small relative to the description text.
       */
  titleSize: PropTypes.oneOf(["s", "xs"]),
  // description is required if children is omitted
  // description becomes optional when children is present

  /**
       * Placed within a small EuiText `<p>` tag
       */
  description: PropTypes.any,

  /**
       * Accepts an `<EuiIcon>` node or `null`
       */
  icon: PropTypes.oneOfType([PropTypes.element.isRequired, PropTypes.oneOf([null])]),

  /**
       * Custom children
       */
  children: PropTypes.node,

  /**
       * Use only if you want to forego a button in the footer and make the whole card clickable
       */
  onClick: PropTypes.oneOfType([PropTypes.func.isRequired, PropTypes.func.isRequired]),
  isDisabled: PropTypes.bool,
  href: PropTypes.string,
  target: PropTypes.string,
  rel: PropTypes.string,

  /**
       * Adds a badge to top of the card to label it as "Beta" or other non-GA state.
       * Accepts all the props of [EuiBetaBadge](#/display/badge#beta-badge-type), where `label` is required.
       */
  betaBadgeProps: PropTypes.any,

  /**
       * Matches to the color property of EuiPanel. If defined, removes any border & shadow.
       * Leave as `undefined` to display as a default panel.
       * Selectable cards will always display as a default panel.
       */
  display: PropTypes.any,

  /**
       * Padding applied around the content of the card
       */
  paddingSize: PropTypes.any,

  /**
       * Adds a button to the bottom of the card to allow for in-place selection
       */
  selectable: PropTypes.shape({
    href: PropTypes.string,
    onClick: PropTypes.func,

    /**
       * Any of our named colors
       */

    /**
       * Any of our named colors
       */
    color: PropTypes.oneOf(["primary", "danger", "text", "ghost", "success", "warning"]),
    size: PropTypes.oneOf(["xs", "s", "m"]),

    /**
       * Ensure the text of the button sits flush to the left, right, or both sides of its container
       */

    /**
       * Ensure the text of the button sits flush to the left, right, or both sides of its container
       */
    flush: PropTypes.oneOf(["left", "right", "both"]),

    /**
       * `disabled` is also allowed
       */

    /**
       * `disabled` is also allowed
       */
    isDisabled: PropTypes.bool,

    /**
       * Force disables the button and changes the icon to a loading spinner
       */

    /**
       * Force disables the button and changes the icon to a loading spinner
       */
    isLoading: PropTypes.bool,

    /**
       * Is in the selected state
       */

    /**
       * Applies the boolean state as the `aria-pressed` property to create a toggle button.
       * *Only use when the readable text does not change between states.*
       */

    /**
       * Applies the boolean state as the `aria-pressed` property to create a toggle button.
       * *Only use when the readable text does not change between states.*
       */
    isSelected: PropTypes.bool,
    target: PropTypes.string,
    rel: PropTypes.string,
    type: PropTypes.oneOf(["button", "submit"]),
    buttonRef: PropTypes.any,

    /**
       * Object of props passed to the <span/> wrapping the button's content
       */
    contentProps: PropTypes.any,

    /**
       * Any `type` accepted by EuiIcon
       */
    iconType: PropTypes.oneOfType([PropTypes.oneOf(["accessibility", "addDataApp", "advancedSettingsApp", "agentApp", "aggregate", "alert", "analyzeEvent", "annotation", "apmApp", "apmTrace", "appSearchApp", "apps", "arrowDown", "arrowLeft", "arrowRight", "arrowUp", "arrowStart", "arrowEnd", "asterisk", "auditbeatApp", "beaker", "bell", "bellSlash", "bolt", "boxesHorizontal", "boxesVertical", "branch", "broom", "brush", "bug", "bullseye", "calendar", "canvasApp", "check", "checkInCircleFilled", "cheer", "classificationJob", "clock", "cloudDrizzle", "cloudStormy", "cloudSunny", "codeApp", "color", "compute", "console", "consoleApp", "continuityAbove", "continuityAboveBelow", "continuityBelow", "continuityWithin", "controlsHorizontal", "controlsVertical", "copy", "copyClipboard", "createAdvancedJob", "createMultiMetricJob", "createPopulationJob", "createSingleMetricJob", "cross", "crossClusterReplicationApp", "crossInACircleFilled", "crosshairs", "currency", "cut", "dashboardApp", "dataVisualizer", "database", "devToolsApp", "discoverApp", "document", "documentEdit", "documentation", "documents", "dot", "doubleArrowLeft", "doubleArrowRight", "download", "editorAlignCenter", "editorAlignLeft", "editorAlignRight", "editorBold", "editorCodeBlock", "editorComment", "editorDistributeHorizontal", "editorDistributeVertical", "editorHeading", "editorItalic", "editorItemAlignBottom", "editorItemAlignCenter", "editorItemAlignLeft", "editorItemAlignMiddle", "editorItemAlignRight", "editorItemAlignTop", "editorLink", "editorOrderedList", "editorPositionBottomLeft", "editorPositionBottomRight", "editorPositionTopLeft", "editorPositionTopRight", "editorRedo", "editorStrike", "editorTable", "editorUnderline", "editorUndo", "editorUnorderedList", "email", "empty", "emsApp", "eql", "eraser", "exit", "expand", "expandMini", "exportAction", "eye", "eyeClosed", "faceHappy", "faceNeutral", "faceSad", "filebeatApp", "filter", "flag", "fleetApp", "fold", "folderCheck", "folderClosed", "folderExclamation", "folderOpen", "frameNext", "framePrevious", "fullScreen", "fullScreenExit", "function", "gear", "gisApp", "glasses", "globe", "grab", "grabHorizontal", "graphApp", "grid", "grokApp", "heart", "heartbeatApp", "heatmap", "help", "home", "iInCircle", "image", "importAction", "indexClose", "indexEdit", "indexFlush", "indexManagementApp", "indexMapping", "indexOpen", "indexPatternApp", "indexRollupApp", "indexRuntime", "indexSettings", "inputOutput", "inspect", "invert", "ip", "keyboardShortcut", "kqlField", "kqlFunction", "kqlOperand", "kqlSelector", "kqlValue", "layers", "lensApp", "lettering", "link", "list", "listAdd", "lock", "lockOpen", "logoAWS", "logoAWSMono", "logoAerospike", "logoApache", "logoAppSearch", "logoAzure", "logoAzureMono", "logoBeats", "logoBusinessAnalytics", "logoCeph", "logoCloud", "logoCloudEnterprise", "logoCode", "logoCodesandbox", "logoCouchbase", "logoDocker", "logoDropwizard", "logoElastic", "logoElasticStack", "logoElasticsearch", "logoEnterpriseSearch", "logoEtcd", "logoGCP", "logoGCPMono", "logoGithub", "logoGmail", "logoGolang", "logoGoogleG", "logoHAproxy", "logoIBM", "logoIBMMono", "logoKafka", "logoKibana", "logoKubernetes", "logoLogging", "logoLogstash", "logoMaps", "logoMemcached", "logoMetrics", "logoMongodb", "logoMySQL", "logoNginx", "logoObservability", "logoOsquery", "logoPhp", "logoPostgres", "logoPrometheus", "logoRabbitmq", "logoRedis", "logoSecurity", "logoSiteSearch", "logoSketch", "logoSlack", "logoUptime", "logoWebhook", "logoWindows", "logoWorkplaceSearch", "logsApp", "logstashFilter", "logstashIf", "logstashInput", "logstashOutput", "logstashQueue", "machineLearningApp", "magnet", "magnifyWithExclamation", "magnifyWithMinus", "magnifyWithPlus", "managementApp", "mapMarker", "memory", "menu", "menuDown", "menuLeft", "menuRight", "menuUp", "merge", "metricbeatApp", "metricsApp", "minimize", "minus", "minusInCircle", "minusInCircleFilled", "mobile", "monitoringApp", "moon", "nested", "node", "notebookApp", "number", "offline", "online", "outlierDetectionJob", "package", "packetbeatApp", "pageSelect", "pagesSelect", "paperClip", "partial", "pause", "payment", "pencil", "percent", "pin", "pinFilled", "pipelineApp", "play", "playFilled", "plus", "plusInCircle", "plusInCircleFilled", "popout", "push", "questionInCircle", "quote", "recentlyViewedApp", "refresh", "regressionJob", "reporter", "reportingApp", "returnKey", "save", "savedObjectsApp", "scale", "search", "searchProfilerApp", "securityAnalyticsApp", "securityApp", "securitySignal", "securitySignalDetected", "securitySignalResolved", "shard", "share", "snowflake", "sortDown", "sortLeft", "sortRight", "sortUp", "sortable", "spacesApp", "sqlApp", "starEmpty", "starEmptySpace", "starFilled", "starFilledSpace", "starMinusEmpty", "starMinusFilled", "starPlusEmpty", "starPlusFilled", "stats", "stop", "stopFilled", "stopSlash", "storage", "string", "submodule", "sun", "swatchInput", "symlink", "tableDensityCompact", "tableDensityExpanded", "tableDensityNormal", "tableOfContents", "tag", "tear", "temperature", "timeline", "timelionApp", "timeRefresh", "timeslider", "training", "trash", "unfold", "unlink", "upgradeAssistantApp", "uptimeApp", "user", "users", "usersRolesApp", "vector", "videoPlayer", "visArea", "visAreaStacked", "visBarHorizontal", "visBarHorizontalStacked", "visBarVertical", "visBarVerticalStacked", "visGauge", "visGoal", "visLine", "visMapCoordinate", "visMapRegion", "visMetric", "visPie", "visTable", "visTagCloud", "visText", "visTimelion", "visVega", "visVisualBuilder", "visualizeApp", "watchesApp", "wordWrap", "wordWrapDisabled", "workplaceSearchApp", "wrench", "tokenClass", "tokenProperty", "tokenEnum", "tokenVariable", "tokenMethod", "tokenAnnotation", "tokenException", "tokenInterface", "tokenParameter", "tokenField", "tokenElement", "tokenFunction", "tokenBoolean", "tokenString", "tokenArray", "tokenNumber", "tokenConstant", "tokenObject", "tokenEvent", "tokenKey", "tokenNull", "tokenStruct", "tokenPackage", "tokenOperator", "tokenEnumMember", "tokenRepo", "tokenSymbol", "tokenFile", "tokenModule", "tokenNamespace", "tokenDate", "tokenIP", "tokenNested", "tokenAlias", "tokenShape", "tokenGeo", "tokenRange", "tokenBinary", "tokenJoin", "tokenPercolator", "tokenFlattened", "tokenRankFeature", "tokenRankFeatures", "tokenKeyword", "tokenTag", "tokenCompletionSuggester", "tokenDenseVector", "tokenText", "tokenTokenCount", "tokenSearchType", "tokenHistogram"]).isRequired, PropTypes.string.isRequired, PropTypes.elementType.isRequired]),

    /**
       * Can only be one side `left` or `right`
       */
    iconSide: PropTypes.oneOf(["left", "right"]),

    /**
       * Object of props passed to the <span/> wrapping the content's text/children only (not icon)
       */
    textProps: PropTypes.shape({
      className: PropTypes.string,
      "aria-label": PropTypes.string,
      "data-test-subj": PropTypes.string,
      ref: PropTypes.any,
      "data-text": PropTypes.string
    }),
    iconSize: PropTypes.oneOf(["s", "m"]),
    className: PropTypes.string,
    "aria-label": PropTypes.string,
    "data-test-subj": PropTypes.string
  }),

  /**
       * Use a border style of card instead of shadow
       */
  hasBorder: PropTypes.bool
};