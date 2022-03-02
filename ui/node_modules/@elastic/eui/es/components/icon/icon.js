function _typeof(obj) { "@babel/helpers - typeof"; if (typeof Symbol === "function" && typeof Symbol.iterator === "symbol") { _typeof = function _typeof(obj) { return typeof obj; }; } else { _typeof = function _typeof(obj) { return obj && typeof Symbol === "function" && obj.constructor === Symbol && obj !== Symbol.prototype ? "symbol" : typeof obj; }; } return _typeof(obj); }

function _extends() { _extends = Object.assign || function (target) { for (var i = 1; i < arguments.length; i++) { var source = arguments[i]; for (var key in source) { if (Object.prototype.hasOwnProperty.call(source, key)) { target[key] = source[key]; } } } return target; }; return _extends.apply(this, arguments); }

function _objectWithoutProperties(source, excluded) { if (source == null) return {}; var target = _objectWithoutPropertiesLoose(source, excluded); var key, i; if (Object.getOwnPropertySymbols) { var sourceSymbolKeys = Object.getOwnPropertySymbols(source); for (i = 0; i < sourceSymbolKeys.length; i++) { key = sourceSymbolKeys[i]; if (excluded.indexOf(key) >= 0) continue; if (!Object.prototype.propertyIsEnumerable.call(source, key)) continue; target[key] = source[key]; } } return target; }

function _objectWithoutPropertiesLoose(source, excluded) { if (source == null) return {}; var target = {}; var sourceKeys = Object.keys(source); var key, i; for (i = 0; i < sourceKeys.length; i++) { key = sourceKeys[i]; if (excluded.indexOf(key) >= 0) continue; target[key] = source[key]; } return target; }

function _classCallCheck(instance, Constructor) { if (!(instance instanceof Constructor)) { throw new TypeError("Cannot call a class as a function"); } }

function _defineProperties(target, props) { for (var i = 0; i < props.length; i++) { var descriptor = props[i]; descriptor.enumerable = descriptor.enumerable || false; descriptor.configurable = true; if ("value" in descriptor) descriptor.writable = true; Object.defineProperty(target, descriptor.key, descriptor); } }

function _createClass(Constructor, protoProps, staticProps) { if (protoProps) _defineProperties(Constructor.prototype, protoProps); if (staticProps) _defineProperties(Constructor, staticProps); return Constructor; }

function _inherits(subClass, superClass) { if (typeof superClass !== "function" && superClass !== null) { throw new TypeError("Super expression must either be null or a function"); } subClass.prototype = Object.create(superClass && superClass.prototype, { constructor: { value: subClass, writable: true, configurable: true } }); if (superClass) _setPrototypeOf(subClass, superClass); }

function _setPrototypeOf(o, p) { _setPrototypeOf = Object.setPrototypeOf || function _setPrototypeOf(o, p) { o.__proto__ = p; return o; }; return _setPrototypeOf(o, p); }

function _createSuper(Derived) { var hasNativeReflectConstruct = _isNativeReflectConstruct(); return function _createSuperInternal() { var Super = _getPrototypeOf(Derived), result; if (hasNativeReflectConstruct) { var NewTarget = _getPrototypeOf(this).constructor; result = Reflect.construct(Super, arguments, NewTarget); } else { result = Super.apply(this, arguments); } return _possibleConstructorReturn(this, result); }; }

function _possibleConstructorReturn(self, call) { if (call && (_typeof(call) === "object" || typeof call === "function")) { return call; } return _assertThisInitialized(self); }

function _assertThisInitialized(self) { if (self === void 0) { throw new ReferenceError("this hasn't been initialised - super() hasn't been called"); } return self; }

function _isNativeReflectConstruct() { if (typeof Reflect === "undefined" || !Reflect.construct) return false; if (Reflect.construct.sham) return false; if (typeof Proxy === "function") return true; try { Date.prototype.toString.call(Reflect.construct(Date, [], function () {})); return true; } catch (e) { return false; } }

function _getPrototypeOf(o) { _getPrototypeOf = Object.setPrototypeOf ? Object.getPrototypeOf : function _getPrototypeOf(o) { return o.__proto__ || Object.getPrototypeOf(o); }; return _getPrototypeOf(o); }

function _defineProperty(obj, key, value) { if (key in obj) { Object.defineProperty(obj, key, { value: value, enumerable: true, configurable: true, writable: true }); } else { obj[key] = value; } return obj; }

/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */
import React, { PureComponent } from 'react';
import PropTypes from "prop-types";
import classNames from 'classnames';
import { keysOf } from '../common';
import { icon as empty } from './assets/empty';
import { enqueueStateChange } from '../../services/react';
import { htmlIdGenerator } from '../../services';
import { colorToClassMap, isNamedColor } from './named_colors';
import { jsx as ___EmotionJSX } from "@emotion/react";
var typeToPathMap = {
  accessibility: 'accessibility',
  addDataApp: 'app_add_data',
  advancedSettingsApp: 'app_advanced_settings',
  agentApp: 'app_fleet',
  aggregate: 'aggregate',
  alert: 'alert',
  analyzeEvent: 'analyzeEvent',
  annotation: 'annotation',
  apmApp: 'app_apm',
  apmTrace: 'apm_trace',
  appSearchApp: 'app_app_search',
  apps: 'apps',
  arrowDown: 'arrow_down',
  arrowLeft: 'arrow_left',
  arrowRight: 'arrow_right',
  arrowUp: 'arrow_up',
  arrowStart: 'arrowStart',
  arrowEnd: 'arrowEnd',
  asterisk: 'asterisk',
  auditbeatApp: 'app_auditbeat',
  beaker: 'beaker',
  bell: 'bell',
  bellSlash: 'bellSlash',
  bolt: 'bolt',
  boxesHorizontal: 'boxes_horizontal',
  boxesVertical: 'boxes_vertical',
  branch: 'branch',
  broom: 'broom',
  brush: 'brush',
  bug: 'bug',
  bullseye: 'bullseye',
  calendar: 'calendar',
  canvasApp: 'app_canvas',
  check: 'check',
  checkInCircleFilled: 'checkInCircleFilled',
  cheer: 'cheer',
  classificationJob: 'ml_classification_job',
  clock: 'clock',
  cloudDrizzle: 'cloudDrizzle',
  cloudStormy: 'cloudStormy',
  cloudSunny: 'cloudSunny',
  codeApp: 'app_code',
  color: 'color',
  compute: 'compute',
  console: 'console',
  consoleApp: 'app_console',
  continuityAbove: 'continuityAbove',
  continuityAboveBelow: 'continuityAboveBelow',
  continuityBelow: 'continuityBelow',
  continuityWithin: 'continuityWithin',
  controlsHorizontal: 'controls_horizontal',
  controlsVertical: 'controls_vertical',
  copy: 'copy',
  copyClipboard: 'copy_clipboard',
  createAdvancedJob: 'ml_create_advanced_job',
  createMultiMetricJob: 'ml_create_multi_metric_job',
  createPopulationJob: 'ml_create_population_job',
  createSingleMetricJob: 'ml_create_single_metric_job',
  cross: 'cross',
  crossClusterReplicationApp: 'app_cross_cluster_replication',
  crossInACircleFilled: 'crossInACircleFilled',
  crosshairs: 'crosshairs',
  currency: 'currency',
  cut: 'cut',
  dashboardApp: 'app_dashboard',
  dataVisualizer: 'ml_data_visualizer',
  database: 'database',
  devToolsApp: 'app_devtools',
  discoverApp: 'app_discover',
  document: 'document',
  documentEdit: 'documentEdit',
  documentation: 'documentation',
  documents: 'documents',
  dot: 'dot',
  doubleArrowLeft: 'doubleArrowLeft',
  doubleArrowRight: 'doubleArrowRight',
  download: 'download',
  editorAlignCenter: 'editor_align_center',
  editorAlignLeft: 'editor_align_left',
  editorAlignRight: 'editor_align_right',
  editorBold: 'editor_bold',
  editorCodeBlock: 'editor_code_block',
  editorComment: 'editor_comment',
  editorDistributeHorizontal: 'editorDistributeHorizontal',
  editorDistributeVertical: 'editorDistributeVertical',
  editorHeading: 'editor_heading',
  editorItalic: 'editor_italic',
  editorItemAlignBottom: 'editorItemAlignBottom',
  editorItemAlignCenter: 'editorItemAlignCenter',
  editorItemAlignLeft: 'editorItemAlignLeft',
  editorItemAlignMiddle: 'editorItemAlignMiddle',
  editorItemAlignRight: 'editorItemAlignRight',
  editorItemAlignTop: 'editorItemAlignTop',
  editorLink: 'editor_link',
  editorOrderedList: 'editor_ordered_list',
  editorPositionBottomLeft: 'editorPositionBottomLeft',
  editorPositionBottomRight: 'editorPositionBottomRight',
  editorPositionTopLeft: 'editorPositionTopLeft',
  editorPositionTopRight: 'editorPositionTopRight',
  editorRedo: 'editor_redo',
  editorStrike: 'editor_strike',
  editorTable: 'editor_table',
  editorUnderline: 'editor_underline',
  editorUndo: 'editor_undo',
  editorUnorderedList: 'editor_unordered_list',
  email: 'email',
  empty: 'empty',
  emsApp: 'app_ems',
  eql: 'eql',
  eraser: 'eraser',
  exit: 'exit',
  expand: 'expand',
  expandMini: 'expandMini',
  exportAction: 'export',
  eye: 'eye',
  eyeClosed: 'eye_closed',
  faceHappy: 'face_happy',
  faceNeutral: 'face_neutral',
  faceSad: 'face_sad',
  filebeatApp: 'app_filebeat',
  filter: 'filter',
  flag: 'flag',
  fleetApp: 'app_agent',
  fold: 'fold',
  folderCheck: 'folder_check',
  folderClosed: 'folder_closed',
  folderExclamation: 'folder_exclamation',
  folderOpen: 'folder_open',
  frameNext: 'frameNext',
  framePrevious: 'framePrevious',
  fullScreen: 'full_screen',
  fullScreenExit: 'fullScreenExit',
  function: 'function',
  gear: 'gear',
  gisApp: 'app_gis',
  glasses: 'glasses',
  globe: 'globe',
  grab: 'grab',
  grabHorizontal: 'grab_horizontal',
  graphApp: 'app_graph',
  grid: 'grid',
  grokApp: 'app_grok',
  heart: 'heart',
  heartbeatApp: 'app_heartbeat',
  heatmap: 'heatmap',
  help: 'help',
  home: 'home',
  iInCircle: 'iInCircle',
  image: 'image',
  importAction: 'import',
  indexClose: 'index_close',
  indexEdit: 'index_edit',
  indexFlush: 'index_flush',
  indexManagementApp: 'app_index_management',
  indexMapping: 'index_mapping',
  indexOpen: 'index_open',
  indexPatternApp: 'app_index_pattern',
  indexRollupApp: 'app_index_rollup',
  indexRuntime: 'index_runtime',
  indexSettings: 'index_settings',
  inputOutput: 'inputOutput',
  inspect: 'inspect',
  invert: 'invert',
  ip: 'ip',
  keyboardShortcut: 'keyboard_shortcut',
  kqlField: 'kql_field',
  kqlFunction: 'kql_function',
  kqlOperand: 'kql_operand',
  kqlSelector: 'kql_selector',
  kqlValue: 'kql_value',
  layers: 'layers',
  lensApp: 'app_lens',
  lettering: 'lettering',
  link: 'link',
  list: 'list',
  listAdd: 'list_add',
  lock: 'lock',
  lockOpen: 'lockOpen',
  logoAWS: 'logo_aws',
  logoAWSMono: 'logo_aws_mono',
  logoAerospike: 'logo_aerospike',
  logoApache: 'logo_apache',
  logoAppSearch: 'logo_app_search',
  logoAzure: 'logo_azure',
  logoAzureMono: 'logo_azure_mono',
  logoBeats: 'logo_beats',
  logoBusinessAnalytics: 'logo_business_analytics',
  logoCeph: 'logo_ceph',
  logoCloud: 'logo_cloud',
  logoCloudEnterprise: 'logo_cloud_ece',
  logoCode: 'logo_code',
  logoCodesandbox: 'logo_codesandbox',
  logoCouchbase: 'logo_couchbase',
  logoDocker: 'logo_docker',
  logoDropwizard: 'logo_dropwizard',
  logoElastic: 'logo_elastic',
  logoElasticStack: 'logo_elastic_stack',
  logoElasticsearch: 'logo_elasticsearch',
  logoEnterpriseSearch: 'logo_enterprise_search',
  logoEtcd: 'logo_etcd',
  logoGCP: 'logo_gcp',
  logoGCPMono: 'logo_gcp_mono',
  logoGithub: 'logo_github',
  logoGmail: 'logo_gmail',
  logoGolang: 'logo_golang',
  logoGoogleG: 'logo_google_g',
  logoHAproxy: 'logo_haproxy',
  logoIBM: 'logo_ibm',
  logoIBMMono: 'logo_ibm_mono',
  logoKafka: 'logo_kafka',
  logoKibana: 'logo_kibana',
  logoKubernetes: 'logo_kubernetes',
  logoLogging: 'logo_logging',
  logoLogstash: 'logo_logstash',
  logoMaps: 'logo_maps',
  logoMemcached: 'logo_memcached',
  logoMetrics: 'logo_metrics',
  logoMongodb: 'logo_mongodb',
  logoMySQL: 'logo_mysql',
  logoNginx: 'logo_nginx',
  logoObservability: 'logo_observability',
  logoOsquery: 'logo_osquery',
  logoPhp: 'logo_php',
  logoPostgres: 'logo_postgres',
  logoPrometheus: 'logo_prometheus',
  logoRabbitmq: 'logo_rabbitmq',
  logoRedis: 'logo_redis',
  logoSecurity: 'logo_security',
  logoSiteSearch: 'logo_site_search',
  logoSketch: 'logo_sketch',
  logoSlack: 'logo_slack',
  logoUptime: 'logo_uptime',
  logoWebhook: 'logo_webhook',
  logoWindows: 'logo_windows',
  logoWorkplaceSearch: 'logo_workplace_search',
  logsApp: 'app_logs',
  logstashFilter: 'logstash_filter',
  logstashIf: 'logstash_if',
  logstashInput: 'logstash_input',
  logstashOutput: 'logstash_output',
  logstashQueue: 'logstash_queue',
  machineLearningApp: 'app_ml',
  magnet: 'magnet',
  magnifyWithExclamation: 'magnifyWithExclamation',
  magnifyWithMinus: 'magnifyWithMinus',
  magnifyWithPlus: 'magnifyWithPlus',
  managementApp: 'app_management',
  mapMarker: 'map_marker',
  memory: 'memory',
  menu: 'menu',
  menuDown: 'menuDown',
  menuLeft: 'menuLeft',
  menuRight: 'menuRight',
  menuUp: 'menuUp',
  merge: 'merge',
  metricbeatApp: 'app_metricbeat',
  metricsApp: 'app_metrics',
  minimize: 'minimize',
  minus: 'minus',
  minusInCircle: 'minus_in_circle',
  minusInCircleFilled: 'minus_in_circle_filled',
  mobile: 'mobile',
  monitoringApp: 'app_monitoring',
  moon: 'moon',
  nested: 'nested',
  node: 'node',
  notebookApp: 'app_notebook',
  number: 'number',
  offline: 'offline',
  online: 'online',
  outlierDetectionJob: 'ml_outlier_detection_job',
  package: 'package',
  packetbeatApp: 'app_packetbeat',
  pageSelect: 'pageSelect',
  pagesSelect: 'pagesSelect',
  paperClip: 'paper_clip',
  partial: 'partial',
  pause: 'pause',
  payment: 'payment',
  pencil: 'pencil',
  percent: 'percent',
  pin: 'pin',
  pinFilled: 'pin_filled',
  pipelineApp: 'app_pipeline',
  play: 'play',
  playFilled: 'playFilled',
  plus: 'plus',
  plusInCircle: 'plus_in_circle',
  plusInCircleFilled: 'plus_in_circle_filled',
  popout: 'popout',
  push: 'push',
  questionInCircle: 'question_in_circle',
  quote: 'quote',
  recentlyViewedApp: 'app_recently_viewed',
  refresh: 'refresh',
  regressionJob: 'ml_regression_job',
  reporter: 'reporter',
  reportingApp: 'app_reporting',
  returnKey: 'return_key',
  save: 'save',
  savedObjectsApp: 'app_saved_objects',
  scale: 'scale',
  search: 'search',
  searchProfilerApp: 'app_search_profiler',
  securityAnalyticsApp: 'app_security_analytics',
  securityApp: 'app_security',
  securitySignal: 'securitySignal',
  securitySignalDetected: 'securitySignalDetected',
  securitySignalResolved: 'securitySignalResolved',
  shard: 'shard',
  share: 'share',
  snowflake: 'snowflake',
  sortDown: 'sort_down',
  sortLeft: 'sortLeft',
  sortRight: 'sortRight',
  sortUp: 'sort_up',
  sortable: 'sortable',
  spacesApp: 'app_spaces',
  sqlApp: 'app_sql',
  starEmpty: 'star_empty',
  starEmptySpace: 'star_empty_space',
  starFilled: 'star_filled',
  starFilledSpace: 'star_filled_space',
  starMinusEmpty: 'star_minus_empty',
  starMinusFilled: 'star_minus_filled',
  starPlusEmpty: 'starPlusEmpty',
  starPlusFilled: 'starPlusFilled',
  stats: 'stats',
  stop: 'stop',
  stopFilled: 'stop_filled',
  stopSlash: 'stop_slash',
  storage: 'storage',
  string: 'string',
  submodule: 'submodule',
  sun: 'sun',
  swatchInput: 'swatch_input',
  // Undocumented on purpose. Has an extra stroke for EuiColorPicker
  symlink: 'symlink',
  tableDensityCompact: 'table_density_compact',
  tableDensityExpanded: 'table_density_expanded',
  tableDensityNormal: 'table_density_normal',
  tableOfContents: 'tableOfContents',
  tag: 'tag',
  tear: 'tear',
  temperature: 'temperature',
  timeline: 'timeline',
  timelionApp: 'app_timelion',
  timeRefresh: 'timeRefresh',
  timeslider: 'timeslider',
  training: 'training',
  trash: 'trash',
  unfold: 'unfold',
  unlink: 'unlink',
  upgradeAssistantApp: 'app_upgrade_assistant',
  uptimeApp: 'app_uptime',
  user: 'user',
  users: 'users',
  usersRolesApp: 'app_users_roles',
  vector: 'vector',
  videoPlayer: 'videoPlayer',
  visArea: 'vis_area',
  visAreaStacked: 'vis_area_stacked',
  visBarHorizontal: 'vis_bar_horizontal',
  visBarHorizontalStacked: 'vis_bar_horizontal_stacked',
  visBarVertical: 'vis_bar_vertical',
  visBarVerticalStacked: 'vis_bar_vertical_stacked',
  visGauge: 'vis_gauge',
  visGoal: 'vis_goal',
  visLine: 'vis_line',
  visMapCoordinate: 'vis_map_coordinate',
  visMapRegion: 'vis_map_region',
  visMetric: 'vis_metric',
  visPie: 'vis_pie',
  visTable: 'vis_table',
  visTagCloud: 'vis_tag_cloud',
  visText: 'vis_text',
  visTimelion: 'vis_timelion',
  visVega: 'vis_vega',
  visVisualBuilder: 'vis_visual_builder',
  visualizeApp: 'app_visualize',
  watchesApp: 'app_watches',
  wordWrap: 'wordWrap',
  wordWrapDisabled: 'wordWrapDisabled',
  workplaceSearchApp: 'app_workplace_search',
  wrench: 'wrench',
  // Token Icon Imports
  tokenClass: 'tokenClass',
  tokenProperty: 'tokenProperty',
  tokenEnum: 'tokenEnum',
  tokenVariable: 'tokenVariable',
  tokenMethod: 'tokenMethod',
  tokenAnnotation: 'tokenAnnotation',
  tokenException: 'tokenException',
  tokenInterface: 'tokenInterface',
  tokenParameter: 'tokenParameter',
  tokenField: 'tokenField',
  tokenElement: 'tokenElement',
  tokenFunction: 'tokenFunction',
  tokenBoolean: 'tokenBoolean',
  tokenString: 'tokenString',
  tokenArray: 'tokenArray',
  tokenNumber: 'tokenNumber',
  tokenConstant: 'tokenConstant',
  tokenObject: 'tokenObject',
  tokenEvent: 'tokenEvent',
  tokenKey: 'tokenKey',
  tokenNull: 'tokenNull',
  tokenStruct: 'tokenStruct',
  tokenPackage: 'tokenPackage',
  tokenOperator: 'tokenOperator',
  tokenEnumMember: 'tokenEnumMember',
  tokenRepo: 'tokenRepo',
  tokenSymbol: 'tokenSymbol',
  tokenFile: 'tokenFile',
  tokenModule: 'tokenModule',
  tokenNamespace: 'tokenNamespace',
  tokenDate: 'tokenDate',
  tokenIP: 'tokenIP',
  tokenNested: 'tokenNested',
  tokenAlias: 'tokenAlias',
  tokenShape: 'tokenShape',
  tokenGeo: 'tokenGeo',
  tokenRange: 'tokenRange',
  tokenBinary: 'tokenBinary',
  tokenJoin: 'tokenJoin',
  tokenPercolator: 'tokenPercolator',
  tokenFlattened: 'tokenFlattened',
  tokenRankFeature: 'tokenRankFeature',
  tokenRankFeatures: 'tokenRankFeatures',
  tokenKeyword: 'tokenKeyword',
  tokenTag: 'tokenTag',
  tokenCompletionSuggester: 'tokenCompletionSuggester',
  tokenDenseVector: 'tokenDenseVector',
  tokenText: 'tokenText',
  tokenTokenCount: 'tokenTokenCount',
  tokenSearchType: 'tokenSearchType',
  tokenHistogram: 'tokenHistogram'
};
export var TYPES = keysOf(typeToPathMap);
export var COLORS = keysOf(colorToClassMap); // We accept arbitrary color strings, which are impossible to type.

var sizeToClassNameMap = {
  original: null,
  s: 'euiIcon--small',
  m: 'euiIcon--medium',
  l: 'euiIcon--large',
  xl: 'euiIcon--xLarge',
  xxl: 'euiIcon--xxLarge'
};
export var SIZES = keysOf(sizeToClassNameMap);

function isEuiIconType(x) {
  return typeof x === 'string' && typeToPathMap.hasOwnProperty(x);
}

function getInitialIcon(icon) {
  if (icon == null) {
    return undefined;
  }

  if (isEuiIconType(icon)) {
    if (iconComponentCache.hasOwnProperty(icon)) {
      return iconComponentCache[icon];
    }

    return undefined;
  }

  return icon;
}

var generateId = htmlIdGenerator();
var iconComponentCache = {};
export var clearIconComponentCache = function clearIconComponentCache(iconType) {
  if (iconType != null) {
    delete iconComponentCache[iconType];
  } else {
    iconComponentCache = {};
  }
};
export var appendIconComponentCache = function appendIconComponentCache(iconTypeToIconComponentMap) {
  for (var _iconType in iconTypeToIconComponentMap) {
    if (iconTypeToIconComponentMap.hasOwnProperty(_iconType)) {
      iconComponentCache[_iconType] = iconTypeToIconComponentMap[_iconType];
    }
  }
};
export var EuiIcon = /*#__PURE__*/function (_PureComponent) {
  _inherits(EuiIcon, _PureComponent);

  var _super = _createSuper(EuiIcon);

  function EuiIcon(props) {
    var _this;

    _classCallCheck(this, EuiIcon);

    _this = _super.call(this, props);

    _defineProperty(_assertThisInitialized(_this), "isMounted", true);

    _defineProperty(_assertThisInitialized(_this), "loadIconComponent", function (iconType) {
      if (iconComponentCache.hasOwnProperty(iconType)) {
        // exists in cache
        _this.setState({
          isLoading: false,
          neededLoading: false,
          icon: iconComponentCache[iconType]
        });

        _this.onIconLoad();

        return;
      }

      import(
      /* webpackChunkName: "icon.[request]" */
      // It's important that we don't use a template string here, it
      // stops webpack from building a dynamic require context.
      // eslint-disable-next-line prefer-template
      './assets/' + typeToPathMap[iconType]).then(function (_ref) {
        var icon = _ref.icon;
        iconComponentCache[iconType] = icon;
        enqueueStateChange(function () {
          if (_this.isMounted && _this.props.type === iconType) {
            _this.setState({
              icon: icon,
              iconTitle: iconType,
              isLoading: false
            }, _this.onIconLoad);
          }
        });
      });
    });

    _defineProperty(_assertThisInitialized(_this), "onIconLoad", function () {
      var onIconLoad = _this.props.onIconLoad;

      if (onIconLoad) {
        onIconLoad();
      }
    });

    var type = props.type;
    var initialIcon = getInitialIcon(type);
    var isLoading = false;

    if (isEuiIconType(type) && initialIcon == null) {
      isLoading = true;

      _this.loadIconComponent(type);
    } else {
      _this.onIconLoad();
    }

    _this.state = {
      icon: initialIcon,
      iconTitle: undefined,
      isLoading: isLoading,
      neededLoading: isLoading
    };
    return _this;
  }

  _createClass(EuiIcon, [{
    key: "componentDidUpdate",
    value: function componentDidUpdate(prevProps) {
      var type = this.props.type;

      if (type !== prevProps.type) {
        if (isEuiIconType(type)) {
          // eslint-disable-next-line react/no-did-update-set-state
          this.setState({
            neededLoading: iconComponentCache.hasOwnProperty(type),
            isLoading: true
          });
          this.loadIconComponent(type);
        } else {
          // eslint-disable-next-line react/no-did-update-set-state
          this.setState({
            icon: type,
            neededLoading: true,
            isLoading: false
          });
        }
      }
    }
  }, {
    key: "componentWillUnmount",
    value: function componentWillUnmount() {
      this.isMounted = false;
    }
  }, {
    key: "render",
    value: function render() {
      var _this$props = this.props,
          type = _this$props.type,
          _this$props$size = _this$props.size,
          size = _this$props$size === void 0 ? 'm' : _this$props$size,
          color = _this$props.color,
          className = _this$props.className,
          tabIndex = _this$props.tabIndex,
          title = _this$props.title,
          onIconLoad = _this$props.onIconLoad,
          rest = _objectWithoutProperties(_this$props, ["type", "size", "color", "className", "tabIndex", "title", "onIconLoad"]);

      var _this$state = this.state,
          isLoading = _this$state.isLoading,
          neededLoading = _this$state.neededLoading;
      var optionalColorClass = null;
      var optionalCustomStyles = null;

      if (color) {
        if (isNamedColor(color)) {
          optionalColorClass = colorToClassMap[color];
        } else {
          optionalCustomStyles = {
            color: color
          };
          optionalColorClass = 'euiIcon--customColor';
        }
      } // These icons are a little special and get some extra CSS flexibility


      var isAppIcon = type && typeof type === 'string' && (/.+App$/.test(type) || /.+Job$/.test(type) || type === 'dataVisualizer');
      var appIconHasColor = color && color !== 'default'; // parent is not one of

      var classes = classNames('euiIcon', sizeToClassNameMap[size], optionalColorClass, {
        // The app icon only gets the .euiIcon--app class if no color is passed or if color="default" is passed
        'euiIcon--app': isAppIcon && !appIconHasColor,
        'euiIcon-isLoading': isLoading,
        'euiIcon-isLoaded': !isLoading && neededLoading
      }, className);
      var icon = this.state.icon || empty; // This is a fix for IE and Edge, which ignores tabindex="-1" on an SVG, but respects
      // focusable="false".
      //   - If there's no tabindex specified, we'll default the icon to not be focusable,
      //     which is how SVGs behave in Chrome, Safari, and FF.
      //   - If tabindex is -1, then the consumer wants the icon to be focusable by JavaScript only.
      //   - If the tabindex is 0, the consumer wants the icon to be keyboard focusable.

      var focusable = tabIndex == null || tabIndex === -1 ? 'false' : 'true';

      if (typeof icon === 'string') {
        return ___EmotionJSX("img", _extends({
          alt: title ? title : '',
          src: icon,
          className: classes,
          tabIndex: tabIndex
        }, rest));
      } else {
        var Svg = icon; // If it's an empty icon, or if there is no aria-label, aria-labelledby, or title it gets aria-hidden true

        var isAriaHidden = icon === empty || !(this.props['aria-label'] || this.props['aria-labelledby'] || this.props.title);
        var hideIconEmpty = isAriaHidden && {
          'aria-hidden': true
        };
        var titleId; // If no aria-label or aria-labelledby is provided but there's a title, a titleId is generated
        //  The svg aria-labelledby attribute gets this titleId
        //  The svg title element gets this titleId as an id

        if (!this.props['aria-label'] && !this.props['aria-labelledby'] && title) {
          titleId = {
            titleId: generateId()
          };
        }

        return ___EmotionJSX(Svg, _extends({
          className: classes,
          style: optionalCustomStyles,
          tabIndex: tabIndex,
          focusable: focusable,
          role: "img",
          title: title,
          "data-icon-type": this.state.iconTitle
        }, titleId, rest, hideIconEmpty));
      }
    }
  }]);

  return EuiIcon;
}(PureComponent);
EuiIcon.propTypes = {
  className: PropTypes.string,
  "aria-label": PropTypes.string,
  "data-test-subj": PropTypes.string,

  /**
       * `Enum` is any of the named icons listed in the docs, `string` is usually a URL to an SVG file, and `elementType` is any React SVG component
       */
  type: PropTypes.oneOfType([PropTypes.oneOf(["accessibility", "addDataApp", "advancedSettingsApp", "agentApp", "aggregate", "alert", "analyzeEvent", "annotation", "apmApp", "apmTrace", "appSearchApp", "apps", "arrowDown", "arrowLeft", "arrowRight", "arrowUp", "arrowStart", "arrowEnd", "asterisk", "auditbeatApp", "beaker", "bell", "bellSlash", "bolt", "boxesHorizontal", "boxesVertical", "branch", "broom", "brush", "bug", "bullseye", "calendar", "canvasApp", "check", "checkInCircleFilled", "cheer", "classificationJob", "clock", "cloudDrizzle", "cloudStormy", "cloudSunny", "codeApp", "color", "compute", "console", "consoleApp", "continuityAbove", "continuityAboveBelow", "continuityBelow", "continuityWithin", "controlsHorizontal", "controlsVertical", "copy", "copyClipboard", "createAdvancedJob", "createMultiMetricJob", "createPopulationJob", "createSingleMetricJob", "cross", "crossClusterReplicationApp", "crossInACircleFilled", "crosshairs", "currency", "cut", "dashboardApp", "dataVisualizer", "database", "devToolsApp", "discoverApp", "document", "documentEdit", "documentation", "documents", "dot", "doubleArrowLeft", "doubleArrowRight", "download", "editorAlignCenter", "editorAlignLeft", "editorAlignRight", "editorBold", "editorCodeBlock", "editorComment", "editorDistributeHorizontal", "editorDistributeVertical", "editorHeading", "editorItalic", "editorItemAlignBottom", "editorItemAlignCenter", "editorItemAlignLeft", "editorItemAlignMiddle", "editorItemAlignRight", "editorItemAlignTop", "editorLink", "editorOrderedList", "editorPositionBottomLeft", "editorPositionBottomRight", "editorPositionTopLeft", "editorPositionTopRight", "editorRedo", "editorStrike", "editorTable", "editorUnderline", "editorUndo", "editorUnorderedList", "email", "empty", "emsApp", "eql", "eraser", "exit", "expand", "expandMini", "exportAction", "eye", "eyeClosed", "faceHappy", "faceNeutral", "faceSad", "filebeatApp", "filter", "flag", "fleetApp", "fold", "folderCheck", "folderClosed", "folderExclamation", "folderOpen", "frameNext", "framePrevious", "fullScreen", "fullScreenExit", "function", "gear", "gisApp", "glasses", "globe", "grab", "grabHorizontal", "graphApp", "grid", "grokApp", "heart", "heartbeatApp", "heatmap", "help", "home", "iInCircle", "image", "importAction", "indexClose", "indexEdit", "indexFlush", "indexManagementApp", "indexMapping", "indexOpen", "indexPatternApp", "indexRollupApp", "indexRuntime", "indexSettings", "inputOutput", "inspect", "invert", "ip", "keyboardShortcut", "kqlField", "kqlFunction", "kqlOperand", "kqlSelector", "kqlValue", "layers", "lensApp", "lettering", "link", "list", "listAdd", "lock", "lockOpen", "logoAWS", "logoAWSMono", "logoAerospike", "logoApache", "logoAppSearch", "logoAzure", "logoAzureMono", "logoBeats", "logoBusinessAnalytics", "logoCeph", "logoCloud", "logoCloudEnterprise", "logoCode", "logoCodesandbox", "logoCouchbase", "logoDocker", "logoDropwizard", "logoElastic", "logoElasticStack", "logoElasticsearch", "logoEnterpriseSearch", "logoEtcd", "logoGCP", "logoGCPMono", "logoGithub", "logoGmail", "logoGolang", "logoGoogleG", "logoHAproxy", "logoIBM", "logoIBMMono", "logoKafka", "logoKibana", "logoKubernetes", "logoLogging", "logoLogstash", "logoMaps", "logoMemcached", "logoMetrics", "logoMongodb", "logoMySQL", "logoNginx", "logoObservability", "logoOsquery", "logoPhp", "logoPostgres", "logoPrometheus", "logoRabbitmq", "logoRedis", "logoSecurity", "logoSiteSearch", "logoSketch", "logoSlack", "logoUptime", "logoWebhook", "logoWindows", "logoWorkplaceSearch", "logsApp", "logstashFilter", "logstashIf", "logstashInput", "logstashOutput", "logstashQueue", "machineLearningApp", "magnet", "magnifyWithExclamation", "magnifyWithMinus", "magnifyWithPlus", "managementApp", "mapMarker", "memory", "menu", "menuDown", "menuLeft", "menuRight", "menuUp", "merge", "metricbeatApp", "metricsApp", "minimize", "minus", "minusInCircle", "minusInCircleFilled", "mobile", "monitoringApp", "moon", "nested", "node", "notebookApp", "number", "offline", "online", "outlierDetectionJob", "package", "packetbeatApp", "pageSelect", "pagesSelect", "paperClip", "partial", "pause", "payment", "pencil", "percent", "pin", "pinFilled", "pipelineApp", "play", "playFilled", "plus", "plusInCircle", "plusInCircleFilled", "popout", "push", "questionInCircle", "quote", "recentlyViewedApp", "refresh", "regressionJob", "reporter", "reportingApp", "returnKey", "save", "savedObjectsApp", "scale", "search", "searchProfilerApp", "securityAnalyticsApp", "securityApp", "securitySignal", "securitySignalDetected", "securitySignalResolved", "shard", "share", "snowflake", "sortDown", "sortLeft", "sortRight", "sortUp", "sortable", "spacesApp", "sqlApp", "starEmpty", "starEmptySpace", "starFilled", "starFilledSpace", "starMinusEmpty", "starMinusFilled", "starPlusEmpty", "starPlusFilled", "stats", "stop", "stopFilled", "stopSlash", "storage", "string", "submodule", "sun", "swatchInput", "symlink", "tableDensityCompact", "tableDensityExpanded", "tableDensityNormal", "tableOfContents", "tag", "tear", "temperature", "timeline", "timelionApp", "timeRefresh", "timeslider", "training", "trash", "unfold", "unlink", "upgradeAssistantApp", "uptimeApp", "user", "users", "usersRolesApp", "vector", "videoPlayer", "visArea", "visAreaStacked", "visBarHorizontal", "visBarHorizontalStacked", "visBarVertical", "visBarVerticalStacked", "visGauge", "visGoal", "visLine", "visMapCoordinate", "visMapRegion", "visMetric", "visPie", "visTable", "visTagCloud", "visText", "visTimelion", "visVega", "visVisualBuilder", "visualizeApp", "watchesApp", "wordWrap", "wordWrapDisabled", "workplaceSearchApp", "wrench", "tokenClass", "tokenProperty", "tokenEnum", "tokenVariable", "tokenMethod", "tokenAnnotation", "tokenException", "tokenInterface", "tokenParameter", "tokenField", "tokenElement", "tokenFunction", "tokenBoolean", "tokenString", "tokenArray", "tokenNumber", "tokenConstant", "tokenObject", "tokenEvent", "tokenKey", "tokenNull", "tokenStruct", "tokenPackage", "tokenOperator", "tokenEnumMember", "tokenRepo", "tokenSymbol", "tokenFile", "tokenModule", "tokenNamespace", "tokenDate", "tokenIP", "tokenNested", "tokenAlias", "tokenShape", "tokenGeo", "tokenRange", "tokenBinary", "tokenJoin", "tokenPercolator", "tokenFlattened", "tokenRankFeature", "tokenRankFeatures", "tokenKeyword", "tokenTag", "tokenCompletionSuggester", "tokenDenseVector", "tokenText", "tokenTokenCount", "tokenSearchType", "tokenHistogram"]).isRequired, PropTypes.string.isRequired, PropTypes.elementType.isRequired]).isRequired,

  /**
       * One of EUI's color palette or a valid CSS color value https://developer.mozilla.org/en-US/docs/Web/CSS/color_value.
       * Note that coloring only works if your SVG is removed of fill attributes.
       */
  color: PropTypes.oneOfType([PropTypes.string.isRequired, PropTypes.oneOf(["default", "primary", "success", "accent", "warning", "danger", "text", "subdued", "ghost", "inherit"]).isRequired]),

  /**
       * Note that every size other than `original` assumes the provided SVG sits on a square viewbox.
       */
  size: PropTypes.oneOf(["original", "s", "m", "l", "xl", "xxl"]),

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
};