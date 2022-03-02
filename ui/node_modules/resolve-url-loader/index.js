/*
 * MIT License http://opensource.org/licenses/MIT
 * Author: Ben Holloway @bholloway
 */
'use strict';

var os                = require('os'),
    path              = require('path'),
    fs                = require('fs'),
    util              = require('util'),
    loaderUtils       = require('loader-utils'),
    SourceMapConsumer = require('source-map').SourceMapConsumer;

var adjustSourceMap = require('adjust-sourcemap-loader/lib/process');

var valueProcessor   = require('./lib/value-processor'),
    joinFn           = require('./lib/join-function'),
    logToTestHarness = require('./lib/log-to-test-harness');

const DEPRECATED_OPTIONS = {
  engine: [
    'DEP_RESOLVE_URL_LOADER_OPTION_ENGINE',
    'the "engine" option is deprecated, "postcss" engine is the default, using "rework" engine is not advised'
  ],
  keepQuery: [
    'DEP_RESOLVE_URL_LOADER_OPTION_KEEP_QUERY',
    '"keepQuery" option has been removed, the query and/or hash are now always retained'
  ],
  absolute: [
    'DEP_RESOLVE_URL_LOADER_OPTION_ABSOLUTE',
    '"absolute" option has been removed, consider the "join" option if absolute paths must be processed'
  ],
  attempts: [
    'DEP_RESOLVE_URL_LOADER_OPTION_ATTEMPTS',
    '"attempts" option has been removed, consider the "join" option if search is needed'
  ],
  includeRoot: [
    'DEP_RESOLVE_URL_LOADER_OPTION_INCLUDE_ROOT',
    '"includeRoot" option has been removed, consider the "join" option if search is needed'
  ],
  fail: [
    'DEP_RESOLVE_URL_LOADER_OPTION_FAIL',
    '"fail" option has been removed'
  ]
};

/**
 * A webpack loader that resolves absolute url() paths relative to their original source file.
 * Requires source-maps to do any meaningful work.
 * @param {string} content Css content
 * @param {object} sourceMap The source-map
 * @returns {string|String}
 */
function resolveUrlLoader(content, sourceMap) {
  /* jshint validthis:true */

  // details of the file being processed
  var loader = this;

  // a relative loader.context is a problem
  if (/^\./.test(loader.context)) {
    return handleAsError(
      'webpack misconfiguration',
      'loader.context is relative, expected absolute'
    );
  }

  // infer webpack version from new loader features
  var isWebpackGte5 = 'getOptions' in loader && typeof loader.getOptions === 'function';

  // webpack 1: prefer loader query, else options object
  // webpack 2: prefer loader options
  // webpack 3: deprecate loader.options object
  // webpack 4: loader.options no longer defined
  var rawOptions = loaderUtils.getOptions(loader),
      options    = Object.assign(
        {
          sourceMap: loader.sourceMap,
          engine   : 'postcss',
          silent   : false,
          removeCR : os.EOL.includes('\r'),
          root     : false,
          debug    : false,
          join     : joinFn.defaultJoin
        },
        rawOptions
      );

  // maybe log options for the test harness
  if (process.env.RESOLVE_URL_LOADER_TEST_HARNESS) {
    logToTestHarness(
      process[process.env.RESOLVE_URL_LOADER_TEST_HARNESS],
      options
    );
  }

  // deprecated options
  var deprecatedItems = Object.entries(DEPRECATED_OPTIONS).filter(([key]) => key in rawOptions);
  if (deprecatedItems.length) {
    deprecatedItems.forEach(([, value]) => handleAsDeprecated(...value));
  }

  // validate join option
  if (typeof options.join !== 'function') {
    return handleAsError(
      'loader misconfiguration',
      '"join" option must be a Function'
    );
  } else if (options.join.length !== 2) {
    return handleAsError(
      'loader misconfiguration',
      '"join" Function must take exactly 2 arguments (options, loader)'
    );
  }

  // validate the result of calling the join option
  var joinProper = options.join(options, loader);
  if (typeof joinProper !== 'function') {
    return handleAsError(
      'loader misconfiguration',
      '"join" option must itself return a Function when it is called'
    );
  } else if (joinProper.length !== 1) {
    return handleAsError(
      'loader misconfiguration',
      '"join" Function must create a function that takes exactly 1 arguments (item)'
    );
  }

  // validate root option
  if (typeof options.root === 'string') {
    var isValid = (options.root === '') ||
      (path.isAbsolute(options.root) && fs.existsSync(options.root) && fs.statSync(options.root).isDirectory());

    if (!isValid) {
      return handleAsError(
        'loader misconfiguration',
        '"root" option must be an empty string or an absolute path to an existing directory'
      );
    }
  } else if (options.root !== false) {
    handleAsWarning(
      'loader misconfiguration',
      '"root" option must be string where used or false where unused'
    );
  }

  // loader result is cacheable
  loader.cacheable();

  // incoming source-map
  var sourceMapConsumer, absSourceMap;
  if (sourceMap) {

    // support non-standard string encoded source-map (per less-loader)
    if (typeof sourceMap === 'string') {
      try {
        sourceMap = JSON.parse(sourceMap);
      }
      catch (exception) {
        return handleAsError(
          'source-map error',
          'cannot parse source-map string (from less-loader?)'
        );
      }
    }

    // leverage adjust-sourcemap-loader's codecs to avoid having to make any assumptions about the sourcemap
    //  historically this is a regular source of breakage
    try {
      absSourceMap = adjustSourceMap(loader, {format: 'absolute'}, sourceMap);
    }
    catch (exception) {
      return handleAsError(
        'source-map error',
        exception.message
      );
    }

    // prepare the adjusted sass source-map for later look-ups
    sourceMapConsumer = new SourceMapConsumer(absSourceMap);
  } else {
    handleAsWarning(
      'webpack misconfiguration',
      'webpack or the upstream loader did not supply a source-map'
    );
  }

  // choose a CSS engine
  var enginePath    = /^[\w-]+$/.test(options.engine) && path.join(__dirname, 'lib', 'engine', options.engine + '.js');
  var isValidEngine = fs.existsSync(enginePath);
  if (!isValidEngine) {
    return handleAsError(
      'loader misconfiguration',
      '"engine" option is not valid'
    );
  }

  // allow engine to throw at initialisation
  var engine;
  try {
    engine = require(enginePath);
  } catch (error) {
    return handleAsError(
      'error initialising',
      error
    );
  }

  // process async
  var callback = loader.async();
  Promise
    .resolve(engine(loader.resourcePath, content, {
      outputSourceMap     : !!options.sourceMap,
      absSourceMap        : absSourceMap,
      sourceMapConsumer   : sourceMapConsumer,
      removeCR            : options.removeCR,
      transformDeclaration: valueProcessor({
        join     : joinProper,
        root     : options.root,
        directory: path.dirname(loader.resourcePath)
      })
    }))
    .catch(onFailure)
    .then(onSuccess);

  function onFailure(error) {
    callback(encodeError('error processing CSS', error));
  }

  function onSuccess(result) {
    if (result) {
      // complete with source-map
      //  webpack4 and earlier: source-map sources are relative to the file being processed
      //  webpack5: source-map sources are relative to the project root but without a leading slash
      if (options.sourceMap) {
        var finalMap = adjustSourceMap(loader, {
          format: isWebpackGte5 ? 'projectRelative' : 'sourceRelative'
        }, result.map);
        callback(null, result.content, finalMap);
      }
      // complete without source-map
      else {
        callback(null, result.content);
      }
    }
  }

  /**
   * Trigger a node deprecation message for the given exception and return the original content.
   * @param {string} code Deprecation code
   * @param {string} message Deprecation message
   * @returns {string} The original CSS content
   */
  function handleAsDeprecated(code, message) {
    if (!options.silent) {
      util.deprecate(() => undefined, message, code)();
    }
    return content;
  }

  /**
   * Push a warning for the given exception and return the original content.
   * @param {string} label Summary of the error
   * @param {string|Error} [exception] Optional extended error details
   * @returns {string} The original CSS content
   */
  function handleAsWarning(label, exception) {
    if (!options.silent) {
      loader.emitWarning(encodeError(label, exception));
    }
    return content;
  }

  /**
   * Push a warning for the given exception and return the original content.
   * @param {string} label Summary of the error
   * @param {string|Error} [exception] Optional extended error details
   * @returns {string} The original CSS content
   */
  function handleAsError(label, exception) {
    loader.emitError(encodeError(label, exception));
    return content;
  }

  function encodeError(label, exception) {
    return new Error(
      [
        'resolve-url-loader',
        ': ',
        [label]
          .concat(
            (typeof exception === 'string') && exception ||
            Array.isArray(exception) && exception ||
            (exception instanceof Error) && [exception.message, exception.stack.split('\n')[1].trim()] ||
            []
          )
          .filter(Boolean)
          .join('\n  ')
      ].join('')
    );
  }
}

module.exports = Object.assign(resolveUrlLoader, joinFn);
