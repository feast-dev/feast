"use strict";

var _interopRequireDefault = require("@babel/runtime/helpers/interopRequireDefault");

Object.defineProperty(exports, "__esModule", {
  value: true
});
exports.astToEsQueryDsl = exports._isFlagToQuery = exports._fieldValuesToQuery = exports._termValuesToQuery = void 0;

var _toConsumableArray2 = _interopRequireDefault(require("@babel/runtime/helpers/toConsumableArray"));

var _defineProperty2 = _interopRequireDefault(require("@babel/runtime/helpers/defineProperty"));

var _date_format = require("./date_format");

var _date_value = require("./date_value");

var _ast = require("./ast");

var _predicate = require("../../../services/predicate");

var _common = require("../../common");

function _createForOfIteratorHelper(o, allowArrayLike) { var it; if (typeof Symbol === "undefined" || o[Symbol.iterator] == null) { if (Array.isArray(o) || (it = _unsupportedIterableToArray(o)) || allowArrayLike && o && typeof o.length === "number") { if (it) o = it; var i = 0; var F = function F() {}; return { s: F, n: function n() { if (i >= o.length) return { done: true }; return { done: false, value: o[i++] }; }, e: function e(_e) { throw _e; }, f: F }; } throw new TypeError("Invalid attempt to iterate non-iterable instance.\nIn order to be iterable, non-array objects must have a [Symbol.iterator]() method."); } var normalCompletion = true, didErr = false, err; return { s: function s() { it = o[Symbol.iterator](); }, n: function n() { var step = it.next(); normalCompletion = step.done; return step; }, e: function e(_e2) { didErr = true; err = _e2; }, f: function f() { try { if (!normalCompletion && it.return != null) it.return(); } finally { if (didErr) throw err; } } }; }

function _unsupportedIterableToArray(o, minLen) { if (!o) return; if (typeof o === "string") return _arrayLikeToArray(o, minLen); var n = Object.prototype.toString.call(o).slice(8, -1); if (n === "Object" && o.constructor) n = o.constructor.name; if (n === "Map" || n === "Set") return Array.from(o); if (n === "Arguments" || /^(?:Ui|I)nt(?:8|16|32)(?:Clamped)?Array$/.test(n)) return _arrayLikeToArray(o, minLen); }

function _arrayLikeToArray(arr, len) { if (len == null || len > arr.length) len = arr.length; for (var i = 0, arr2 = new Array(len); i < len; i++) { arr2[i] = arr[i]; } return arr2; }

var processDateOperation = function processDateOperation(value, operator) {
  var granularity = value.granularity,
      resolve = value.resolve;
  var expression = (0, _date_format.printIso8601)(resolve());

  if (!granularity) {
    return {
      operator: operator,
      expression: expression
    };
  }

  switch (operator) {
    case _ast.AST.Operator.GT:
      expression = "".concat(expression, "||+1").concat(granularity.es, "/").concat(granularity.es);
      return {
        operator: _ast.AST.Operator.GTE,
        expression: expression
      };

    case _ast.AST.Operator.GTE:
      expression = "".concat(expression, "||/").concat(granularity.es);
      return {
        operator: operator,
        expression: expression
      };

    case _ast.AST.Operator.LT:
      expression = "".concat(expression, "||/").concat(granularity.es);
      return {
        operator: operator,
        expression: expression
      };

    case _ast.AST.Operator.LTE:
      expression = "".concat(expression, "||+1").concat(granularity.es, "/").concat(granularity.es);
      return {
        operator: _ast.AST.Operator.LT,
        expression: expression
      };

    default:
      expression = "".concat(expression, "||/").concat(granularity.es);
      return {
        expression: expression
      };
  }
};

var _termValuesToQuery = function _termValuesToQuery(values, options) {
  var body = {
    query: values.join(' ')
  };

  if (body.query === '') {
    return;
  }

  if (options.defaultFields) {
    body.fields = options.defaultFields;
  }

  return {
    simple_query_string: body
  };
};

exports._termValuesToQuery = _termValuesToQuery;

var _fieldValuesToQuery = function _fieldValuesToQuery(field, operations, andOr) {
  var queries = [];
  (0, _common.keysOf)(operations).forEach(function (operator) {
    var values = operations[operator];

    switch (operator) {
      case _ast.AST.Operator.EQ:
        var _terms = [];
        var phrases = [];
        var dates = [];
        values.forEach(function (value) {
          if ((0, _date_value.isDateValue)(value)) {
            dates.push(value);
          } else if ((0, _predicate.isDateLike)(value)) {
            dates.push((0, _date_value.dateValue)(value));
          } else if ((0, _predicate.isString)(value) && value.match(/\s/)) {
            phrases.push(value);
          } else {
            _terms.push(value);
          }
        });

        if (_terms.length > 0) {
          queries.push({
            match: (0, _defineProperty2.default)({}, field, {
              query: _terms.join(' '),
              operator: andOr
            })
          });
        }

        if (phrases.length > 0) {
          queries.push.apply(queries, (0, _toConsumableArray2.default)(phrases.map(function (phrase) {
            return {
              match_phrase: (0, _defineProperty2.default)({}, field, phrase)
            };
          })));
        }

        if (dates.length > 0) {
          queries.push.apply(queries, (0, _toConsumableArray2.default)(dates.map(function (value) {
            return {
              match: (0, _defineProperty2.default)({}, field, processDateOperation(value).expression)
            };
          })));
        }

        break;

      default:
        values.forEach(function (value) {
          if ((0, _date_value.isDateValue)(value)) {
            var operation = processDateOperation(value, operator);
            queries.push({
              range: (0, _defineProperty2.default)({}, field, (0, _defineProperty2.default)({}, operation.operator, operation.expression))
            });
          } else {
            queries.push({
              range: (0, _defineProperty2.default)({}, field, (0, _defineProperty2.default)({}, operator, value))
            });
          }
        });
    }
  });

  if (queries.length === 1) {
    return queries[0];
  }

  var key = andOr === 'and' ? 'must' : 'should';
  return {
    bool: (0, _defineProperty2.default)({}, key, [].concat(queries))
  };
};

exports._fieldValuesToQuery = _fieldValuesToQuery;

var _isFlagToQuery = function _isFlagToQuery(flag, on) {
  return {
    term: (0, _defineProperty2.default)({}, flag, on)
  };
};

exports._isFlagToQuery = _isFlagToQuery;

var collectTerms = function collectTerms(clauses) {
  var values = {
    must: [],
    mustNot: []
  };

  var _iterator = _createForOfIteratorHelper(clauses),
      _step;

  try {
    for (_iterator.s(); !(_step = _iterator.n()).done;) {
      var clause = _step.value;

      if (_ast.AST.Match.isMustClause(clause)) {
        values.must.push(clause.value);
      } else {
        values.mustNot.push(clause.value);
      }
    }
  } catch (err) {
    _iterator.e(err);
  } finally {
    _iterator.f();
  }

  return values;
};

var collectFields = function collectFields(clauses) {
  var fieldArray = function fieldArray(obj, field, operator) {
    if (!obj[field]) {
      obj[field] = {};
    }

    if (!obj[field][operator]) {
      obj[field][operator] = [];
    }

    return obj[field][operator];
  };

  return clauses.reduce(function (fields, clause) {
    if (_ast.AST.Match.isMustClause(clause)) {
      if ((0, _predicate.isArray)(clause.value)) {
        var _fieldArray;

        (_fieldArray = fieldArray(fields.must.or, clause.field, clause.operator)).push.apply(_fieldArray, (0, _toConsumableArray2.default)(clause.value));
      } else {
        fieldArray(fields.must.and, clause.field, clause.operator).push(clause.value);
      }
    } else {
      if ((0, _predicate.isArray)(clause.value)) {
        var _fieldArray2;

        (_fieldArray2 = fieldArray(fields.mustNot.or, clause.field, clause.operator)).push.apply(_fieldArray2, (0, _toConsumableArray2.default)(clause.value));
      } else {
        fieldArray(fields.mustNot.and, clause.field, clause.operator).push(clause.value);
      }
    }

    return fields;
  }, {
    must: {
      and: {},
      or: {}
    },
    mustNot: {
      and: {},
      or: {}
    }
  });
};

var clausesToEsQueryDsl = function clausesToEsQueryDsl(_ref) {
  var fields = _ref.fields,
      terms = _ref.terms,
      is = _ref.is;
  var options = arguments.length > 1 && arguments[1] !== undefined ? arguments[1] : {};
  var extraMustQueries = options.extraMustQueries || [];
  var extraMustNotQueries = options.extraMustNotQueries || [];
  var termValuesToQuery = options.termValuesToQuery || _termValuesToQuery;
  var fieldValuesToQuery = options.fieldValuesToQuery || _fieldValuesToQuery;
  var isFlagToQuery = options.isFlagToQuery || _isFlagToQuery;
  var must = [];
  must.push.apply(must, (0, _toConsumableArray2.default)(extraMustQueries));
  var termMustQuery = termValuesToQuery(terms.must, options);

  if (termMustQuery) {
    must.push(termMustQuery);
  }

  Object.keys(fields.must.and).forEach(function (field) {
    must.push(fieldValuesToQuery(field, fields.must.and[field], 'and'));
  });
  Object.keys(fields.must.or).forEach(function (field) {
    must.push(fieldValuesToQuery(field, fields.must.or[field], 'or'));
  });
  is.forEach(function (clause) {
    must.push(isFlagToQuery(clause.flag, _ast.AST.Match.isMustClause(clause)));
  });
  var mustNot = [];
  mustNot.push.apply(mustNot, (0, _toConsumableArray2.default)(extraMustNotQueries));
  var termMustNotQuery = termValuesToQuery(terms.mustNot, options);

  if (termMustNotQuery) {
    mustNot.push(termMustNotQuery);
  }

  Object.keys(fields.mustNot.and).forEach(function (field) {
    mustNot.push(fieldValuesToQuery(field, fields.mustNot.and[field], 'and'));
  });
  Object.keys(fields.mustNot.or).forEach(function (field) {
    mustNot.push(fieldValuesToQuery(field, fields.mustNot.or[field], 'or'));
  });
  var bool = {};

  if (must.length !== 0) {
    bool.must = must;
  }

  if (mustNot.length !== 0) {
    bool.must_not = mustNot;
  }

  return bool;
};

var EMPTY_TERMS = {
  must: [],
  mustNot: []
};
var EMPTY_FIELDS = {
  must: {
    and: {},
    or: {}
  },
  mustNot: {
    and: {},
    or: {}
  }
};

var astToEsQueryDsl = function astToEsQueryDsl(ast) {
  var options = arguments.length > 1 && arguments[1] !== undefined ? arguments[1] : {};

  if (ast.clauses.length === 0) {
    return {
      match_all: {}
    };
  }

  var terms = collectTerms(ast.getTermClauses());
  var fields = collectFields(ast.getFieldClauses());
  var is = ast.getIsClauses();
  var matchesBool = clausesToEsQueryDsl({
    terms: terms,
    fields: fields,
    is: is
  }, options);
  var hasTopMatches = Object.keys(matchesBool).length > 0;
  var groupClauses = ast.getGroupClauses();

  if (groupClauses.length === 0) {
    // there are no GroupClauses, everything at top level is combined as a must
    return {
      bool: matchesBool
    };
  } else {
    // there is at least one GroupClause, wrap the above clauses in another layer and append the ORs
    var must = groupClauses.reduce(function (must, groupClause) {
      var clauses = groupClause.value.reduce(function (clauses, clause) {
        if (_ast.AST.Term.isInstance(clause)) {
          clauses.push(clausesToEsQueryDsl({
            terms: collectTerms([clause]),
            fields: EMPTY_FIELDS,
            is: []
          }));
        } else if (_ast.AST.Field.isInstance(clause)) {
          clauses.push(clausesToEsQueryDsl({
            terms: EMPTY_TERMS,
            fields: collectFields([clause]),
            is: []
          }));
        } else if (_ast.AST.Is.isInstance(clause)) {
          clauses.push(clausesToEsQueryDsl({
            terms: EMPTY_TERMS,
            fields: EMPTY_FIELDS,
            is: [clause]
          }));
        }

        return clauses;
      }, []);
      must.push({
        bool: {
          should: clauses.map(function (clause) {
            return {
              bool: clause
            };
          })
        }
      });
      return must;
    }, hasTopMatches // only include the first match group if there are any conditions
    ? [{
      bool: matchesBool
    }] : []);
    return {
      bool: {
        must: must
      }
    };
  }
};

exports.astToEsQueryDsl = astToEsQueryDsl;