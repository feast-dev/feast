/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */
import { printIso8601 } from './date_format';
import { isDateValue } from './date_value';
import { AST, Operator } from './ast';
import { isArray, isDateLike, isString, isBoolean, isNumber } from '../../../services/predicate';

var emitMatch = function emitMatch(match) {
  if (!match) {
    return '';
  }

  return AST.Match.isMust(match) ? '+' : '-';
};

var escapeValue = function escapeValue(value) {
  if (typeof value === 'string') {
    return value.replace(/([\\"])/g, '\\$1');
  }

  return value;
};

var emitFieldDateLikeClause = function emitFieldDateLikeClause(field, value, operator, match) {
  var matchOp = emitMatch(match);

  switch (operator) {
    case Operator.EQ:
      return "".concat(matchOp).concat(field, ":").concat(printIso8601(value));

    case Operator.GT:
      return "".concat(matchOp).concat(field, ":>").concat(printIso8601(value));

    case Operator.GTE:
      return "".concat(matchOp).concat(field, ":>=").concat(printIso8601(value));

    case Operator.LT:
      return "".concat(matchOp).concat(field, ":<").concat(printIso8601(value));

    case Operator.LTE:
      return "".concat(matchOp).concat(field, ":<=").concat(printIso8601(value));

    default:
      throw new Error("unknown operator [".concat(operator, "]"));
  }
};

var emitFieldDateValueClause = function emitFieldDateValueClause(field, value, operator, match) {
  var matchOp = emitMatch(match);
  var granularity = value.granularity,
      resolve = value.resolve;
  var date = resolve();

  if (granularity) {
    switch (operator) {
      case Operator.EQ:
        var gte = granularity.iso8601(granularity.start(date));
        var lt = granularity.iso8601(granularity.startOfNext(date));
        return "".concat(matchOp).concat(field, ":(>=").concat(gte, " AND <").concat(lt, ")");

      case Operator.GT:
        return "".concat(matchOp).concat(field, ":>=").concat(granularity.iso8601(granularity.startOfNext(date)));

      case Operator.GTE:
        return "".concat(matchOp).concat(field, ":>=").concat(granularity.iso8601(granularity.start(date)));

      case Operator.LT:
        return "".concat(matchOp).concat(field, ":<").concat(granularity.iso8601(granularity.start(date)));

      case Operator.LTE:
        return "".concat(matchOp).concat(field, ":<").concat(granularity.iso8601(granularity.startOfNext(date)));

      default:
        throw new Error("unknown operator [".concat(operator, "]"));
    }
  }

  return emitFieldDateLikeClause(field, date, operator, match);
};

var emitFieldNumericClause = function emitFieldNumericClause(field, value, operator, match) {
  var matchOp = emitMatch(match);

  switch (operator) {
    case Operator.EQ:
      return "".concat(matchOp).concat(field, ":").concat(value);

    case Operator.GT:
      return "".concat(matchOp).concat(field, ":>").concat(value);

    case Operator.GTE:
      return "".concat(matchOp).concat(field, ":>=").concat(value);

    case Operator.LT:
      return "".concat(matchOp).concat(field, ":<").concat(value);

    case Operator.LTE:
      return "".concat(matchOp).concat(field, ":<=").concat(value);

    default:
      throw new Error("unknown operator [".concat(operator, "]"));
  }
};

var emitFieldStringClause = function emitFieldStringClause(field, value, match) {
  var matchOp = emitMatch(match);

  if (value.match(/\s/)) {
    return "".concat(matchOp).concat(field, ":\"").concat(escapeValue(value), "\"");
  }

  return "".concat(matchOp).concat(field, ":").concat(escapeValue(value));
};

var emitFieldBooleanClause = function emitFieldBooleanClause(field, value, match) {
  var matchOp = emitMatch(match);
  return "".concat(matchOp).concat(field, ":").concat(value);
};

var emitFieldSingleValueClause = function emitFieldSingleValueClause(field, value, operator, match) {
  if (isDateValue(value)) {
    return emitFieldDateValueClause(field, value, operator, match);
  }

  if (isDateLike(value)) {
    return emitFieldDateLikeClause(field, value, operator, match);
  }

  if (isString(value)) {
    return emitFieldStringClause(field, value, match);
  }

  if (isNumber(value)) {
    return emitFieldNumericClause(field, value, operator, match);
  }

  if (isBoolean(value)) {
    return emitFieldBooleanClause(field, value, match);
  }

  throw new Error("unknown type of field value [".concat(value, "]"));
};

var emitFieldClause = function emitFieldClause(clause, isGroupMember) {
  var field = clause.field,
      value = clause.value,
      operator = clause.operator;
  var match = clause.match;

  if (isGroupMember && AST.Match.isMust(match)) {
    match = undefined;
  }

  if (!isArray(value)) {
    return emitFieldSingleValueClause(field, value, operator, match);
  }

  var matchOp = emitMatch(match);
  var clauses = value.map(function (v) {
    return emitFieldSingleValueClause(field, v, operator);
  }).join(' OR ');
  return "".concat(matchOp, "(").concat(clauses, ")");
};

var emitTermClause = function emitTermClause(clause, isGroupMember) {
  var value = clause.value;
  var match = clause.match;

  if (isGroupMember && AST.Match.isMust(match)) {
    match = undefined;
  }

  var matchOp = emitMatch(match);
  return "".concat(matchOp).concat(escapeValue(value));
};

var emitIsClause = function emitIsClause(clause, isGroupMember) {
  var flag = clause.flag,
      match = clause.match;
  var matchOp = isGroupMember ? '' : '+';
  var flagValue = AST.Match.isMust(match);
  return "".concat(matchOp).concat(flag, ":").concat(flagValue);
};

var emitGroupClause = function emitGroupClause(clause) {
  var value = clause.value;
  var formattedValues = value.map(function (clause) {
    return emitClause(clause, true);
  });
  return "+(".concat(formattedValues.join(' '), ")");
};

function emitClause(clause) {
  var isGroupMember = arguments.length > 1 && arguments[1] !== undefined ? arguments[1] : false;

  if (AST.Field.isInstance(clause)) {
    return emitFieldClause(clause, isGroupMember);
  }

  if (AST.Term.isInstance(clause)) {
    return emitTermClause(clause, isGroupMember);
  }

  if (AST.Is.isInstance(clause)) {
    return emitIsClause(clause, isGroupMember);
  }

  if (AST.Group.isInstance(clause)) {
    return emitGroupClause(clause);
  }

  throw new Error("unknown clause type [".concat(JSON.stringify(clause), "]"));
}

export var astToEsQueryString = function astToEsQueryString(ast) {
  if (ast.clauses.length === 0) {
    return '*';
  }

  return ast.clauses.map(function (clause) {
    return emitClause(clause);
  }).join(' ');
};