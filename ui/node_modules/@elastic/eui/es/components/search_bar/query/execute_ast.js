var _nameToOperatorMap;

function _defineProperty(obj, key, value) { if (key in obj) { Object.defineProperty(obj, key, { value: value, enumerable: true, configurable: true, writable: true }); } else { obj[key] = value; } return obj; }

/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */
import { get } from '../../../services/objects';
import { isString, isArray } from '../../../services/predicate';
import { eq, exact, gt, gte, lt, lte } from './operators';
import { AST } from './ast';
var EXPLAIN_FIELD = '__explain';
var nameToOperatorMap = (_nameToOperatorMap = {}, _defineProperty(_nameToOperatorMap, AST.Operator.EQ, eq), _defineProperty(_nameToOperatorMap, AST.Operator.EXACT, exact), _defineProperty(_nameToOperatorMap, AST.Operator.GT, gt), _defineProperty(_nameToOperatorMap, AST.Operator.GTE, gte), _defineProperty(_nameToOperatorMap, AST.Operator.LT, lt), _defineProperty(_nameToOperatorMap, AST.Operator.LTE, lte), _nameToOperatorMap);

var defaultIsClauseMatcher = function defaultIsClauseMatcher(item, clause, explain) {
  var type = clause.type,
      flag = clause.flag,
      match = clause.match;
  var value = get(item, clause.flag);
  var must = AST.Match.isMustClause(clause);
  var hit = !!value === must;

  if (explain && hit) {
    explain.push({
      hit: hit,
      type: type,
      flag: flag,
      match: match
    });
  }

  return hit;
};

var fieldClauseMatcher = function fieldClauseMatcher(item, field) {
  var clauses = arguments.length > 2 && arguments[2] !== undefined ? arguments[2] : [];
  var explain = arguments.length > 3 ? arguments[3] : undefined;
  return clauses.every(function (clause) {
    var type = clause.type,
        value = clause.value,
        match = clause.match;
    var operator = nameToOperatorMap[clause.operator];

    if (!operator) {
      // unknown matcher
      return true;
    }

    if (!AST.Match.isMust(match)) {
      operator = function operator(value, token) {
        return !nameToOperatorMap[clause.operator](value, token);
      };
    }

    var itemValue = get(item, field);
    var hit = isArray(value) ? value.some(function (v) {
      return operator(itemValue, v);
    }) : operator(itemValue, value);

    if (explain && hit) {
      explain.push({
        hit: hit,
        type: type,
        field: field,
        value: value,
        match: match,
        operator: operator
      });
    }

    return hit;
  });
}; // You might think that we could specify `item: T` here and do something
// with `keyof`, but that wouldn't work with `nested.field.name`


var extractStringFieldsFromItem = function extractStringFieldsFromItem(item) {
  return Object.keys(item).reduce(function (fields, key) {
    if (isString(item[key])) {
      fields.push(key);
    }

    return fields;
  }, []);
};

var termClauseMatcher = function termClauseMatcher(item, fields) {
  var clauses = arguments.length > 2 && arguments[2] !== undefined ? arguments[2] : [];
  var explain = arguments.length > 3 ? arguments[3] : undefined;
  var searchableFields = fields || extractStringFieldsFromItem(item);
  return clauses.every(function (clause) {
    var type = clause.type,
        value = clause.value,
        match = clause.match;
    var isMustClause = AST.Match.isMustClause(clause);
    var equals = nameToOperatorMap[AST.Operator.EQ];
    var containsMatches = searchableFields.some(function (field) {
      var itemValue = get(item, field);
      var isMatch = equals(itemValue, value);

      if (explain) {
        // If testing for the presence of a term, then we record a match as a match.
        // If testing for the absence of a term, then we invert this logic: we record a
        // non-match as a match.
        var hit = isMustClause && isMatch || !isMustClause && !isMatch;

        if (hit) {
          explain.push({
            hit: hit,
            type: type,
            field: field,
            match: match,
            value: value
          });
        }
      }

      return isMatch;
    });

    if (isMustClause) {
      // If we're testing for the presence of a term, then we only need 1 field to match.
      return containsMatches;
    } // If we're testing for the absence of a term, we can't have any matching fields at all.


    return !containsMatches;
  });
};

export var createFilter = function createFilter(ast, defaultFields) {
  var isClauseMatcher = arguments.length > 2 && arguments[2] !== undefined ? arguments[2] : defaultIsClauseMatcher;
  var explain = arguments.length > 3 && arguments[3] !== undefined ? arguments[3] : false;
  // Return items which pass ALL conditions: matches the terms entered, the specified field values,
  // and the specified "is" clauses.
  return function (item) {
    var explainLines = explain ? [] : undefined;

    if (explainLines) {
      // @ts-ignore technically, we could require T to extend `{ __explain?: Explain[] }` but that seems
      // like a ridiculous requirement on the caller.
      item[EXPLAIN_FIELD] = explainLines;
    }

    var termClauses = ast.getTermClauses();
    var fields = ast.getFieldNames();
    var isClauses = ast.getIsClauses();
    var groupClauses = ast.getGroupClauses();
    var isTermMatch = termClauseMatcher(item, defaultFields, termClauses, explainLines);

    if (!isTermMatch) {
      return false;
    }

    var isFieldsMatch = fields.every(function (field) {
      return fieldClauseMatcher(item, field, ast.getFieldClauses(field), explainLines);
    });

    if (!isFieldsMatch) {
      return false;
    }

    var isIsMatch = isClauses.every(function (clause) {
      return isClauseMatcher(item, clause, explainLines);
    });

    if (!isIsMatch) {
      return false;
    }

    var isGroupMatch = groupClauses.every(function (clause) {
      var matchesGroup = clause.value.some(function (clause) {
        if (AST.Term.isInstance(clause)) {
          return termClauseMatcher(item, defaultFields, [clause], explainLines);
        }

        if (AST.Field.isInstance(clause)) {
          return fieldClauseMatcher(item, clause.field, [clause], explainLines);
        }

        if (AST.Is.isInstance(clause)) {
          return isClauseMatcher(item, clause, explainLines);
        }

        throw new Error("Unknown query clause type in group, [".concat(clause.type, "]"));
      });
      return AST.Match.isMustClause(clause) ? matchesGroup : !matchesGroup;
    });
    return isGroupMatch;
  };
};
export function executeAst(ast, items) {
  var options = arguments.length > 2 && arguments[2] !== undefined ? arguments[2] : {};
  var isClauseMatcher = options.isClauseMatcher,
      defaultFields = options.defaultFields,
      explain = options.explain;
  var filter = createFilter(ast, defaultFields, isClauseMatcher, explain);
  return items.filter(filter);
}