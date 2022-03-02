import _defineProperty from "@babel/runtime/helpers/defineProperty";
import _toConsumableArray from "@babel/runtime/helpers/toConsumableArray";

function ownKeys(object, enumerableOnly) { var keys = Object.keys(object); if (Object.getOwnPropertySymbols) { var symbols = Object.getOwnPropertySymbols(object); if (enumerableOnly) symbols = symbols.filter(function (sym) { return Object.getOwnPropertyDescriptor(object, sym).enumerable; }); keys.push.apply(keys, symbols); } return keys; }

function _objectSpread(target) { for (var i = 1; i < arguments.length; i++) { var source = arguments[i] != null ? arguments[i] : {}; if (i % 2) { ownKeys(Object(source), true).forEach(function (key) { _defineProperty(target, key, source[key]); }); } else if (Object.getOwnPropertyDescriptors) { Object.defineProperties(target, Object.getOwnPropertyDescriptors(source)); } else { ownKeys(Object(source)).forEach(function (key) { Object.defineProperty(target, key, Object.getOwnPropertyDescriptor(source, key)); }); } } return target; }

/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */
import { AST } from './ast';
import { isArray, isString, isDateLike } from '../../../services/predicate';
import { dateFormat as defaultDateFormat } from './date_format';
import { dateValueParser, isDateValue } from './date_value'; // @ts-ignore This is a Babel plugin that parses inline PEG grammars.

// eslint-disable-line import/no-unresolved
var parser = function () {
  "use strict";

  function peg$subclass(child, parent) {
    function ctor() {
      this.constructor = child;
    }

    ctor.prototype = parent.prototype;
    child.prototype = new ctor();
  }

  function peg$SyntaxError(message, expected, found, location) {
    this.message = message;
    this.expected = expected;
    this.found = found;
    this.location = location;
    this.name = "SyntaxError";

    if (typeof Error.captureStackTrace === "function") {
      Error.captureStackTrace(this, peg$SyntaxError);
    }
  }

  peg$subclass(peg$SyntaxError, Error);

  peg$SyntaxError.buildMessage = function (expected, found) {
    var DESCRIBE_EXPECTATION_FNS = {
      literal: function literal(expectation) {
        return "\"" + literalEscape(expectation.text) + "\"";
      },
      "class": function _class(expectation) {
        var escapedParts = "",
            i;

        for (i = 0; i < expectation.parts.length; i++) {
          escapedParts += expectation.parts[i] instanceof Array ? classEscape(expectation.parts[i][0]) + "-" + classEscape(expectation.parts[i][1]) : classEscape(expectation.parts[i]);
        }

        return "[" + (expectation.inverted ? "^" : "") + escapedParts + "]";
      },
      any: function any(expectation) {
        return "any character";
      },
      end: function end(expectation) {
        return "end of input";
      },
      other: function other(expectation) {
        return expectation.description;
      }
    };

    function hex(ch) {
      return ch.charCodeAt(0).toString(16).toUpperCase();
    }

    function literalEscape(s) {
      return s.replace(/\\/g, '\\\\').replace(/"/g, '\\"').replace(/\0/g, '\\0').replace(/\t/g, '\\t').replace(/\n/g, '\\n').replace(/\r/g, '\\r').replace(/[\x00-\x0F]/g, function (ch) {
        return '\\x0' + hex(ch);
      }).replace(/[\x10-\x1F\x7F-\x9F]/g, function (ch) {
        return '\\x' + hex(ch);
      });
    }

    function classEscape(s) {
      return s.replace(/\\/g, '\\\\').replace(/\]/g, '\\]').replace(/\^/g, '\\^').replace(/-/g, '\\-').replace(/\0/g, '\\0').replace(/\t/g, '\\t').replace(/\n/g, '\\n').replace(/\r/g, '\\r').replace(/[\x00-\x0F]/g, function (ch) {
        return '\\x0' + hex(ch);
      }).replace(/[\x10-\x1F\x7F-\x9F]/g, function (ch) {
        return '\\x' + hex(ch);
      });
    }

    function describeExpectation(expectation) {
      return DESCRIBE_EXPECTATION_FNS[expectation.type](expectation);
    }

    function describeExpected(expected) {
      var descriptions = new Array(expected.length),
          i,
          j;

      for (i = 0; i < expected.length; i++) {
        descriptions[i] = describeExpectation(expected[i]);
      }

      descriptions.sort();

      if (descriptions.length > 0) {
        for (i = 1, j = 1; i < descriptions.length; i++) {
          if (descriptions[i - 1] !== descriptions[i]) {
            descriptions[j] = descriptions[i];
            j++;
          }
        }

        descriptions.length = j;
      }

      switch (descriptions.length) {
        case 1:
          return descriptions[0];

        case 2:
          return descriptions[0] + " or " + descriptions[1];

        default:
          return descriptions.slice(0, -1).join(", ") + ", or " + descriptions[descriptions.length - 1];
      }
    }

    function describeFound(found) {
      return found ? "\"" + literalEscape(found) + "\"" : "end of input";
    }

    return "Expected " + describeExpected(expected) + " but " + describeFound(found) + " found.";
  };

  function peg$parse(input, options) {
    options = options !== void 0 ? options : {};

    var peg$FAILED = {},
        peg$startRuleFunctions = {
      Query: peg$parseQuery
    },
        peg$startRuleFunction = peg$parseQuery,
        peg$c0 = function peg$c0(clauses) {
      return clauses;
    },
        peg$c1 = function peg$c1() {
      return [];
    },
        peg$c2 = function peg$c2(head, clause) {
      return clause;
    },
        peg$c3 = function peg$c3(head, tail) {
      return [head].concat(_toConsumableArray(tail));
    },
        peg$c4 = "(",
        peg$c5 = peg$literalExpectation("(", false),
        peg$c6 = ")",
        peg$c7 = peg$literalExpectation(")", false),
        peg$c8 = function peg$c8(head, tail) {
      return [head].concat(_toConsumableArray(tail));
    },
        peg$c9 = "-",
        peg$c10 = peg$literalExpectation("-", false),
        peg$c11 = function peg$c11(group) {
      return AST.Group.mustNot(group);
    },
        peg$c12 = function peg$c12(group) {
      return AST.Group.must(group);
    },
        peg$c13 = function peg$c13(value) {
      return AST.Term.mustNot(value);
    },
        peg$c14 = function peg$c14(value) {
      return AST.Term.must(value);
    },
        peg$c15 = function peg$c15(flag) {
      return AST.Is.mustNot(flag);
    },
        peg$c16 = function peg$c16(flag) {
      return AST.Is.must(flag);
    },
        peg$c17 = "is:",
        peg$c18 = peg$literalExpectation("is:", false),
        peg$c19 = function peg$c19(flag) {
      validateFlag(flag, location(), ctx);
      return flag;
    },
        peg$c20 = peg$otherExpectation("field"),
        peg$c21 = function peg$c21(fv) {
      return AST.Field.mustNot.eq(fv.field, fv.value);
    },
        peg$c22 = function peg$c22(fv) {
      return AST.Field.mustNot.exact(fv.field, fv.value);
    },
        peg$c23 = function peg$c23(fv) {
      return AST.Field.mustNot.gt(fv.field, fv.value);
    },
        peg$c24 = function peg$c24(fv) {
      return AST.Field.mustNot.gte(fv.field, fv.value);
    },
        peg$c25 = function peg$c25(fv) {
      return AST.Field.mustNot.lt(fv.field, fv.value);
    },
        peg$c26 = function peg$c26(fv) {
      return AST.Field.mustNot.lte(fv.field, fv.value);
    },
        peg$c27 = function peg$c27(fv) {
      return AST.Field.must.eq(fv.field, fv.value);
    },
        peg$c28 = function peg$c28(fv) {
      return AST.Field.must.exact(fv.field, fv.value);
    },
        peg$c29 = function peg$c29(fv) {
      return AST.Field.must.gt(fv.field, fv.value);
    },
        peg$c30 = function peg$c30(fv) {
      return AST.Field.must.gte(fv.field, fv.value);
    },
        peg$c31 = function peg$c31(fv) {
      return AST.Field.must.lt(fv.field, fv.value);
    },
        peg$c32 = function peg$c32(fv) {
      return AST.Field.must.lte(fv.field, fv.value);
    },
        peg$c33 = ":",
        peg$c34 = peg$literalExpectation(":", false),
        peg$c35 = function peg$c35(field, valueExpression) {
      return {
        field: field,
        value: resolveFieldValue(field, valueExpression, ctx)
      };
    },
        peg$c36 = "=",
        peg$c37 = peg$literalExpectation("=", false),
        peg$c38 = ">",
        peg$c39 = peg$literalExpectation(">", false),
        peg$c40 = function peg$c40(field, valueExpression) {
      return {
        field: field,
        value: resolveFieldValue(field, valueExpression, ctx)
      };
    },
        peg$c41 = ">=",
        peg$c42 = peg$literalExpectation(">=", false),
        peg$c43 = "<",
        peg$c44 = peg$literalExpectation("<", false),
        peg$c45 = "<=",
        peg$c46 = peg$literalExpectation("<=", false),
        peg$c47 = peg$otherExpectation("flag name"),
        peg$c48 = peg$otherExpectation("field name"),
        peg$c49 = function peg$c49() {
      return unescapeValue(text());
    },
        peg$c50 = /^[\-_]/,
        peg$c51 = peg$classExpectation(["-", "_"], false, false),
        peg$c52 = peg$otherExpectation("field value"),
        peg$c53 = peg$otherExpectation("term"),
        peg$c54 = function peg$c54(value) {
      return value.expression;
    },
        peg$c55 = function peg$c55(head, value) {
      return value;
    },
        peg$c56 = function peg$c56(head, tail) {
      return [head].concat(_toConsumableArray(tail));
    },
        peg$c57 = "\"",
        peg$c58 = peg$literalExpectation("\"", false),
        peg$c59 = function peg$c59() {
      return unescapePhraseValue(text());
    },
        peg$c60 = function peg$c60(phrase) {
      return Exp.string(phrase, location());
    },
        peg$c61 = /^[^\\" ]/,
        peg$c62 = peg$classExpectation(["\\", "\"", " "], true, false),
        peg$c63 = "\\",
        peg$c64 = peg$literalExpectation("\\", false),
        peg$c65 = peg$anyExpectation(),
        peg$c66 = function peg$c66() {
      if (text().toLowerCase() === 'or') {
        error('To use OR in a text search, put it inside quotes: "or". To ' + 'perform a logical OR, enclose the words in parenthesis: (foo:bar or bar).');
      }

      return Exp.string(unescapeValue(text()), location());
    },
        peg$c67 = /^[\-_*:\/]/,
        peg$c68 = peg$classExpectation(["-", "_", "*", ":", "/"], false, false),
        peg$c69 = /^[\xC0-\uFFFF]/,
        peg$c70 = peg$classExpectation([["\xC0", "\uFFFF"]], false, false),
        peg$c71 = /^[\-:\\()]/,
        peg$c72 = peg$classExpectation(["-", ":", "\\", "(", ")"], false, false),
        peg$c73 = /^[oO]/,
        peg$c74 = peg$classExpectation(["o", "O"], false, false),
        peg$c75 = /^[rR]/,
        peg$c76 = peg$classExpectation(["r", "R"], false, false),
        peg$c77 = function peg$c77(bool) {
      return bool;
    },
        peg$c78 = /^[tT]/,
        peg$c79 = peg$classExpectation(["t", "T"], false, false),
        peg$c80 = /^[uU]/,
        peg$c81 = peg$classExpectation(["u", "U"], false, false),
        peg$c82 = /^[eE]/,
        peg$c83 = peg$classExpectation(["e", "E"], false, false),
        peg$c84 = function peg$c84() {
      return Exp.boolean(text(), location());
    },
        peg$c85 = /^[fF]/,
        peg$c86 = peg$classExpectation(["f", "F"], false, false),
        peg$c87 = /^[aA]/,
        peg$c88 = peg$classExpectation(["a", "A"], false, false),
        peg$c89 = /^[lL]/,
        peg$c90 = peg$classExpectation(["l", "L"], false, false),
        peg$c91 = /^[sS]/,
        peg$c92 = peg$classExpectation(["s", "S"], false, false),
        peg$c93 = /^[yY]/,
        peg$c94 = peg$classExpectation(["y", "Y"], false, false),
        peg$c95 = /^[nN]/,
        peg$c96 = peg$classExpectation(["n", "N"], false, false),
        peg$c97 = /^[\-]/,
        peg$c98 = peg$classExpectation(["-"], false, false),
        peg$c99 = /^[0-9]/,
        peg$c100 = peg$classExpectation([["0", "9"]], false, false),
        peg$c101 = ".",
        peg$c102 = peg$literalExpectation(".", false),
        peg$c103 = function peg$c103() {
      return Exp.number(text(), location());
    },
        peg$c104 = function peg$c104(num) {
      return num;
    },
        peg$c105 = "'",
        peg$c106 = peg$literalExpectation("'", false),
        peg$c107 = function peg$c107() {
      return text();
    },
        peg$c108 = function peg$c108(expression) {
      return Exp.date(expression, location());
    },
        peg$c109 = peg$otherExpectation("alpha numeric"),
        peg$c110 = /^[a-zA-Z0-9.]/,
        peg$c111 = peg$classExpectation([["a", "z"], ["A", "Z"], ["0", "9"], "."], false, false),
        peg$c112 = peg$otherExpectation("whitespace"),
        peg$c113 = /^[ \t\n\r]/,
        peg$c114 = peg$classExpectation([" ", "\t", "\n", "\r"], false, false),
        peg$currPos = 0,
        peg$savedPos = 0,
        peg$posDetailsCache = [{
      line: 1,
      column: 1
    }],
        peg$maxFailPos = 0,
        peg$maxFailExpected = [],
        peg$silentFails = 0,
        peg$result;

    if ("startRule" in options) {
      if (!(options.startRule in peg$startRuleFunctions)) {
        throw new Error("Can't start parsing from rule \"" + options.startRule + "\".");
      }

      peg$startRuleFunction = peg$startRuleFunctions[options.startRule];
    }

    function text() {
      return input.substring(peg$savedPos, peg$currPos);
    }

    function location() {
      return peg$computeLocation(peg$savedPos, peg$currPos);
    }

    function expected(description, location) {
      location = location !== void 0 ? location : peg$computeLocation(peg$savedPos, peg$currPos);
      throw peg$buildStructuredError([peg$otherExpectation(description)], input.substring(peg$savedPos, peg$currPos), location);
    }

    function error(message, location) {
      location = location !== void 0 ? location : peg$computeLocation(peg$savedPos, peg$currPos);
      throw peg$buildSimpleError(message, location);
    }

    function peg$literalExpectation(text, ignoreCase) {
      return {
        type: "literal",
        text: text,
        ignoreCase: ignoreCase
      };
    }

    function peg$classExpectation(parts, inverted, ignoreCase) {
      return {
        type: "class",
        parts: parts,
        inverted: inverted,
        ignoreCase: ignoreCase
      };
    }

    function peg$anyExpectation() {
      return {
        type: "any"
      };
    }

    function peg$endExpectation() {
      return {
        type: "end"
      };
    }

    function peg$otherExpectation(description) {
      return {
        type: "other",
        description: description
      };
    }

    function peg$computePosDetails(pos) {
      var details = peg$posDetailsCache[pos],
          p;

      if (details) {
        return details;
      } else {
        p = pos - 1;

        while (!peg$posDetailsCache[p]) {
          p--;
        }

        details = peg$posDetailsCache[p];
        details = {
          line: details.line,
          column: details.column
        };

        while (p < pos) {
          if (input.charCodeAt(p) === 10) {
            details.line++;
            details.column = 1;
          } else {
            details.column++;
          }

          p++;
        }

        peg$posDetailsCache[pos] = details;
        return details;
      }
    }

    function peg$computeLocation(startPos, endPos) {
      var startPosDetails = peg$computePosDetails(startPos),
          endPosDetails = peg$computePosDetails(endPos);
      return {
        start: {
          offset: startPos,
          line: startPosDetails.line,
          column: startPosDetails.column
        },
        end: {
          offset: endPos,
          line: endPosDetails.line,
          column: endPosDetails.column
        }
      };
    }

    function peg$fail(expected) {
      if (peg$currPos < peg$maxFailPos) {
        return;
      }

      if (peg$currPos > peg$maxFailPos) {
        peg$maxFailPos = peg$currPos;
        peg$maxFailExpected = [];
      }

      peg$maxFailExpected.push(expected);
    }

    function peg$buildSimpleError(message, location) {
      return new peg$SyntaxError(message, null, null, location);
    }

    function peg$buildStructuredError(expected, found, location) {
      return new peg$SyntaxError(peg$SyntaxError.buildMessage(expected, found), expected, found, location);
    }

    function peg$parseQuery() {
      var s0, s1;
      s0 = peg$currPos;
      s1 = peg$parseClauses();

      if (s1 !== peg$FAILED) {
        peg$savedPos = s0;
        s1 = peg$c0(s1);
      }

      s0 = s1;

      if (s0 === peg$FAILED) {
        s0 = peg$currPos;
        s1 = peg$parsespace();

        if (s1 === peg$FAILED) {
          s1 = null;
        }

        if (s1 !== peg$FAILED) {
          peg$savedPos = s0;
          s1 = peg$c1();
        }

        s0 = s1;
      }

      return s0;
    }

    function peg$parseClauses() {
      var s0, s1, s2, s3, s4, s5, s6;
      s0 = peg$currPos;
      s1 = peg$parsespace();

      if (s1 === peg$FAILED) {
        s1 = null;
      }

      if (s1 !== peg$FAILED) {
        s2 = peg$parseClause();

        if (s2 !== peg$FAILED) {
          s3 = [];
          s4 = peg$currPos;
          s5 = peg$parsespace();

          if (s5 !== peg$FAILED) {
            s6 = peg$parseClause();

            if (s6 !== peg$FAILED) {
              peg$savedPos = s4;
              s5 = peg$c2(s2, s6);
              s4 = s5;
            } else {
              peg$currPos = s4;
              s4 = peg$FAILED;
            }
          } else {
            peg$currPos = s4;
            s4 = peg$FAILED;
          }

          while (s4 !== peg$FAILED) {
            s3.push(s4);
            s4 = peg$currPos;
            s5 = peg$parsespace();

            if (s5 !== peg$FAILED) {
              s6 = peg$parseClause();

              if (s6 !== peg$FAILED) {
                peg$savedPos = s4;
                s5 = peg$c2(s2, s6);
                s4 = s5;
              } else {
                peg$currPos = s4;
                s4 = peg$FAILED;
              }
            } else {
              peg$currPos = s4;
              s4 = peg$FAILED;
            }
          }

          if (s3 !== peg$FAILED) {
            s4 = peg$parsespace();

            if (s4 === peg$FAILED) {
              s4 = null;
            }

            if (s4 !== peg$FAILED) {
              peg$savedPos = s0;
              s1 = peg$c3(s2, s3);
              s0 = s1;
            } else {
              peg$currPos = s0;
              s0 = peg$FAILED;
            }
          } else {
            peg$currPos = s0;
            s0 = peg$FAILED;
          }
        } else {
          peg$currPos = s0;
          s0 = peg$FAILED;
        }
      } else {
        peg$currPos = s0;
        s0 = peg$FAILED;
      }

      return s0;
    }

    function peg$parseClause() {
      var s0;
      s0 = peg$parseGroupClause();

      if (s0 === peg$FAILED) {
        s0 = peg$parseIsClause();

        if (s0 === peg$FAILED) {
          s0 = peg$parseFieldClause();

          if (s0 === peg$FAILED) {
            s0 = peg$parseTermClause();
          }
        }
      }

      return s0;
    }

    function peg$parseSubGroupClause() {
      var s0, s1, s2, s3, s4, s5, s6, s7, s8;
      s0 = peg$currPos;

      if (input.charCodeAt(peg$currPos) === 40) {
        s1 = peg$c4;
        peg$currPos++;
      } else {
        s1 = peg$FAILED;

        if (peg$silentFails === 0) {
          peg$fail(peg$c5);
        }
      }

      if (s1 !== peg$FAILED) {
        s2 = peg$parseClause();

        if (s2 !== peg$FAILED) {
          s3 = [];
          s4 = peg$currPos;
          s5 = peg$parsespace();

          if (s5 === peg$FAILED) {
            s5 = null;
          }

          if (s5 !== peg$FAILED) {
            s6 = peg$parseorWord();

            if (s6 !== peg$FAILED) {
              s7 = peg$parsespace();

              if (s7 === peg$FAILED) {
                s7 = null;
              }

              if (s7 !== peg$FAILED) {
                s8 = peg$parseClause();

                if (s8 !== peg$FAILED) {
                  peg$savedPos = s4;
                  s5 = peg$c2(s2, s8);
                  s4 = s5;
                } else {
                  peg$currPos = s4;
                  s4 = peg$FAILED;
                }
              } else {
                peg$currPos = s4;
                s4 = peg$FAILED;
              }
            } else {
              peg$currPos = s4;
              s4 = peg$FAILED;
            }
          } else {
            peg$currPos = s4;
            s4 = peg$FAILED;
          }

          while (s4 !== peg$FAILED) {
            s3.push(s4);
            s4 = peg$currPos;
            s5 = peg$parsespace();

            if (s5 === peg$FAILED) {
              s5 = null;
            }

            if (s5 !== peg$FAILED) {
              s6 = peg$parseorWord();

              if (s6 !== peg$FAILED) {
                s7 = peg$parsespace();

                if (s7 === peg$FAILED) {
                  s7 = null;
                }

                if (s7 !== peg$FAILED) {
                  s8 = peg$parseClause();

                  if (s8 !== peg$FAILED) {
                    peg$savedPos = s4;
                    s5 = peg$c2(s2, s8);
                    s4 = s5;
                  } else {
                    peg$currPos = s4;
                    s4 = peg$FAILED;
                  }
                } else {
                  peg$currPos = s4;
                  s4 = peg$FAILED;
                }
              } else {
                peg$currPos = s4;
                s4 = peg$FAILED;
              }
            } else {
              peg$currPos = s4;
              s4 = peg$FAILED;
            }
          }

          if (s3 !== peg$FAILED) {
            if (input.charCodeAt(peg$currPos) === 41) {
              s4 = peg$c6;
              peg$currPos++;
            } else {
              s4 = peg$FAILED;

              if (peg$silentFails === 0) {
                peg$fail(peg$c7);
              }
            }

            if (s4 !== peg$FAILED) {
              peg$savedPos = s0;
              s1 = peg$c8(s2, s3);
              s0 = s1;
            } else {
              peg$currPos = s0;
              s0 = peg$FAILED;
            }
          } else {
            peg$currPos = s0;
            s0 = peg$FAILED;
          }
        } else {
          peg$currPos = s0;
          s0 = peg$FAILED;
        }
      } else {
        peg$currPos = s0;
        s0 = peg$FAILED;
      }

      return s0;
    }

    function peg$parseGroupClause() {
      var s0, s1, s2, s3;
      s0 = peg$currPos;
      s1 = peg$parsespace();

      if (s1 === peg$FAILED) {
        s1 = null;
      }

      if (s1 !== peg$FAILED) {
        if (input.charCodeAt(peg$currPos) === 45) {
          s2 = peg$c9;
          peg$currPos++;
        } else {
          s2 = peg$FAILED;

          if (peg$silentFails === 0) {
            peg$fail(peg$c10);
          }
        }

        if (s2 !== peg$FAILED) {
          s3 = peg$parseSubGroupClause();

          if (s3 !== peg$FAILED) {
            peg$savedPos = s0;
            s1 = peg$c11(s3);
            s0 = s1;
          } else {
            peg$currPos = s0;
            s0 = peg$FAILED;
          }
        } else {
          peg$currPos = s0;
          s0 = peg$FAILED;
        }
      } else {
        peg$currPos = s0;
        s0 = peg$FAILED;
      }

      if (s0 === peg$FAILED) {
        s0 = peg$currPos;
        s1 = peg$parsespace();

        if (s1 === peg$FAILED) {
          s1 = null;
        }

        if (s1 !== peg$FAILED) {
          s2 = peg$parseSubGroupClause();

          if (s2 !== peg$FAILED) {
            peg$savedPos = s0;
            s1 = peg$c12(s2);
            s0 = s1;
          } else {
            peg$currPos = s0;
            s0 = peg$FAILED;
          }
        } else {
          peg$currPos = s0;
          s0 = peg$FAILED;
        }
      }

      return s0;
    }

    function peg$parseTermClause() {
      var s0, s1, s2, s3;
      s0 = peg$currPos;
      s1 = peg$parsespace();

      if (s1 === peg$FAILED) {
        s1 = null;
      }

      if (s1 !== peg$FAILED) {
        if (input.charCodeAt(peg$currPos) === 45) {
          s2 = peg$c9;
          peg$currPos++;
        } else {
          s2 = peg$FAILED;

          if (peg$silentFails === 0) {
            peg$fail(peg$c10);
          }
        }

        if (s2 !== peg$FAILED) {
          s3 = peg$parsetermValue();

          if (s3 !== peg$FAILED) {
            peg$savedPos = s0;
            s1 = peg$c13(s3);
            s0 = s1;
          } else {
            peg$currPos = s0;
            s0 = peg$FAILED;
          }
        } else {
          peg$currPos = s0;
          s0 = peg$FAILED;
        }
      } else {
        peg$currPos = s0;
        s0 = peg$FAILED;
      }

      if (s0 === peg$FAILED) {
        s0 = peg$currPos;
        s1 = peg$parsespace();

        if (s1 === peg$FAILED) {
          s1 = null;
        }

        if (s1 !== peg$FAILED) {
          s2 = peg$parsetermValue();

          if (s2 !== peg$FAILED) {
            peg$savedPos = s0;
            s1 = peg$c14(s2);
            s0 = s1;
          } else {
            peg$currPos = s0;
            s0 = peg$FAILED;
          }
        } else {
          peg$currPos = s0;
          s0 = peg$FAILED;
        }
      }

      return s0;
    }

    function peg$parseIsClause() {
      var s0, s1, s2, s3;
      s0 = peg$currPos;
      s1 = peg$parsespace();

      if (s1 === peg$FAILED) {
        s1 = null;
      }

      if (s1 !== peg$FAILED) {
        if (input.charCodeAt(peg$currPos) === 45) {
          s2 = peg$c9;
          peg$currPos++;
        } else {
          s2 = peg$FAILED;

          if (peg$silentFails === 0) {
            peg$fail(peg$c10);
          }
        }

        if (s2 !== peg$FAILED) {
          s3 = peg$parseIsFlag();

          if (s3 !== peg$FAILED) {
            peg$savedPos = s0;
            s1 = peg$c15(s3);
            s0 = s1;
          } else {
            peg$currPos = s0;
            s0 = peg$FAILED;
          }
        } else {
          peg$currPos = s0;
          s0 = peg$FAILED;
        }
      } else {
        peg$currPos = s0;
        s0 = peg$FAILED;
      }

      if (s0 === peg$FAILED) {
        s0 = peg$currPos;
        s1 = peg$parsespace();

        if (s1 === peg$FAILED) {
          s1 = null;
        }

        if (s1 !== peg$FAILED) {
          s2 = peg$parseIsFlag();

          if (s2 !== peg$FAILED) {
            peg$savedPos = s0;
            s1 = peg$c16(s2);
            s0 = s1;
          } else {
            peg$currPos = s0;
            s0 = peg$FAILED;
          }
        } else {
          peg$currPos = s0;
          s0 = peg$FAILED;
        }
      }

      return s0;
    }

    function peg$parseIsFlag() {
      var s0, s1, s2;
      s0 = peg$currPos;

      if (input.substr(peg$currPos, 3) === peg$c17) {
        s1 = peg$c17;
        peg$currPos += 3;
      } else {
        s1 = peg$FAILED;

        if (peg$silentFails === 0) {
          peg$fail(peg$c18);
        }
      }

      if (s1 !== peg$FAILED) {
        s2 = peg$parseflagName();

        if (s2 !== peg$FAILED) {
          peg$savedPos = s0;
          s1 = peg$c19(s2);
          s0 = s1;
        } else {
          peg$currPos = s0;
          s0 = peg$FAILED;
        }
      } else {
        peg$currPos = s0;
        s0 = peg$FAILED;
      }

      return s0;
    }

    function peg$parseFieldClause() {
      var s0, s1, s2, s3;
      peg$silentFails++;
      s0 = peg$currPos;
      s1 = peg$parsespace();

      if (s1 === peg$FAILED) {
        s1 = null;
      }

      if (s1 !== peg$FAILED) {
        if (input.charCodeAt(peg$currPos) === 45) {
          s2 = peg$c9;
          peg$currPos++;
        } else {
          s2 = peg$FAILED;

          if (peg$silentFails === 0) {
            peg$fail(peg$c10);
          }
        }

        if (s2 !== peg$FAILED) {
          s3 = peg$parseFieldEQValue();

          if (s3 !== peg$FAILED) {
            peg$savedPos = s0;
            s1 = peg$c21(s3);
            s0 = s1;
          } else {
            peg$currPos = s0;
            s0 = peg$FAILED;
          }
        } else {
          peg$currPos = s0;
          s0 = peg$FAILED;
        }
      } else {
        peg$currPos = s0;
        s0 = peg$FAILED;
      }

      if (s0 === peg$FAILED) {
        s0 = peg$currPos;
        s1 = peg$parsespace();

        if (s1 === peg$FAILED) {
          s1 = null;
        }

        if (s1 !== peg$FAILED) {
          if (input.charCodeAt(peg$currPos) === 45) {
            s2 = peg$c9;
            peg$currPos++;
          } else {
            s2 = peg$FAILED;

            if (peg$silentFails === 0) {
              peg$fail(peg$c10);
            }
          }

          if (s2 !== peg$FAILED) {
            s3 = peg$parseFieldEXACTValue();

            if (s3 !== peg$FAILED) {
              peg$savedPos = s0;
              s1 = peg$c22(s3);
              s0 = s1;
            } else {
              peg$currPos = s0;
              s0 = peg$FAILED;
            }
          } else {
            peg$currPos = s0;
            s0 = peg$FAILED;
          }
        } else {
          peg$currPos = s0;
          s0 = peg$FAILED;
        }

        if (s0 === peg$FAILED) {
          s0 = peg$currPos;
          s1 = peg$parsespace();

          if (s1 === peg$FAILED) {
            s1 = null;
          }

          if (s1 !== peg$FAILED) {
            if (input.charCodeAt(peg$currPos) === 45) {
              s2 = peg$c9;
              peg$currPos++;
            } else {
              s2 = peg$FAILED;

              if (peg$silentFails === 0) {
                peg$fail(peg$c10);
              }
            }

            if (s2 !== peg$FAILED) {
              s3 = peg$parseFieldGTValue();

              if (s3 !== peg$FAILED) {
                peg$savedPos = s0;
                s1 = peg$c23(s3);
                s0 = s1;
              } else {
                peg$currPos = s0;
                s0 = peg$FAILED;
              }
            } else {
              peg$currPos = s0;
              s0 = peg$FAILED;
            }
          } else {
            peg$currPos = s0;
            s0 = peg$FAILED;
          }

          if (s0 === peg$FAILED) {
            s0 = peg$currPos;
            s1 = peg$parsespace();

            if (s1 === peg$FAILED) {
              s1 = null;
            }

            if (s1 !== peg$FAILED) {
              if (input.charCodeAt(peg$currPos) === 45) {
                s2 = peg$c9;
                peg$currPos++;
              } else {
                s2 = peg$FAILED;

                if (peg$silentFails === 0) {
                  peg$fail(peg$c10);
                }
              }

              if (s2 !== peg$FAILED) {
                s3 = peg$parseFieldGTEValue();

                if (s3 !== peg$FAILED) {
                  peg$savedPos = s0;
                  s1 = peg$c24(s3);
                  s0 = s1;
                } else {
                  peg$currPos = s0;
                  s0 = peg$FAILED;
                }
              } else {
                peg$currPos = s0;
                s0 = peg$FAILED;
              }
            } else {
              peg$currPos = s0;
              s0 = peg$FAILED;
            }

            if (s0 === peg$FAILED) {
              s0 = peg$currPos;
              s1 = peg$parsespace();

              if (s1 === peg$FAILED) {
                s1 = null;
              }

              if (s1 !== peg$FAILED) {
                if (input.charCodeAt(peg$currPos) === 45) {
                  s2 = peg$c9;
                  peg$currPos++;
                } else {
                  s2 = peg$FAILED;

                  if (peg$silentFails === 0) {
                    peg$fail(peg$c10);
                  }
                }

                if (s2 !== peg$FAILED) {
                  s3 = peg$parseFieldLTValue();

                  if (s3 !== peg$FAILED) {
                    peg$savedPos = s0;
                    s1 = peg$c25(s3);
                    s0 = s1;
                  } else {
                    peg$currPos = s0;
                    s0 = peg$FAILED;
                  }
                } else {
                  peg$currPos = s0;
                  s0 = peg$FAILED;
                }
              } else {
                peg$currPos = s0;
                s0 = peg$FAILED;
              }

              if (s0 === peg$FAILED) {
                s0 = peg$currPos;
                s1 = peg$parsespace();

                if (s1 === peg$FAILED) {
                  s1 = null;
                }

                if (s1 !== peg$FAILED) {
                  if (input.charCodeAt(peg$currPos) === 45) {
                    s2 = peg$c9;
                    peg$currPos++;
                  } else {
                    s2 = peg$FAILED;

                    if (peg$silentFails === 0) {
                      peg$fail(peg$c10);
                    }
                  }

                  if (s2 !== peg$FAILED) {
                    s3 = peg$parseFieldLTEValue();

                    if (s3 !== peg$FAILED) {
                      peg$savedPos = s0;
                      s1 = peg$c26(s3);
                      s0 = s1;
                    } else {
                      peg$currPos = s0;
                      s0 = peg$FAILED;
                    }
                  } else {
                    peg$currPos = s0;
                    s0 = peg$FAILED;
                  }
                } else {
                  peg$currPos = s0;
                  s0 = peg$FAILED;
                }

                if (s0 === peg$FAILED) {
                  s0 = peg$currPos;
                  s1 = peg$parsespace();

                  if (s1 === peg$FAILED) {
                    s1 = null;
                  }

                  if (s1 !== peg$FAILED) {
                    s2 = peg$parseFieldEQValue();

                    if (s2 !== peg$FAILED) {
                      peg$savedPos = s0;
                      s1 = peg$c27(s2);
                      s0 = s1;
                    } else {
                      peg$currPos = s0;
                      s0 = peg$FAILED;
                    }
                  } else {
                    peg$currPos = s0;
                    s0 = peg$FAILED;
                  }

                  if (s0 === peg$FAILED) {
                    s0 = peg$currPos;
                    s1 = peg$parsespace();

                    if (s1 === peg$FAILED) {
                      s1 = null;
                    }

                    if (s1 !== peg$FAILED) {
                      s2 = peg$parseFieldEXACTValue();

                      if (s2 !== peg$FAILED) {
                        peg$savedPos = s0;
                        s1 = peg$c28(s2);
                        s0 = s1;
                      } else {
                        peg$currPos = s0;
                        s0 = peg$FAILED;
                      }
                    } else {
                      peg$currPos = s0;
                      s0 = peg$FAILED;
                    }

                    if (s0 === peg$FAILED) {
                      s0 = peg$currPos;
                      s1 = peg$parsespace();

                      if (s1 === peg$FAILED) {
                        s1 = null;
                      }

                      if (s1 !== peg$FAILED) {
                        s2 = peg$parseFieldGTValue();

                        if (s2 !== peg$FAILED) {
                          peg$savedPos = s0;
                          s1 = peg$c29(s2);
                          s0 = s1;
                        } else {
                          peg$currPos = s0;
                          s0 = peg$FAILED;
                        }
                      } else {
                        peg$currPos = s0;
                        s0 = peg$FAILED;
                      }

                      if (s0 === peg$FAILED) {
                        s0 = peg$currPos;
                        s1 = peg$parsespace();

                        if (s1 === peg$FAILED) {
                          s1 = null;
                        }

                        if (s1 !== peg$FAILED) {
                          s2 = peg$parseFieldGTEValue();

                          if (s2 !== peg$FAILED) {
                            peg$savedPos = s0;
                            s1 = peg$c30(s2);
                            s0 = s1;
                          } else {
                            peg$currPos = s0;
                            s0 = peg$FAILED;
                          }
                        } else {
                          peg$currPos = s0;
                          s0 = peg$FAILED;
                        }

                        if (s0 === peg$FAILED) {
                          s0 = peg$currPos;
                          s1 = peg$parsespace();

                          if (s1 === peg$FAILED) {
                            s1 = null;
                          }

                          if (s1 !== peg$FAILED) {
                            s2 = peg$parseFieldLTValue();

                            if (s2 !== peg$FAILED) {
                              peg$savedPos = s0;
                              s1 = peg$c31(s2);
                              s0 = s1;
                            } else {
                              peg$currPos = s0;
                              s0 = peg$FAILED;
                            }
                          } else {
                            peg$currPos = s0;
                            s0 = peg$FAILED;
                          }

                          if (s0 === peg$FAILED) {
                            s0 = peg$currPos;
                            s1 = peg$parsespace();

                            if (s1 === peg$FAILED) {
                              s1 = null;
                            }

                            if (s1 !== peg$FAILED) {
                              s2 = peg$parseFieldLTEValue();

                              if (s2 !== peg$FAILED) {
                                peg$savedPos = s0;
                                s1 = peg$c32(s2);
                                s0 = s1;
                              } else {
                                peg$currPos = s0;
                                s0 = peg$FAILED;
                              }
                            } else {
                              peg$currPos = s0;
                              s0 = peg$FAILED;
                            }
                          }
                        }
                      }
                    }
                  }
                }
              }
            }
          }
        }
      }

      peg$silentFails--;

      if (s0 === peg$FAILED) {
        s1 = peg$FAILED;

        if (peg$silentFails === 0) {
          peg$fail(peg$c20);
        }
      }

      return s0;
    }

    function peg$parseFieldEQValue() {
      var s0, s1, s2, s3;
      s0 = peg$currPos;
      s1 = peg$parsefieldName();

      if (s1 !== peg$FAILED) {
        if (input.charCodeAt(peg$currPos) === 58) {
          s2 = peg$c33;
          peg$currPos++;
        } else {
          s2 = peg$FAILED;

          if (peg$silentFails === 0) {
            peg$fail(peg$c34);
          }
        }

        if (s2 !== peg$FAILED) {
          s3 = peg$parsefieldContainsValue();

          if (s3 !== peg$FAILED) {
            peg$savedPos = s0;
            s1 = peg$c35(s1, s3);
            s0 = s1;
          } else {
            peg$currPos = s0;
            s0 = peg$FAILED;
          }
        } else {
          peg$currPos = s0;
          s0 = peg$FAILED;
        }
      } else {
        peg$currPos = s0;
        s0 = peg$FAILED;
      }

      return s0;
    }

    function peg$parseFieldEXACTValue() {
      var s0, s1, s2, s3;
      s0 = peg$currPos;
      s1 = peg$parsefieldName();

      if (s1 !== peg$FAILED) {
        if (input.charCodeAt(peg$currPos) === 61) {
          s2 = peg$c36;
          peg$currPos++;
        } else {
          s2 = peg$FAILED;

          if (peg$silentFails === 0) {
            peg$fail(peg$c37);
          }
        }

        if (s2 !== peg$FAILED) {
          s3 = peg$parsefieldContainsValue();

          if (s3 !== peg$FAILED) {
            peg$savedPos = s0;
            s1 = peg$c35(s1, s3);
            s0 = s1;
          } else {
            peg$currPos = s0;
            s0 = peg$FAILED;
          }
        } else {
          peg$currPos = s0;
          s0 = peg$FAILED;
        }
      } else {
        peg$currPos = s0;
        s0 = peg$FAILED;
      }

      return s0;
    }

    function peg$parseFieldGTValue() {
      var s0, s1, s2, s3;
      s0 = peg$currPos;
      s1 = peg$parsefieldName();

      if (s1 !== peg$FAILED) {
        if (input.charCodeAt(peg$currPos) === 62) {
          s2 = peg$c38;
          peg$currPos++;
        } else {
          s2 = peg$FAILED;

          if (peg$silentFails === 0) {
            peg$fail(peg$c39);
          }
        }

        if (s2 !== peg$FAILED) {
          s3 = peg$parserangeValue();

          if (s3 !== peg$FAILED) {
            peg$savedPos = s0;
            s1 = peg$c40(s1, s3);
            s0 = s1;
          } else {
            peg$currPos = s0;
            s0 = peg$FAILED;
          }
        } else {
          peg$currPos = s0;
          s0 = peg$FAILED;
        }
      } else {
        peg$currPos = s0;
        s0 = peg$FAILED;
      }

      return s0;
    }

    function peg$parseFieldGTEValue() {
      var s0, s1, s2, s3;
      s0 = peg$currPos;
      s1 = peg$parsefieldName();

      if (s1 !== peg$FAILED) {
        if (input.substr(peg$currPos, 2) === peg$c41) {
          s2 = peg$c41;
          peg$currPos += 2;
        } else {
          s2 = peg$FAILED;

          if (peg$silentFails === 0) {
            peg$fail(peg$c42);
          }
        }

        if (s2 !== peg$FAILED) {
          s3 = peg$parserangeValue();

          if (s3 !== peg$FAILED) {
            peg$savedPos = s0;
            s1 = peg$c40(s1, s3);
            s0 = s1;
          } else {
            peg$currPos = s0;
            s0 = peg$FAILED;
          }
        } else {
          peg$currPos = s0;
          s0 = peg$FAILED;
        }
      } else {
        peg$currPos = s0;
        s0 = peg$FAILED;
      }

      return s0;
    }

    function peg$parseFieldLTValue() {
      var s0, s1, s2, s3;
      s0 = peg$currPos;
      s1 = peg$parsefieldName();

      if (s1 !== peg$FAILED) {
        if (input.charCodeAt(peg$currPos) === 60) {
          s2 = peg$c43;
          peg$currPos++;
        } else {
          s2 = peg$FAILED;

          if (peg$silentFails === 0) {
            peg$fail(peg$c44);
          }
        }

        if (s2 !== peg$FAILED) {
          s3 = peg$parserangeValue();

          if (s3 !== peg$FAILED) {
            peg$savedPos = s0;
            s1 = peg$c40(s1, s3);
            s0 = s1;
          } else {
            peg$currPos = s0;
            s0 = peg$FAILED;
          }
        } else {
          peg$currPos = s0;
          s0 = peg$FAILED;
        }
      } else {
        peg$currPos = s0;
        s0 = peg$FAILED;
      }

      return s0;
    }

    function peg$parseFieldLTEValue() {
      var s0, s1, s2, s3;
      s0 = peg$currPos;
      s1 = peg$parsefieldName();

      if (s1 !== peg$FAILED) {
        if (input.substr(peg$currPos, 2) === peg$c45) {
          s2 = peg$c45;
          peg$currPos += 2;
        } else {
          s2 = peg$FAILED;

          if (peg$silentFails === 0) {
            peg$fail(peg$c46);
          }
        }

        if (s2 !== peg$FAILED) {
          s3 = peg$parserangeValue();

          if (s3 !== peg$FAILED) {
            peg$savedPos = s0;
            s1 = peg$c40(s1, s3);
            s0 = s1;
          } else {
            peg$currPos = s0;
            s0 = peg$FAILED;
          }
        } else {
          peg$currPos = s0;
          s0 = peg$FAILED;
        }
      } else {
        peg$currPos = s0;
        s0 = peg$FAILED;
      }

      return s0;
    }

    function peg$parseflagName() {
      var s0, s1;
      peg$silentFails++;
      s0 = peg$parseidentifier();
      peg$silentFails--;

      if (s0 === peg$FAILED) {
        s1 = peg$FAILED;

        if (peg$silentFails === 0) {
          peg$fail(peg$c47);
        }
      }

      return s0;
    }

    function peg$parsefieldName() {
      var s0, s1;
      peg$silentFails++;
      s0 = peg$parseidentifier();
      peg$silentFails--;

      if (s0 === peg$FAILED) {
        s1 = peg$FAILED;

        if (peg$silentFails === 0) {
          peg$fail(peg$c48);
        }
      }

      return s0;
    }

    function peg$parseidentifier() {
      var s0, s1, s2;
      s0 = peg$currPos;
      s1 = [];
      s2 = peg$parseidentifierChar();

      if (s2 !== peg$FAILED) {
        while (s2 !== peg$FAILED) {
          s1.push(s2);
          s2 = peg$parseidentifierChar();
        }
      } else {
        s1 = peg$FAILED;
      }

      if (s1 !== peg$FAILED) {
        peg$savedPos = s0;
        s1 = peg$c49();
      }

      s0 = s1;
      return s0;
    }

    function peg$parseidentifierChar() {
      var s0;
      s0 = peg$parsealnum();

      if (s0 === peg$FAILED) {
        if (peg$c50.test(input.charAt(peg$currPos))) {
          s0 = input.charAt(peg$currPos);
          peg$currPos++;
        } else {
          s0 = peg$FAILED;

          if (peg$silentFails === 0) {
            peg$fail(peg$c51);
          }
        }

        if (s0 === peg$FAILED) {
          s0 = peg$parseescapedChar();
        }
      }

      return s0;
    }

    function peg$parsefieldContainsValue() {
      var s0, s1;
      peg$silentFails++;
      s0 = peg$parsecontainsOrValues();

      if (s0 === peg$FAILED) {
        s0 = peg$parsecontainsValue();
      }

      peg$silentFails--;

      if (s0 === peg$FAILED) {
        s1 = peg$FAILED;

        if (peg$silentFails === 0) {
          peg$fail(peg$c52);
        }
      }

      return s0;
    }

    function peg$parsetermValue() {
      var s0, s1;
      peg$silentFails++;
      s0 = peg$currPos;
      s1 = peg$parsecontainsValue();

      if (s1 !== peg$FAILED) {
        peg$savedPos = s0;
        s1 = peg$c54(s1);
      }

      s0 = s1;
      peg$silentFails--;

      if (s0 === peg$FAILED) {
        s1 = peg$FAILED;

        if (peg$silentFails === 0) {
          peg$fail(peg$c53);
        }
      }

      return s0;
    }

    function peg$parsecontainsOrValues() {
      var s0, s1, s2, s3, s4, s5, s6, s7, s8, s9;
      s0 = peg$currPos;

      if (input.charCodeAt(peg$currPos) === 40) {
        s1 = peg$c4;
        peg$currPos++;
      } else {
        s1 = peg$FAILED;

        if (peg$silentFails === 0) {
          peg$fail(peg$c5);
        }
      }

      if (s1 !== peg$FAILED) {
        s2 = peg$parsespace();

        if (s2 === peg$FAILED) {
          s2 = null;
        }

        if (s2 !== peg$FAILED) {
          s3 = peg$parsecontainsValue();

          if (s3 !== peg$FAILED) {
            s4 = [];
            s5 = peg$currPos;
            s6 = peg$parsespace();

            if (s6 !== peg$FAILED) {
              s7 = peg$parseorWord();

              if (s7 !== peg$FAILED) {
                s8 = peg$parsespace();

                if (s8 !== peg$FAILED) {
                  s9 = peg$parsecontainsValue();

                  if (s9 !== peg$FAILED) {
                    peg$savedPos = s5;
                    s6 = peg$c55(s3, s9);
                    s5 = s6;
                  } else {
                    peg$currPos = s5;
                    s5 = peg$FAILED;
                  }
                } else {
                  peg$currPos = s5;
                  s5 = peg$FAILED;
                }
              } else {
                peg$currPos = s5;
                s5 = peg$FAILED;
              }
            } else {
              peg$currPos = s5;
              s5 = peg$FAILED;
            }

            while (s5 !== peg$FAILED) {
              s4.push(s5);
              s5 = peg$currPos;
              s6 = peg$parsespace();

              if (s6 !== peg$FAILED) {
                s7 = peg$parseorWord();

                if (s7 !== peg$FAILED) {
                  s8 = peg$parsespace();

                  if (s8 !== peg$FAILED) {
                    s9 = peg$parsecontainsValue();

                    if (s9 !== peg$FAILED) {
                      peg$savedPos = s5;
                      s6 = peg$c55(s3, s9);
                      s5 = s6;
                    } else {
                      peg$currPos = s5;
                      s5 = peg$FAILED;
                    }
                  } else {
                    peg$currPos = s5;
                    s5 = peg$FAILED;
                  }
                } else {
                  peg$currPos = s5;
                  s5 = peg$FAILED;
                }
              } else {
                peg$currPos = s5;
                s5 = peg$FAILED;
              }
            }

            if (s4 !== peg$FAILED) {
              s5 = peg$parsespace();

              if (s5 === peg$FAILED) {
                s5 = null;
              }

              if (s5 !== peg$FAILED) {
                if (input.charCodeAt(peg$currPos) === 41) {
                  s6 = peg$c6;
                  peg$currPos++;
                } else {
                  s6 = peg$FAILED;

                  if (peg$silentFails === 0) {
                    peg$fail(peg$c7);
                  }
                }

                if (s6 !== peg$FAILED) {
                  peg$savedPos = s0;
                  s1 = peg$c56(s3, s4);
                  s0 = s1;
                } else {
                  peg$currPos = s0;
                  s0 = peg$FAILED;
                }
              } else {
                peg$currPos = s0;
                s0 = peg$FAILED;
              }
            } else {
              peg$currPos = s0;
              s0 = peg$FAILED;
            }
          } else {
            peg$currPos = s0;
            s0 = peg$FAILED;
          }
        } else {
          peg$currPos = s0;
          s0 = peg$FAILED;
        }
      } else {
        peg$currPos = s0;
        s0 = peg$FAILED;
      }

      return s0;
    }

    function peg$parserangeValue() {
      var s0;
      s0 = peg$parsenumberWord();

      if (s0 === peg$FAILED) {
        s0 = peg$parsedate();
      }

      return s0;
    }

    function peg$parsecontainsValue() {
      var s0;
      s0 = peg$parsenumberWord();

      if (s0 === peg$FAILED) {
        s0 = peg$parsedate();

        if (s0 === peg$FAILED) {
          s0 = peg$parsebooleanWord();

          if (s0 === peg$FAILED) {
            s0 = peg$parsephrase();

            if (s0 === peg$FAILED) {
              s0 = peg$parseword();
            }
          }
        }
      }

      return s0;
    }

    function peg$parsephrase() {
      var s0, s1, s2, s3, s4, s5, s6, s7, s8, s9;
      s0 = peg$currPos;

      if (input.charCodeAt(peg$currPos) === 34) {
        s1 = peg$c57;
        peg$currPos++;
      } else {
        s1 = peg$FAILED;

        if (peg$silentFails === 0) {
          peg$fail(peg$c58);
        }
      }

      if (s1 !== peg$FAILED) {
        s2 = peg$currPos;
        s3 = peg$parsespace();

        if (s3 === peg$FAILED) {
          s3 = null;
        }

        if (s3 !== peg$FAILED) {
          s4 = [];
          s5 = peg$parsephraseWord();

          if (s5 !== peg$FAILED) {
            while (s5 !== peg$FAILED) {
              s4.push(s5);
              s5 = peg$parsephraseWord();
            }
          } else {
            s4 = peg$FAILED;
          }

          if (s4 === peg$FAILED) {
            s4 = null;
          }

          if (s4 !== peg$FAILED) {
            s5 = [];
            s6 = peg$currPos;
            s7 = peg$parsespace();

            if (s7 !== peg$FAILED) {
              s8 = [];
              s9 = peg$parsephraseWord();

              if (s9 !== peg$FAILED) {
                while (s9 !== peg$FAILED) {
                  s8.push(s9);
                  s9 = peg$parsephraseWord();
                }
              } else {
                s8 = peg$FAILED;
              }

              if (s8 !== peg$FAILED) {
                s7 = [s7, s8];
                s6 = s7;
              } else {
                peg$currPos = s6;
                s6 = peg$FAILED;
              }
            } else {
              peg$currPos = s6;
              s6 = peg$FAILED;
            }

            while (s6 !== peg$FAILED) {
              s5.push(s6);
              s6 = peg$currPos;
              s7 = peg$parsespace();

              if (s7 !== peg$FAILED) {
                s8 = [];
                s9 = peg$parsephraseWord();

                if (s9 !== peg$FAILED) {
                  while (s9 !== peg$FAILED) {
                    s8.push(s9);
                    s9 = peg$parsephraseWord();
                  }
                } else {
                  s8 = peg$FAILED;
                }

                if (s8 !== peg$FAILED) {
                  s7 = [s7, s8];
                  s6 = s7;
                } else {
                  peg$currPos = s6;
                  s6 = peg$FAILED;
                }
              } else {
                peg$currPos = s6;
                s6 = peg$FAILED;
              }
            }

            if (s5 !== peg$FAILED) {
              s6 = peg$parsespace();

              if (s6 === peg$FAILED) {
                s6 = null;
              }

              if (s6 !== peg$FAILED) {
                peg$savedPos = s2;
                s3 = peg$c59();
                s2 = s3;
              } else {
                peg$currPos = s2;
                s2 = peg$FAILED;
              }
            } else {
              peg$currPos = s2;
              s2 = peg$FAILED;
            }
          } else {
            peg$currPos = s2;
            s2 = peg$FAILED;
          }
        } else {
          peg$currPos = s2;
          s2 = peg$FAILED;
        }

        if (s2 !== peg$FAILED) {
          if (input.charCodeAt(peg$currPos) === 34) {
            s3 = peg$c57;
            peg$currPos++;
          } else {
            s3 = peg$FAILED;

            if (peg$silentFails === 0) {
              peg$fail(peg$c58);
            }
          }

          if (s3 !== peg$FAILED) {
            peg$savedPos = s0;
            s1 = peg$c60(s2);
            s0 = s1;
          } else {
            peg$currPos = s0;
            s0 = peg$FAILED;
          }
        } else {
          peg$currPos = s0;
          s0 = peg$FAILED;
        }
      } else {
        peg$currPos = s0;
        s0 = peg$FAILED;
      }

      return s0;
    }

    function peg$parsephraseWord() {
      var s0, s1, s2;
      s0 = [];

      if (peg$c61.test(input.charAt(peg$currPos))) {
        s1 = input.charAt(peg$currPos);
        peg$currPos++;
      } else {
        s1 = peg$FAILED;

        if (peg$silentFails === 0) {
          peg$fail(peg$c62);
        }
      }

      if (s1 !== peg$FAILED) {
        while (s1 !== peg$FAILED) {
          s0.push(s1);

          if (peg$c61.test(input.charAt(peg$currPos))) {
            s1 = input.charAt(peg$currPos);
            peg$currPos++;
          } else {
            s1 = peg$FAILED;

            if (peg$silentFails === 0) {
              peg$fail(peg$c62);
            }
          }
        }
      } else {
        s0 = peg$FAILED;
      }

      if (s0 === peg$FAILED) {
        s0 = peg$currPos;

        if (input.charCodeAt(peg$currPos) === 92) {
          s1 = peg$c63;
          peg$currPos++;
        } else {
          s1 = peg$FAILED;

          if (peg$silentFails === 0) {
            peg$fail(peg$c64);
          }
        }

        if (s1 !== peg$FAILED) {
          if (input.length > peg$currPos) {
            s2 = input.charAt(peg$currPos);
            peg$currPos++;
          } else {
            s2 = peg$FAILED;

            if (peg$silentFails === 0) {
              peg$fail(peg$c65);
            }
          }

          if (s2 !== peg$FAILED) {
            s1 = [s1, s2];
            s0 = s1;
          } else {
            peg$currPos = s0;
            s0 = peg$FAILED;
          }
        } else {
          peg$currPos = s0;
          s0 = peg$FAILED;
        }
      }

      return s0;
    }

    function peg$parseword() {
      var s0, s1, s2;
      s0 = peg$currPos;
      s1 = [];
      s2 = peg$parsewordChar();

      if (s2 !== peg$FAILED) {
        while (s2 !== peg$FAILED) {
          s1.push(s2);
          s2 = peg$parsewordChar();
        }
      } else {
        s1 = peg$FAILED;
      }

      if (s1 !== peg$FAILED) {
        peg$savedPos = s0;
        s1 = peg$c66();
      }

      s0 = s1;
      return s0;
    }

    function peg$parsewordChar() {
      var s0;
      s0 = peg$parsealnum();

      if (s0 === peg$FAILED) {
        if (peg$c67.test(input.charAt(peg$currPos))) {
          s0 = input.charAt(peg$currPos);
          peg$currPos++;
        } else {
          s0 = peg$FAILED;

          if (peg$silentFails === 0) {
            peg$fail(peg$c68);
          }
        }

        if (s0 === peg$FAILED) {
          s0 = peg$parseescapedChar();

          if (s0 === peg$FAILED) {
            s0 = peg$parseextendedGlyph();
          }
        }
      }

      return s0;
    }

    function peg$parseextendedGlyph() {
      var s0;

      if (peg$c69.test(input.charAt(peg$currPos))) {
        s0 = input.charAt(peg$currPos);
        peg$currPos++;
      } else {
        s0 = peg$FAILED;

        if (peg$silentFails === 0) {
          peg$fail(peg$c70);
        }
      }

      return s0;
    }

    function peg$parseescapedChar() {
      var s0, s1, s2;
      s0 = peg$currPos;

      if (input.charCodeAt(peg$currPos) === 92) {
        s1 = peg$c63;
        peg$currPos++;
      } else {
        s1 = peg$FAILED;

        if (peg$silentFails === 0) {
          peg$fail(peg$c64);
        }
      }

      if (s1 !== peg$FAILED) {
        s2 = peg$parsereservedChar();

        if (s2 !== peg$FAILED) {
          s1 = [s1, s2];
          s0 = s1;
        } else {
          peg$currPos = s0;
          s0 = peg$FAILED;
        }
      } else {
        peg$currPos = s0;
        s0 = peg$FAILED;
      }

      return s0;
    }

    function peg$parsereservedChar() {
      var s0;

      if (peg$c71.test(input.charAt(peg$currPos))) {
        s0 = input.charAt(peg$currPos);
        peg$currPos++;
      } else {
        s0 = peg$FAILED;

        if (peg$silentFails === 0) {
          peg$fail(peg$c72);
        }
      }

      return s0;
    }

    function peg$parseorWord() {
      var s0, s1, s2;
      s0 = peg$currPos;

      if (peg$c73.test(input.charAt(peg$currPos))) {
        s1 = input.charAt(peg$currPos);
        peg$currPos++;
      } else {
        s1 = peg$FAILED;

        if (peg$silentFails === 0) {
          peg$fail(peg$c74);
        }
      }

      if (s1 !== peg$FAILED) {
        if (peg$c75.test(input.charAt(peg$currPos))) {
          s2 = input.charAt(peg$currPos);
          peg$currPos++;
        } else {
          s2 = peg$FAILED;

          if (peg$silentFails === 0) {
            peg$fail(peg$c76);
          }
        }

        if (s2 !== peg$FAILED) {
          s1 = [s1, s2];
          s0 = s1;
        } else {
          peg$currPos = s0;
          s0 = peg$FAILED;
        }
      } else {
        peg$currPos = s0;
        s0 = peg$FAILED;
      }

      return s0;
    }

    function peg$parsebooleanWord() {
      var s0, s1, s2, s3;
      s0 = peg$currPos;
      s1 = peg$parseboolean();

      if (s1 !== peg$FAILED) {
        s2 = peg$currPos;
        peg$silentFails++;
        s3 = peg$parsespace();
        peg$silentFails--;

        if (s3 !== peg$FAILED) {
          peg$currPos = s2;
          s2 = void 0;
        } else {
          s2 = peg$FAILED;
        }

        if (s2 !== peg$FAILED) {
          peg$savedPos = s0;
          s1 = peg$c77(s1);
          s0 = s1;
        } else {
          peg$currPos = s0;
          s0 = peg$FAILED;
        }
      } else {
        peg$currPos = s0;
        s0 = peg$FAILED;
      }

      if (s0 === peg$FAILED) {
        s0 = peg$currPos;
        s1 = peg$parseboolean();

        if (s1 !== peg$FAILED) {
          s2 = peg$currPos;
          peg$silentFails++;

          if (input.length > peg$currPos) {
            s3 = input.charAt(peg$currPos);
            peg$currPos++;
          } else {
            s3 = peg$FAILED;

            if (peg$silentFails === 0) {
              peg$fail(peg$c65);
            }
          }

          peg$silentFails--;

          if (s3 === peg$FAILED) {
            s2 = void 0;
          } else {
            peg$currPos = s2;
            s2 = peg$FAILED;
          }

          if (s2 !== peg$FAILED) {
            peg$savedPos = s0;
            s1 = peg$c77(s1);
            s0 = s1;
          } else {
            peg$currPos = s0;
            s0 = peg$FAILED;
          }
        } else {
          peg$currPos = s0;
          s0 = peg$FAILED;
        }
      }

      return s0;
    }

    function peg$parseboolean() {
      var s0, s1, s2, s3, s4, s5;
      s0 = peg$currPos;

      if (peg$c78.test(input.charAt(peg$currPos))) {
        s1 = input.charAt(peg$currPos);
        peg$currPos++;
      } else {
        s1 = peg$FAILED;

        if (peg$silentFails === 0) {
          peg$fail(peg$c79);
        }
      }

      if (s1 !== peg$FAILED) {
        if (peg$c75.test(input.charAt(peg$currPos))) {
          s2 = input.charAt(peg$currPos);
          peg$currPos++;
        } else {
          s2 = peg$FAILED;

          if (peg$silentFails === 0) {
            peg$fail(peg$c76);
          }
        }

        if (s2 !== peg$FAILED) {
          if (peg$c80.test(input.charAt(peg$currPos))) {
            s3 = input.charAt(peg$currPos);
            peg$currPos++;
          } else {
            s3 = peg$FAILED;

            if (peg$silentFails === 0) {
              peg$fail(peg$c81);
            }
          }

          if (s3 !== peg$FAILED) {
            if (peg$c82.test(input.charAt(peg$currPos))) {
              s4 = input.charAt(peg$currPos);
              peg$currPos++;
            } else {
              s4 = peg$FAILED;

              if (peg$silentFails === 0) {
                peg$fail(peg$c83);
              }
            }

            if (s4 !== peg$FAILED) {
              peg$savedPos = s0;
              s1 = peg$c84();
              s0 = s1;
            } else {
              peg$currPos = s0;
              s0 = peg$FAILED;
            }
          } else {
            peg$currPos = s0;
            s0 = peg$FAILED;
          }
        } else {
          peg$currPos = s0;
          s0 = peg$FAILED;
        }
      } else {
        peg$currPos = s0;
        s0 = peg$FAILED;
      }

      if (s0 === peg$FAILED) {
        s0 = peg$currPos;

        if (peg$c85.test(input.charAt(peg$currPos))) {
          s1 = input.charAt(peg$currPos);
          peg$currPos++;
        } else {
          s1 = peg$FAILED;

          if (peg$silentFails === 0) {
            peg$fail(peg$c86);
          }
        }

        if (s1 !== peg$FAILED) {
          if (peg$c87.test(input.charAt(peg$currPos))) {
            s2 = input.charAt(peg$currPos);
            peg$currPos++;
          } else {
            s2 = peg$FAILED;

            if (peg$silentFails === 0) {
              peg$fail(peg$c88);
            }
          }

          if (s2 !== peg$FAILED) {
            if (peg$c89.test(input.charAt(peg$currPos))) {
              s3 = input.charAt(peg$currPos);
              peg$currPos++;
            } else {
              s3 = peg$FAILED;

              if (peg$silentFails === 0) {
                peg$fail(peg$c90);
              }
            }

            if (s3 !== peg$FAILED) {
              if (peg$c91.test(input.charAt(peg$currPos))) {
                s4 = input.charAt(peg$currPos);
                peg$currPos++;
              } else {
                s4 = peg$FAILED;

                if (peg$silentFails === 0) {
                  peg$fail(peg$c92);
                }
              }

              if (s4 !== peg$FAILED) {
                if (peg$c82.test(input.charAt(peg$currPos))) {
                  s5 = input.charAt(peg$currPos);
                  peg$currPos++;
                } else {
                  s5 = peg$FAILED;

                  if (peg$silentFails === 0) {
                    peg$fail(peg$c83);
                  }
                }

                if (s5 !== peg$FAILED) {
                  peg$savedPos = s0;
                  s1 = peg$c84();
                  s0 = s1;
                } else {
                  peg$currPos = s0;
                  s0 = peg$FAILED;
                }
              } else {
                peg$currPos = s0;
                s0 = peg$FAILED;
              }
            } else {
              peg$currPos = s0;
              s0 = peg$FAILED;
            }
          } else {
            peg$currPos = s0;
            s0 = peg$FAILED;
          }
        } else {
          peg$currPos = s0;
          s0 = peg$FAILED;
        }

        if (s0 === peg$FAILED) {
          s0 = peg$currPos;

          if (peg$c93.test(input.charAt(peg$currPos))) {
            s1 = input.charAt(peg$currPos);
            peg$currPos++;
          } else {
            s1 = peg$FAILED;

            if (peg$silentFails === 0) {
              peg$fail(peg$c94);
            }
          }

          if (s1 !== peg$FAILED) {
            if (peg$c82.test(input.charAt(peg$currPos))) {
              s2 = input.charAt(peg$currPos);
              peg$currPos++;
            } else {
              s2 = peg$FAILED;

              if (peg$silentFails === 0) {
                peg$fail(peg$c83);
              }
            }

            if (s2 !== peg$FAILED) {
              if (peg$c91.test(input.charAt(peg$currPos))) {
                s3 = input.charAt(peg$currPos);
                peg$currPos++;
              } else {
                s3 = peg$FAILED;

                if (peg$silentFails === 0) {
                  peg$fail(peg$c92);
                }
              }

              if (s3 !== peg$FAILED) {
                peg$savedPos = s0;
                s1 = peg$c84();
                s0 = s1;
              } else {
                peg$currPos = s0;
                s0 = peg$FAILED;
              }
            } else {
              peg$currPos = s0;
              s0 = peg$FAILED;
            }
          } else {
            peg$currPos = s0;
            s0 = peg$FAILED;
          }

          if (s0 === peg$FAILED) {
            s0 = peg$currPos;

            if (peg$c95.test(input.charAt(peg$currPos))) {
              s1 = input.charAt(peg$currPos);
              peg$currPos++;
            } else {
              s1 = peg$FAILED;

              if (peg$silentFails === 0) {
                peg$fail(peg$c96);
              }
            }

            if (s1 !== peg$FAILED) {
              if (peg$c73.test(input.charAt(peg$currPos))) {
                s2 = input.charAt(peg$currPos);
                peg$currPos++;
              } else {
                s2 = peg$FAILED;

                if (peg$silentFails === 0) {
                  peg$fail(peg$c74);
                }
              }

              if (s2 !== peg$FAILED) {
                peg$savedPos = s0;
                s1 = peg$c84();
                s0 = s1;
              } else {
                peg$currPos = s0;
                s0 = peg$FAILED;
              }
            } else {
              peg$currPos = s0;
              s0 = peg$FAILED;
            }

            if (s0 === peg$FAILED) {
              s0 = peg$currPos;

              if (peg$c73.test(input.charAt(peg$currPos))) {
                s1 = input.charAt(peg$currPos);
                peg$currPos++;
              } else {
                s1 = peg$FAILED;

                if (peg$silentFails === 0) {
                  peg$fail(peg$c74);
                }
              }

              if (s1 !== peg$FAILED) {
                if (peg$c95.test(input.charAt(peg$currPos))) {
                  s2 = input.charAt(peg$currPos);
                  peg$currPos++;
                } else {
                  s2 = peg$FAILED;

                  if (peg$silentFails === 0) {
                    peg$fail(peg$c96);
                  }
                }

                if (s2 !== peg$FAILED) {
                  peg$savedPos = s0;
                  s1 = peg$c84();
                  s0 = s1;
                } else {
                  peg$currPos = s0;
                  s0 = peg$FAILED;
                }
              } else {
                peg$currPos = s0;
                s0 = peg$FAILED;
              }

              if (s0 === peg$FAILED) {
                s0 = peg$currPos;

                if (peg$c73.test(input.charAt(peg$currPos))) {
                  s1 = input.charAt(peg$currPos);
                  peg$currPos++;
                } else {
                  s1 = peg$FAILED;

                  if (peg$silentFails === 0) {
                    peg$fail(peg$c74);
                  }
                }

                if (s1 !== peg$FAILED) {
                  if (peg$c85.test(input.charAt(peg$currPos))) {
                    s2 = input.charAt(peg$currPos);
                    peg$currPos++;
                  } else {
                    s2 = peg$FAILED;

                    if (peg$silentFails === 0) {
                      peg$fail(peg$c86);
                    }
                  }

                  if (s2 !== peg$FAILED) {
                    if (peg$c85.test(input.charAt(peg$currPos))) {
                      s3 = input.charAt(peg$currPos);
                      peg$currPos++;
                    } else {
                      s3 = peg$FAILED;

                      if (peg$silentFails === 0) {
                        peg$fail(peg$c86);
                      }
                    }

                    if (s3 !== peg$FAILED) {
                      peg$savedPos = s0;
                      s1 = peg$c84();
                      s0 = s1;
                    } else {
                      peg$currPos = s0;
                      s0 = peg$FAILED;
                    }
                  } else {
                    peg$currPos = s0;
                    s0 = peg$FAILED;
                  }
                } else {
                  peg$currPos = s0;
                  s0 = peg$FAILED;
                }
              }
            }
          }
        }
      }

      return s0;
    }

    function peg$parsenumber() {
      var s0, s1, s2, s3, s4, s5, s6, s7;
      s0 = peg$currPos;

      if (peg$c97.test(input.charAt(peg$currPos))) {
        s1 = input.charAt(peg$currPos);
        peg$currPos++;
      } else {
        s1 = peg$FAILED;

        if (peg$silentFails === 0) {
          peg$fail(peg$c98);
        }
      }

      if (s1 === peg$FAILED) {
        s1 = null;
      }

      if (s1 !== peg$FAILED) {
        s2 = [];

        if (peg$c99.test(input.charAt(peg$currPos))) {
          s3 = input.charAt(peg$currPos);
          peg$currPos++;
        } else {
          s3 = peg$FAILED;

          if (peg$silentFails === 0) {
            peg$fail(peg$c100);
          }
        }

        if (s3 !== peg$FAILED) {
          while (s3 !== peg$FAILED) {
            s2.push(s3);

            if (peg$c99.test(input.charAt(peg$currPos))) {
              s3 = input.charAt(peg$currPos);
              peg$currPos++;
            } else {
              s3 = peg$FAILED;

              if (peg$silentFails === 0) {
                peg$fail(peg$c100);
              }
            }
          }
        } else {
          s2 = peg$FAILED;
        }

        if (s2 !== peg$FAILED) {
          s3 = [];
          s4 = peg$currPos;

          if (input.charCodeAt(peg$currPos) === 46) {
            s5 = peg$c101;
            peg$currPos++;
          } else {
            s5 = peg$FAILED;

            if (peg$silentFails === 0) {
              peg$fail(peg$c102);
            }
          }

          if (s5 !== peg$FAILED) {
            s6 = [];

            if (peg$c99.test(input.charAt(peg$currPos))) {
              s7 = input.charAt(peg$currPos);
              peg$currPos++;
            } else {
              s7 = peg$FAILED;

              if (peg$silentFails === 0) {
                peg$fail(peg$c100);
              }
            }

            if (s7 !== peg$FAILED) {
              while (s7 !== peg$FAILED) {
                s6.push(s7);

                if (peg$c99.test(input.charAt(peg$currPos))) {
                  s7 = input.charAt(peg$currPos);
                  peg$currPos++;
                } else {
                  s7 = peg$FAILED;

                  if (peg$silentFails === 0) {
                    peg$fail(peg$c100);
                  }
                }
              }
            } else {
              s6 = peg$FAILED;
            }

            if (s6 !== peg$FAILED) {
              s5 = [s5, s6];
              s4 = s5;
            } else {
              peg$currPos = s4;
              s4 = peg$FAILED;
            }
          } else {
            peg$currPos = s4;
            s4 = peg$FAILED;
          }

          while (s4 !== peg$FAILED) {
            s3.push(s4);
            s4 = peg$currPos;

            if (input.charCodeAt(peg$currPos) === 46) {
              s5 = peg$c101;
              peg$currPos++;
            } else {
              s5 = peg$FAILED;

              if (peg$silentFails === 0) {
                peg$fail(peg$c102);
              }
            }

            if (s5 !== peg$FAILED) {
              s6 = [];

              if (peg$c99.test(input.charAt(peg$currPos))) {
                s7 = input.charAt(peg$currPos);
                peg$currPos++;
              } else {
                s7 = peg$FAILED;

                if (peg$silentFails === 0) {
                  peg$fail(peg$c100);
                }
              }

              if (s7 !== peg$FAILED) {
                while (s7 !== peg$FAILED) {
                  s6.push(s7);

                  if (peg$c99.test(input.charAt(peg$currPos))) {
                    s7 = input.charAt(peg$currPos);
                    peg$currPos++;
                  } else {
                    s7 = peg$FAILED;

                    if (peg$silentFails === 0) {
                      peg$fail(peg$c100);
                    }
                  }
                }
              } else {
                s6 = peg$FAILED;
              }

              if (s6 !== peg$FAILED) {
                s5 = [s5, s6];
                s4 = s5;
              } else {
                peg$currPos = s4;
                s4 = peg$FAILED;
              }
            } else {
              peg$currPos = s4;
              s4 = peg$FAILED;
            }
          }

          if (s3 !== peg$FAILED) {
            peg$savedPos = s0;
            s1 = peg$c103();
            s0 = s1;
          } else {
            peg$currPos = s0;
            s0 = peg$FAILED;
          }
        } else {
          peg$currPos = s0;
          s0 = peg$FAILED;
        }
      } else {
        peg$currPos = s0;
        s0 = peg$FAILED;
      }

      return s0;
    }

    function peg$parsenumberWord() {
      var s0, s1, s2, s3;
      s0 = peg$currPos;
      s1 = peg$parsenumber();

      if (s1 !== peg$FAILED) {
        s2 = peg$currPos;
        peg$silentFails++;
        s3 = peg$parsespace();
        peg$silentFails--;

        if (s3 !== peg$FAILED) {
          peg$currPos = s2;
          s2 = void 0;
        } else {
          s2 = peg$FAILED;
        }

        if (s2 !== peg$FAILED) {
          peg$savedPos = s0;
          s1 = peg$c104(s1);
          s0 = s1;
        } else {
          peg$currPos = s0;
          s0 = peg$FAILED;
        }
      } else {
        peg$currPos = s0;
        s0 = peg$FAILED;
      }

      if (s0 === peg$FAILED) {
        s0 = peg$currPos;
        s1 = peg$parsenumber();

        if (s1 !== peg$FAILED) {
          s2 = peg$currPos;
          peg$silentFails++;

          if (input.length > peg$currPos) {
            s3 = input.charAt(peg$currPos);
            peg$currPos++;
          } else {
            s3 = peg$FAILED;

            if (peg$silentFails === 0) {
              peg$fail(peg$c65);
            }
          }

          peg$silentFails--;

          if (s3 === peg$FAILED) {
            s2 = void 0;
          } else {
            peg$currPos = s2;
            s2 = peg$FAILED;
          }

          if (s2 !== peg$FAILED) {
            peg$savedPos = s0;
            s1 = peg$c104(s1);
            s0 = s1;
          } else {
            peg$currPos = s0;
            s0 = peg$FAILED;
          }
        } else {
          peg$currPos = s0;
          s0 = peg$FAILED;
        }
      }

      return s0;
    }

    function peg$parsedate() {
      var s0, s1, s2, s3, s4, s5, s6;
      s0 = peg$currPos;

      if (input.charCodeAt(peg$currPos) === 39) {
        s1 = peg$c105;
        peg$currPos++;
      } else {
        s1 = peg$FAILED;

        if (peg$silentFails === 0) {
          peg$fail(peg$c106);
        }
      }

      if (s1 !== peg$FAILED) {
        s2 = peg$currPos;
        s3 = [];
        s4 = peg$currPos;
        s5 = peg$currPos;
        peg$silentFails++;

        if (input.charCodeAt(peg$currPos) === 39) {
          s6 = peg$c105;
          peg$currPos++;
        } else {
          s6 = peg$FAILED;

          if (peg$silentFails === 0) {
            peg$fail(peg$c106);
          }
        }

        peg$silentFails--;

        if (s6 === peg$FAILED) {
          s5 = void 0;
        } else {
          peg$currPos = s5;
          s5 = peg$FAILED;
        }

        if (s5 !== peg$FAILED) {
          if (input.length > peg$currPos) {
            s6 = input.charAt(peg$currPos);
            peg$currPos++;
          } else {
            s6 = peg$FAILED;

            if (peg$silentFails === 0) {
              peg$fail(peg$c65);
            }
          }

          if (s6 !== peg$FAILED) {
            s5 = [s5, s6];
            s4 = s5;
          } else {
            peg$currPos = s4;
            s4 = peg$FAILED;
          }
        } else {
          peg$currPos = s4;
          s4 = peg$FAILED;
        }

        if (s4 !== peg$FAILED) {
          while (s4 !== peg$FAILED) {
            s3.push(s4);
            s4 = peg$currPos;
            s5 = peg$currPos;
            peg$silentFails++;

            if (input.charCodeAt(peg$currPos) === 39) {
              s6 = peg$c105;
              peg$currPos++;
            } else {
              s6 = peg$FAILED;

              if (peg$silentFails === 0) {
                peg$fail(peg$c106);
              }
            }

            peg$silentFails--;

            if (s6 === peg$FAILED) {
              s5 = void 0;
            } else {
              peg$currPos = s5;
              s5 = peg$FAILED;
            }

            if (s5 !== peg$FAILED) {
              if (input.length > peg$currPos) {
                s6 = input.charAt(peg$currPos);
                peg$currPos++;
              } else {
                s6 = peg$FAILED;

                if (peg$silentFails === 0) {
                  peg$fail(peg$c65);
                }
              }

              if (s6 !== peg$FAILED) {
                s5 = [s5, s6];
                s4 = s5;
              } else {
                peg$currPos = s4;
                s4 = peg$FAILED;
              }
            } else {
              peg$currPos = s4;
              s4 = peg$FAILED;
            }
          }
        } else {
          s3 = peg$FAILED;
        }

        if (s3 !== peg$FAILED) {
          peg$savedPos = s2;
          s3 = peg$c107();
        }

        s2 = s3;

        if (s2 !== peg$FAILED) {
          if (input.charCodeAt(peg$currPos) === 39) {
            s3 = peg$c105;
            peg$currPos++;
          } else {
            s3 = peg$FAILED;

            if (peg$silentFails === 0) {
              peg$fail(peg$c106);
            }
          }

          if (s3 !== peg$FAILED) {
            peg$savedPos = s0;
            s1 = peg$c108(s2);
            s0 = s1;
          } else {
            peg$currPos = s0;
            s0 = peg$FAILED;
          }
        } else {
          peg$currPos = s0;
          s0 = peg$FAILED;
        }
      } else {
        peg$currPos = s0;
        s0 = peg$FAILED;
      }

      return s0;
    }

    function peg$parsealnum() {
      var s0, s1;
      peg$silentFails++;

      if (peg$c110.test(input.charAt(peg$currPos))) {
        s0 = input.charAt(peg$currPos);
        peg$currPos++;
      } else {
        s0 = peg$FAILED;

        if (peg$silentFails === 0) {
          peg$fail(peg$c111);
        }
      }

      peg$silentFails--;

      if (s0 === peg$FAILED) {
        s1 = peg$FAILED;

        if (peg$silentFails === 0) {
          peg$fail(peg$c109);
        }
      }

      return s0;
    }

    function peg$parsespace() {
      var s0, s1;
      peg$silentFails++;
      s0 = [];

      if (peg$c113.test(input.charAt(peg$currPos))) {
        s1 = input.charAt(peg$currPos);
        peg$currPos++;
      } else {
        s1 = peg$FAILED;

        if (peg$silentFails === 0) {
          peg$fail(peg$c114);
        }
      }

      if (s1 !== peg$FAILED) {
        while (s1 !== peg$FAILED) {
          s0.push(s1);

          if (peg$c113.test(input.charAt(peg$currPos))) {
            s1 = input.charAt(peg$currPos);
            peg$currPos++;
          } else {
            s1 = peg$FAILED;

            if (peg$silentFails === 0) {
              peg$fail(peg$c114);
            }
          }
        }
      } else {
        s0 = peg$FAILED;
      }

      peg$silentFails--;

      if (s0 === peg$FAILED) {
        s1 = peg$FAILED;

        if (peg$silentFails === 0) {
          peg$fail(peg$c112);
        }
      }

      return s0;
    }

    var _options = options,
        AST = _options.AST,
        Exp = _options.Exp,
        unescapeValue = _options.unescapeValue,
        unescapePhraseValue = _options.unescapePhraseValue,
        resolveFieldValue = _options.resolveFieldValue;
    var ctx = Object.assign({
      error: error
    }, options);
    peg$result = peg$startRuleFunction();

    if (peg$result !== peg$FAILED && peg$currPos === input.length) {
      return peg$result;
    } else {
      if (peg$result !== peg$FAILED && peg$currPos < input.length) {
        peg$fail(peg$endExpectation());
      }

      throw peg$buildStructuredError(peg$maxFailExpected, peg$maxFailPos < input.length ? input.charAt(peg$maxFailPos) : null, peg$maxFailPos < input.length ? peg$computeLocation(peg$maxFailPos, peg$maxFailPos + 1) : peg$computeLocation(peg$maxFailPos, peg$maxFailPos));
    }
  }

  return {
    SyntaxError: peg$SyntaxError,
    parse: peg$parse
  };
}();

var unescapeValue = function unescapeValue(value) {
  return value.replace(/\\([:\-\\()])/g, '$1');
};

var escapeValue = function escapeValue(value) {
  return value.replace(/([:\-\\()])/g, '\\$1');
};

var unescapePhraseValue = function unescapePhraseValue(value) {
  return value.replace(/\\(.)/g, '$1');
};

var escapePhraseValue = function escapePhraseValue(value) {
  return value.replace(/([\\"])/g, '\\$1');
};

var escapeFieldValue = function escapeFieldValue(value) {
  return value.replace(/(\\)/g, '\\$1');
};

var Exp = {
  date: function date(expression, location) {
    return {
      type: 'date',
      expression: expression,
      location: location
    };
  },
  number: function number(expression, location) {
    return {
      type: 'number',
      expression: expression,
      location: location
    };
  },
  string: function string(expression, location) {
    return {
      type: 'string',
      expression: expression,
      location: location
    };
  },
  boolean: function boolean(expression, location) {
    return {
      type: 'boolean',
      expression: expression,
      location: location
    };
  }
};

var validateFlag = function validateFlag(flag, location, ctx) {
  if (ctx.schema && ctx.schema.strict) {
    if (ctx.schema.flags && ctx.schema.flags.includes(flag)) {
      return;
    }

    if (ctx.schema.fields && ctx.schema.fields[flag] && ctx.schema.fields[flag].type === 'boolean') {
      return;
    }

    ctx.error("Unknown flag `".concat(flag, "`"));
  }
};

var validateFieldValue = function validateFieldValue(field, schemaField, expression, value, location, error) {
  if (schemaField && schemaField.validate) {
    try {
      schemaField.validate(value);
    } catch (e) {
      error("Invalid value `".concat(expression, "` set for field `").concat(field, "` - ").concat(e.message), location);
    }
  }
};

var resolveFieldValue = function resolveFieldValue(field, valueExpression, ctx) {
  var schema = ctx.schema,
      error = ctx.error,
      parseDate = ctx.parseDate;

  if (isArray(valueExpression)) {
    // I don't know if this cast is valid. This function is called recursively and
    // doesn't apply any kind of flat-map.
    return valueExpression.map(function (exp) {
      return resolveFieldValue(field, exp, ctx);
    });
  }

  var location = valueExpression.location;
  var type = valueExpression.type,
      expression = valueExpression.expression;

  if (schema && !schema.fields[field] && schema.strict) {
    error("Unknown field `".concat(field, "`"), location);
  }

  var schemaField = schema && schema.fields[field];

  if (schemaField && schemaField.type !== type && schema.strict) {
    if (schemaField.type === 'string') {
      expression = valueExpression.expression = expression.toString();
      type = valueExpression.type = 'string';
    } else {
      var valueDesc = schemaField.valueDescription || "a ".concat(schemaField.type, " value");
      error("Expected ".concat(valueDesc, " for field `").concat(field, "`, but found `").concat(expression, "`"), location);
    }
  }

  switch (type) {
    case 'date':
      var date = null;

      try {
        date = parseDate(expression);
      } catch (e) {
        error("Invalid data `".concat(expression, "` set for field `").concat(field, "`"), location);
      } // error() throws an exception if called, so now `date` is not null.


      validateFieldValue(field, schemaField, expression, date, location, error);
      return date;

    case 'number':
      var number = Number(expression);

      if (Number.isNaN(number)) {
        error("Invalid number `".concat(expression, "` set for field `").concat(field, "`"), location);
      }

      validateFieldValue(field, schemaField, expression, number, location, error);
      return number;

    case 'boolean':
      // FIXME This would also match 'lion'. It should really anchor the match
      // and the start and end of the input.
      var boolean = !!expression.match(/true|yes|on/i);
      validateFieldValue(field, schemaField, expression, boolean, location, error);
      return boolean;

    default:
      validateFieldValue(field, schemaField, expression, expression, location, error);
      return expression;
  }
};

var printValue = function printValue(value, options) {
  if (isDateValue(value)) {
    return "'".concat(value.text, "'");
  }

  if (isDateLike(value)) {
    var dateFormat = options.dateFormat || defaultDateFormat;
    return "'".concat(dateFormat.print(value), "'");
  }

  if (!isString(value)) {
    return value.toString();
  }

  if (value.length === 0 || value.match(/\s/) || value.toLowerCase() === 'or') {
    return "\"".concat(escapePhraseValue(value), "\"");
  }

  var escapeFn = options.escapeValue || escapeValue;
  return escapeFn(value);
};

var resolveOperator = function resolveOperator(operator) {
  switch (operator) {
    case AST.Operator.EQ:
      return ':';

    case AST.Operator.EXACT:
      return '=';

    case AST.Operator.GT:
      return '>';

    case AST.Operator.GTE:
      return '>=';

    case AST.Operator.LT:
      return '<';

    case AST.Operator.LTE:
      return '<=';

    default:
      throw new Error("unknown field/value operator [".concat(operator, "]"));
  }
};

export var defaultSyntax = Object.freeze({
  parse: function parse(query) {
    var options = arguments.length > 1 && arguments[1] !== undefined ? arguments[1] : {};
    var dateFormat = options.dateFormat || defaultDateFormat;
    var parseDate = dateValueParser(dateFormat);
    var schema = options.schema || {};
    var clauses = parser.parse(query, {
      AST: AST,
      Exp: Exp,
      unescapeValue: unescapeValue,
      unescapePhraseValue: unescapePhraseValue,
      parseDate: parseDate,
      resolveFieldValue: resolveFieldValue,
      validateFlag: validateFlag,
      schema: _objectSpread({
        strict: false,
        flags: [],
        fields: {}
      }, schema)
    });
    return AST.create(clauses);
  },
  printClause: function printClause(clause, text, options) {
    var prefix = AST.Match.isMustClause(clause) ? '' : '-';

    switch (clause.type) {
      case AST.Field.TYPE:
        var op = resolveOperator(clause.operator);

        var printFieldValueOptions = _objectSpread(_objectSpread({}, options), {}, {
          escapeValue: escapeFieldValue
        });

        if (isArray(clause.value)) {
          return "".concat(text, " ").concat(prefix).concat(escapeValue(clause.field)).concat(op, "(").concat(clause.value.map(function (val) {
            return printValue(val, printFieldValueOptions);
          }).join(' or '), ")"); // eslint-disable-line max-len
        }

        return "".concat(text, " ").concat(prefix).concat(escapeValue(clause.field)).concat(op).concat(printValue(clause.value, printFieldValueOptions));

      case AST.Is.TYPE:
        return "".concat(text, " ").concat(prefix, "is:").concat(escapeValue(clause.flag));

      case AST.Term.TYPE:
        return "".concat(text, " ").concat(prefix).concat(printValue(clause.value, options));

      case AST.Group.TYPE:
        return "(".concat(clause.value.map(function (clause) {
          return defaultSyntax.printClause(clause, text, options).trim();
        }).join(' OR '), ")");

      default:
        return text;
    }
  },
  print: function print(ast) {
    var options = arguments.length > 1 && arguments[1] !== undefined ? arguments[1] : {};
    return ast.clauses.reduce(function (text, clause) {
      return defaultSyntax.printClause(clause, text, options);
    }, '').trim();
  }
});