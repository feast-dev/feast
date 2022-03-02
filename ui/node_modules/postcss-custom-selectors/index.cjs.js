'use strict';

var parser = require('postcss-selector-parser');
var fs = require('fs');
var path = require('path');
var postcss = require('postcss');

function _interopDefaultLegacy (e) { return e && typeof e === 'object' && 'default' in e ? e : { 'default': e }; }

function _interopNamespace(e) {
	if (e && e.__esModule) return e;
	var n = Object.create(null);
	if (e) {
		Object.keys(e).forEach(function (k) {
			if (k !== 'default') {
				var d = Object.getOwnPropertyDescriptor(e, k);
				Object.defineProperty(n, k, d.get ? d : {
					enumerable: true,
					get: function () {
						return e[k];
					}
				});
			}
		});
	}
	n['default'] = e;
	return Object.freeze(n);
}

var parser__default = /*#__PURE__*/_interopDefaultLegacy(parser);
var fs__default = /*#__PURE__*/_interopDefaultLegacy(fs);
var path__default = /*#__PURE__*/_interopDefaultLegacy(path);
var postcss__default = /*#__PURE__*/_interopDefaultLegacy(postcss);

/* Return a Selectors AST from a Selectors String
/* ========================================================================== */

var getSelectorsAstFromSelectorsString = (selectorString => {
  let selectorAST;
  parser__default['default'](selectors => {
    selectorAST = selectors;
  }).processSync(selectorString);
  return selectorAST;
});

var getCustomSelectors = ((root, opts) => {
  // initialize custom selectors
  const customSelectors = {}; // for each custom selector atrule that is a child of the css root

  root.nodes.slice().forEach(node => {
    if (isCustomSelector(node)) {
      // extract the name and selectors from the params of the custom selector
      const [, name, selectors] = node.params.match(customSelectorParamsRegExp); // write the parsed selectors to the custom selector

      customSelectors[name] = getSelectorsAstFromSelectorsString(selectors); // conditionally remove the custom selector atrule

      if (!Object(opts).preserve) {
        node.remove();
      }
    }
  });
  return customSelectors;
}); // match the custom selector name

const customSelectorNameRegExp = /^custom-selector$/i; // match the custom selector params

const customSelectorParamsRegExp = /^(:--[A-z][\w-]*)\s+([\W\w]+)\s*$/; // whether the atrule is a custom selector

const isCustomSelector = node => node.type === 'atrule' && customSelectorNameRegExp.test(node.name) && customSelectorParamsRegExp.test(node.params);

// return transformed selectors, replacing custom pseudo selectors with custom selectors
function transformSelectorList(selectorList, customSelectors) {
  let index = selectorList.nodes.length - 1;

  while (index >= 0) {
    const transformedSelectors = transformSelector(selectorList.nodes[index], customSelectors);

    if (transformedSelectors.length) {
      selectorList.nodes.splice(index, 1, ...transformedSelectors);
    }

    --index;
  }

  return selectorList;
} // return custom pseudo selectors replaced with custom selectors

function transformSelector(selector, customSelectors) {
  const transpiledSelectors = [];

  for (const index in selector.nodes) {
    const {
      value,
      nodes
    } = selector.nodes[index];

    if (value in customSelectors) {
      for (const replacementSelector of customSelectors[value].nodes) {
        const selectorClone = selector.clone();
        selectorClone.nodes.splice(index, 1, ...replacementSelector.clone().nodes.map(node => {
          // use spacing from the current usage
          node.spaces = { ...selector.nodes[index].spaces
          };
          return node;
        }));
        const retranspiledSelectors = transformSelector(selectorClone, customSelectors);
        adjustNodesBySelectorEnds(selectorClone.nodes, Number(index));

        if (retranspiledSelectors.length) {
          transpiledSelectors.push(...retranspiledSelectors);
        } else {
          transpiledSelectors.push(selectorClone);
        }
      }

      return transpiledSelectors;
    } else if (nodes && nodes.length) {
      transformSelectorList(selector.nodes[index], customSelectors);
    }
  }

  return transpiledSelectors;
} // match selectors by difficult-to-separate ends


const withoutSelectorStartMatch = /^(tag|universal)$/;
const withoutSelectorEndMatch = /^(class|id|pseudo|tag|universal)$/;

const isWithoutSelectorStart = node => withoutSelectorStartMatch.test(Object(node).type);

const isWithoutSelectorEnd = node => withoutSelectorEndMatch.test(Object(node).type); // adjust nodes by selector ends (so that .class:--h1 becomes h1.class rather than .classh1)


const adjustNodesBySelectorEnds = (nodes, index) => {
  if (index && isWithoutSelectorStart(nodes[index]) && isWithoutSelectorEnd(nodes[index - 1])) {
    let safeIndex = index - 1;

    while (safeIndex && isWithoutSelectorEnd(nodes[safeIndex])) {
      --safeIndex;
    }

    if (safeIndex < index) {
      const node = nodes.splice(index, 1)[0];
      nodes.splice(safeIndex, 0, node);
      nodes[safeIndex].spaces.before = nodes[safeIndex + 1].spaces.before;
      nodes[safeIndex + 1].spaces.before = '';

      if (nodes[index]) {
        nodes[index].spaces.after = nodes[safeIndex].spaces.after;
        nodes[safeIndex].spaces.after = '';
      }
    }
  }
};

var transformRules = ((root, customSelectors, opts) => {
  root.walkRules(customPseudoRegExp, rule => {
    const selector = parser__default['default'](selectors => {
      transformSelectorList(selectors, customSelectors);
    }).processSync(rule.selector);

    if (opts.preserve) {
      rule.cloneBefore({
        selector
      });
    } else {
      rule.selector = selector;
    }
  });
});
const customPseudoRegExp = /:--[A-z][\w-]*/;

/* Import Custom Selectors from CSS AST
/* ========================================================================== */

function importCustomSelectorsFromCSSAST(root) {
  return getCustomSelectors(root);
}
/* Import Custom Selectors from CSS File
/* ========================================================================== */


async function importCustomSelectorsFromCSSFile(from) {
  const css = await readFile(path__default['default'].resolve(from));
  const root = postcss__default['default'].parse(css, {
    from: path__default['default'].resolve(from)
  });
  return importCustomSelectorsFromCSSAST(root);
}
/* Import Custom Selectors from Object
/* ========================================================================== */


function importCustomSelectorsFromObject(object) {
  const customSelectors = Object.assign({}, Object(object).customSelectors || Object(object)['custom-selectors']);

  for (const key in customSelectors) {
    customSelectors[key] = getSelectorsAstFromSelectorsString(customSelectors[key]);
  }

  return customSelectors;
}
/* Import Custom Selectors from JSON file
/* ========================================================================== */


async function importCustomSelectorsFromJSONFile(from) {
  const object = await readJSON(path__default['default'].resolve(from));
  return importCustomSelectorsFromObject(object);
}
/* Import Custom Selectors from JS file
/* ========================================================================== */


async function importCustomSelectorsFromJSFile(from) {
  const object = await Promise.resolve().then(function () { return /*#__PURE__*/_interopNamespace(require(path__default['default'].resolve(from))); });
  return importCustomSelectorsFromObject(object);
}
/* Import Custom Selectors from Sources
/* ========================================================================== */


function importCustomSelectorsFromSources(sources) {
  return sources.map(source => {
    if (source instanceof Promise) {
      return source;
    } else if (source instanceof Function) {
      return source();
    } // read the source as an object


    const opts = source === Object(source) ? source : {
      from: String(source)
    }; // skip objects with custom selectors

    if (Object(opts).customSelectors || Object(opts)['custom-selectors']) {
      return opts;
    } // source pathname


    const from = String(opts.from || ''); // type of file being read from

    const type = (opts.type || path__default['default'].extname(from).slice(1)).toLowerCase();
    return {
      type,
      from
    };
  }).reduce(async (customSelectorsPromise, source) => {
    const customSelectors = await customSelectorsPromise;
    const {
      type,
      from
    } = await source;

    if (type === 'ast') {
      return Object.assign(customSelectors, importCustomSelectorsFromCSSAST(from));
    }

    if (type === 'css') {
      return Object.assign(customSelectors, await importCustomSelectorsFromCSSFile(from));
    }

    if (type === 'js') {
      return Object.assign(customSelectors, await importCustomSelectorsFromJSFile(from));
    }

    if (type === 'json') {
      return Object.assign(customSelectors, await importCustomSelectorsFromJSONFile(from));
    }

    return Object.assign(customSelectors, importCustomSelectorsFromObject(await source));
  }, Promise.resolve({}));
}
/* Helper utilities
/* ========================================================================== */

const readFile = from => new Promise((resolve, reject) => {
  fs__default['default'].readFile(from, 'utf8', (error, result) => {
    if (error) {
      reject(error);
    } else {
      resolve(result);
    }
  });
});

const readJSON = async from => JSON.parse(await readFile(from));

/* Import Custom Selectors from CSS File
/* ========================================================================== */

async function exportCustomSelectorsToCssFile(to, customSelectors) {
  const cssContent = Object.keys(customSelectors).reduce((cssLines, name) => {
    cssLines.push(`@custom-selector ${name} ${customSelectors[name]};`);
    return cssLines;
  }, []).join('\n');
  const css = `${cssContent}\n`;
  await writeFile(to, css);
}
/* Import Custom Selectors from JSON file
/* ========================================================================== */


async function exportCustomSelectorsToJsonFile(to, customSelectors) {
  const jsonContent = JSON.stringify({
    'custom-selectors': customSelectors
  }, null, '  ');
  const json = `${jsonContent}\n`;
  await writeFile(to, json);
}
/* Import Custom Selectors from Common JS file
/* ========================================================================== */


async function exportCustomSelectorsToCjsFile(to, customSelectors) {
  const jsContents = Object.keys(customSelectors).reduce((jsLines, name) => {
    jsLines.push(`\t\t'${escapeForJS(name)}': '${escapeForJS(customSelectors[name])}'`);
    return jsLines;
  }, []).join(',\n');
  const js = `module.exports = {\n\tcustomSelectors: {\n${jsContents}\n\t}\n};\n`;
  await writeFile(to, js);
}
/* Import Custom Selectors from Module JS file
/* ========================================================================== */


async function exportCustomSelectorsToMjsFile(to, customSelectors) {
  const mjsContents = Object.keys(customSelectors).reduce((mjsLines, name) => {
    mjsLines.push(`\t'${escapeForJS(name)}': '${escapeForJS(customSelectors[name])}'`);
    return mjsLines;
  }, []).join(',\n');
  const mjs = `export const customSelectors = {\n${mjsContents}\n};\n`;
  await writeFile(to, mjs);
}
/* Export Custom Selectors to Destinations
/* ========================================================================== */


function exportCustomSelectorsToDestinations(customSelectors, destinations) {
  return Promise.all(destinations.map(async destination => {
    if (destination instanceof Function) {
      await destination(defaultCustomSelectorsToJSON(customSelectors));
    } else {
      // read the destination as an object
      const opts = destination === Object(destination) ? destination : {
        to: String(destination)
      }; // transformer for custom selectors into a JSON-compatible object

      const toJSON = opts.toJSON || defaultCustomSelectorsToJSON;

      if ('customSelectors' in opts) {
        // write directly to an object as customSelectors
        opts.customSelectors = toJSON(customSelectors);
      } else if ('custom-selectors' in opts) {
        // write directly to an object as custom-selectors
        opts['custom-selectors'] = toJSON(customSelectors);
      } else {
        // destination pathname
        const to = String(opts.to || ''); // type of file being written to

        const type = (opts.type || path__default['default'].extname(opts.to).slice(1)).toLowerCase(); // transformed custom selectors

        const customSelectorsJSON = toJSON(customSelectors);

        if (type === 'css') {
          await exportCustomSelectorsToCssFile(to, customSelectorsJSON);
        }

        if (type === 'js') {
          await exportCustomSelectorsToCjsFile(to, customSelectorsJSON);
        }

        if (type === 'json') {
          await exportCustomSelectorsToJsonFile(to, customSelectorsJSON);
        }

        if (type === 'mjs') {
          await exportCustomSelectorsToMjsFile(to, customSelectorsJSON);
        }
      }
    }
  }));
}
/* Helper utilities
/* ========================================================================== */

const defaultCustomSelectorsToJSON = customSelectors => {
  return Object.keys(customSelectors).reduce((customSelectorsJSON, key) => {
    customSelectorsJSON[key] = String(customSelectors[key]);
    return customSelectorsJSON;
  }, {});
};

const writeFile = (to, text) => new Promise((resolve, reject) => {
  fs__default['default'].writeFile(to, text, error => {
    if (error) {
      reject(error);
    } else {
      resolve();
    }
  });
});

const escapeForJS = string => string.replace(/\\([\s\S])|(')/g, '\\$1$2').replace(/\n/g, '\\n').replace(/\r/g, '\\r');

const postcssCustomSelectors = opts => {
  // whether to preserve custom selectors and rules using them
  const preserve = Boolean(Object(opts).preserve); // sources to import custom selectors from

  const importFrom = [].concat(Object(opts).importFrom || []); // destinations to export custom selectors to

  const exportTo = [].concat(Object(opts).exportTo || []); // promise any custom selectors are imported

  const customSelectorsPromise = importCustomSelectorsFromSources(importFrom);
  return {
    postcssPlugin: 'postcss-custom-selectors',

    async Once(root) {
      const customProperties = Object.assign({}, await customSelectorsPromise, getCustomSelectors(root, {
        preserve
      }));
      await exportCustomSelectorsToDestinations(customProperties, exportTo);
      transformRules(root, customProperties, {
        preserve
      });
    }

  };
};

postcssCustomSelectors.postcss = true;

module.exports = postcssCustomSelectors;
//# sourceMappingURL=index.cjs.js.map
