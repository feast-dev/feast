import fs from 'fs';
import path from 'path';
import { parse as parse$1 } from 'postcss';

function parse(string, splitByAnd) {
  const array = [];
  let buffer = '';
  let split = false;
  let func = 0;
  let i = -1;

  while (++i < string.length) {
    const char = string[i];

    if (char === '(') {
      func += 1;
    } else if (char === ')') {
      if (func > 0) {
        func -= 1;
      }
    } else if (func === 0) {
      if (splitByAnd && andRegExp.test(buffer + char)) {
        split = true;
      } else if (!splitByAnd && char === ',') {
        split = true;
      }
    }

    if (split) {
      array.push(splitByAnd ? new MediaExpression(buffer + char) : new MediaQuery(buffer));
      buffer = '';
      split = false;
    } else {
      buffer += char;
    }
  }

  if (buffer !== '') {
    array.push(splitByAnd ? new MediaExpression(buffer) : new MediaQuery(buffer));
  }

  return array;
}

class MediaQueryList {
  constructor(string) {
    this.nodes = parse(string);
  }

  invert() {
    this.nodes.forEach(node => {
      node.invert();
    });
    return this;
  }

  clone() {
    return new MediaQueryList(String(this));
  }

  toString() {
    return this.nodes.join(',');
  }

}

class MediaQuery {
  constructor(string) {
    const [, before, media, after] = string.match(spaceWrapRegExp);
    const [, modifier = '', afterModifier = ' ', type = '', beforeAnd = '', and = '', beforeExpression = '', expression1 = '', expression2 = ''] = media.match(mediaRegExp) || [];
    const raws = {
      before,
      after,
      afterModifier,
      originalModifier: modifier || '',
      beforeAnd,
      and,
      beforeExpression
    };
    const nodes = parse(expression1 || expression2, true);
    Object.assign(this, {
      modifier,
      type,
      raws,
      nodes
    });
  }

  clone(overrides) {
    const instance = new MediaQuery(String(this));
    Object.assign(instance, overrides);
    return instance;
  }

  invert() {
    this.modifier = this.modifier ? '' : this.raws.originalModifier;
    return this;
  }

  toString() {
    const {
      raws
    } = this;
    return `${raws.before}${this.modifier}${this.modifier ? `${raws.afterModifier}` : ''}${this.type}${raws.beforeAnd}${raws.and}${raws.beforeExpression}${this.nodes.join('')}${this.raws.after}`;
  }

}

class MediaExpression {
  constructor(string) {
    const [, value, after = '', and = '', afterAnd = ''] = string.match(andRegExp) || [null, string];
    const raws = {
      after,
      and,
      afterAnd
    };
    Object.assign(this, {
      value,
      raws
    });
  }

  clone(overrides) {
    const instance = new MediaExpression(String(this));
    Object.assign(instance, overrides);
    return instance;
  }

  toString() {
    const {
      raws
    } = this;
    return `${this.value}${raws.after}${raws.and}${raws.afterAnd}`;
  }

}

const modifierRE = '(not|only)';
const typeRE = '(all|print|screen|speech)';
const noExpressionRE = '([\\W\\w]*)';
const expressionRE = '([\\W\\w]+)';
const noSpaceRE = '(\\s*)';
const spaceRE = '(\\s+)';
const andRE = '(?:(\\s+)(and))';
const andRegExp = new RegExp(`^${expressionRE}(?:${andRE}${spaceRE})$`, 'i');
const spaceWrapRegExp = new RegExp(`^${noSpaceRE}${noExpressionRE}${noSpaceRE}$`);
const mediaRegExp = new RegExp(`^(?:${modifierRE}${spaceRE})?(?:${typeRE}(?:${andRE}${spaceRE}${expressionRE})?|${expressionRE})$`, 'i');
var mediaASTFromString = (string => new MediaQueryList(string));

var getCustomMediaFromRoot = ((root, opts) => {
  // initialize custom selectors
  const customMedias = {}; // for each custom selector atrule that is a child of the css root

  root.nodes.slice().forEach(node => {
    if (isCustomMedia(node)) {
      // extract the name and selectors from the params of the custom selector
      const [, name, selectors] = node.params.match(customMediaParamsRegExp); // write the parsed selectors to the custom selector

      customMedias[name] = mediaASTFromString(selectors); // conditionally remove the custom selector atrule

      if (!Object(opts).preserve) {
        node.remove();
      }
    }
  });
  return customMedias;
}); // match the custom selector name

const customMediaNameRegExp = /^custom-media$/i; // match the custom selector params

const customMediaParamsRegExp = /^(--[A-z][\w-]*)\s+([\W\w]+)\s*$/; // whether the atrule is a custom selector

const isCustomMedia = node => node.type === 'atrule' && customMediaNameRegExp.test(node.name) && customMediaParamsRegExp.test(node.params);

/* Get Custom Media from CSS File
/* ========================================================================== */

async function getCustomMediaFromCSSFile(from) {
  const css = await readFile(from);
  const root = parse$1(css, {
    from
  });
  return getCustomMediaFromRoot(root, {
    preserve: true
  });
}
/* Get Custom Media from Object
/* ========================================================================== */


function getCustomMediaFromObject(object) {
  const customMedia = Object.assign({}, Object(object).customMedia, Object(object)['custom-media']);

  for (const key in customMedia) {
    customMedia[key] = mediaASTFromString(customMedia[key]);
  }

  return customMedia;
}
/* Get Custom Media from JSON file
/* ========================================================================== */


async function getCustomMediaFromJSONFile(from) {
  const object = await readJSON(from);
  return getCustomMediaFromObject(object);
}
/* Get Custom Media from JS file
/* ========================================================================== */


async function getCustomMediaFromJSFile(from) {
  const object = await import(from);
  return getCustomMediaFromObject(object);
}
/* Get Custom Media from Sources
/* ========================================================================== */


function getCustomMediaFromSources(sources) {
  return sources.map(source => {
    if (source instanceof Promise) {
      return source;
    } else if (source instanceof Function) {
      return source();
    } // read the source as an object


    const opts = source === Object(source) ? source : {
      from: String(source)
    }; // skip objects with custom media

    if (Object(opts).customMedia || Object(opts)['custom-media']) {
      return opts;
    } // source pathname


    const from = path.resolve(String(opts.from || '')); // type of file being read from

    const type = (opts.type || path.extname(from).slice(1)).toLowerCase();
    return {
      type,
      from
    };
  }).reduce(async (customMedia, source) => {
    const {
      type,
      from
    } = await source;

    if (type === 'css' || type === 'pcss') {
      return Object.assign(await customMedia, await getCustomMediaFromCSSFile(from));
    }

    if (type === 'js') {
      return Object.assign(await customMedia, await getCustomMediaFromJSFile(from));
    }

    if (type === 'json') {
      return Object.assign(await customMedia, await getCustomMediaFromJSONFile(from));
    }

    return Object.assign(await customMedia, getCustomMediaFromObject(await source));
  }, {});
}
/* Helper utilities
/* ========================================================================== */

const readFile = from => new Promise((resolve, reject) => {
  fs.readFile(from, 'utf8', (error, result) => {
    if (error) {
      reject(error);
    } else {
      resolve(result);
    }
  });
});

const readJSON = async from => JSON.parse(await readFile(from));

// return transformed medias, replacing custom pseudo medias with custom medias
function transformMediaList(mediaList, customMedias) {
  let index = mediaList.nodes.length - 1;

  while (index >= 0) {
    const transformedMedias = transformMedia(mediaList.nodes[index], customMedias);

    if (transformedMedias.length) {
      mediaList.nodes.splice(index, 1, ...transformedMedias);
    }

    --index;
  }

  return mediaList;
} // return custom pseudo medias replaced with custom medias

function transformMedia(media, customMedias) {
  const transpiledMedias = [];

  for (const index in media.nodes) {
    const {
      value,
      nodes
    } = media.nodes[index];
    const key = value.replace(customPseudoRegExp, '$1');

    if (key in customMedias) {
      for (const replacementMedia of customMedias[key].nodes) {
        // use the first available modifier unless they cancel each other out
        const modifier = media.modifier !== replacementMedia.modifier ? media.modifier || replacementMedia.modifier : '';
        const mediaClone = media.clone({
          modifier,
          // conditionally use the raws from the first available modifier
          raws: !modifier || media.modifier ? { ...media.raws
          } : { ...replacementMedia.raws
          },
          type: media.type || replacementMedia.type
        }); // conditionally include more replacement raws when the type is present

        if (mediaClone.type === replacementMedia.type) {
          Object.assign(mediaClone.raws, {
            and: replacementMedia.raws.and,
            beforeAnd: replacementMedia.raws.beforeAnd,
            beforeExpression: replacementMedia.raws.beforeExpression
          });
        }

        mediaClone.nodes.splice(index, 1, ...replacementMedia.clone().nodes.map(node => {
          // use raws and spacing from the current usage
          if (media.nodes[index].raws.and) {
            node.raws = { ...media.nodes[index].raws
            };
          }

          node.spaces = { ...media.nodes[index].spaces
          };
          return node;
        })); // remove the currently transformed key to prevent recursion

        const nextCustomMedia = getCustomMediasWithoutKey(customMedias, key);
        const retranspiledMedias = transformMedia(mediaClone, nextCustomMedia);

        if (retranspiledMedias.length) {
          transpiledMedias.push(...retranspiledMedias);
        } else {
          transpiledMedias.push(mediaClone);
        }
      }

      return transpiledMedias;
    } else if (nodes && nodes.length) {
      transformMediaList(media.nodes[index], customMedias);
    }
  }

  return transpiledMedias;
}

const customPseudoRegExp = /\((--[A-z][\w-]*)\)/;

const getCustomMediasWithoutKey = (customMedias, key) => {
  const nextCustomMedias = Object.assign({}, customMedias);
  delete nextCustomMedias[key];
  return nextCustomMedias;
};

var transformAtrules = ((root, customMedia, opts) => {
  root.walkAtRules(mediaAtRuleRegExp, atrule => {
    if (customPseudoRegExp$1.test(atrule.params)) {
      const mediaAST = mediaASTFromString(atrule.params);
      const params = String(transformMediaList(mediaAST, customMedia));

      if (opts.preserve) {
        atrule.cloneBefore({
          params
        });
      } else {
        atrule.params = params;
      }
    }
  });
});
const mediaAtRuleRegExp = /^media$/i;
const customPseudoRegExp$1 = /\(--[A-z][\w-]*\)/;

/* Write Custom Media from CSS File
/* ========================================================================== */

async function writeCustomMediaToCssFile(to, customMedia) {
  const cssContent = Object.keys(customMedia).reduce((cssLines, name) => {
    cssLines.push(`@custom-media ${name} ${customMedia[name]};`);
    return cssLines;
  }, []).join('\n');
  const css = `${cssContent}\n`;
  await writeFile(to, css);
}
/* Write Custom Media from JSON file
/* ========================================================================== */


async function writeCustomMediaToJsonFile(to, customMedia) {
  const jsonContent = JSON.stringify({
    'custom-media': customMedia
  }, null, '  ');
  const json = `${jsonContent}\n`;
  await writeFile(to, json);
}
/* Write Custom Media from Common JS file
/* ========================================================================== */


async function writeCustomMediaToCjsFile(to, customMedia) {
  const jsContents = Object.keys(customMedia).reduce((jsLines, name) => {
    jsLines.push(`\t\t'${escapeForJS(name)}': '${escapeForJS(customMedia[name])}'`);
    return jsLines;
  }, []).join(',\n');
  const js = `module.exports = {\n\tcustomMedia: {\n${jsContents}\n\t}\n};\n`;
  await writeFile(to, js);
}
/* Write Custom Media from Module JS file
/* ========================================================================== */


async function writeCustomMediaToMjsFile(to, customMedia) {
  const mjsContents = Object.keys(customMedia).reduce((mjsLines, name) => {
    mjsLines.push(`\t'${escapeForJS(name)}': '${escapeForJS(customMedia[name])}'`);
    return mjsLines;
  }, []).join(',\n');
  const mjs = `export const customMedia = {\n${mjsContents}\n};\n`;
  await writeFile(to, mjs);
}
/* Write Custom Media to Exports
/* ========================================================================== */


function writeCustomMediaToExports(customMedia, destinations) {
  return Promise.all(destinations.map(async destination => {
    if (destination instanceof Function) {
      await destination(defaultCustomMediaToJSON(customMedia));
    } else {
      // read the destination as an object
      const opts = destination === Object(destination) ? destination : {
        to: String(destination)
      }; // transformer for custom media into a JSON-compatible object

      const toJSON = opts.toJSON || defaultCustomMediaToJSON;

      if ('customMedia' in opts) {
        // write directly to an object as customMedia
        opts.customMedia = toJSON(customMedia);
      } else if ('custom-media' in opts) {
        // write directly to an object as custom-media
        opts['custom-media'] = toJSON(customMedia);
      } else {
        // destination pathname
        const to = String(opts.to || ''); // type of file being written to

        const type = (opts.type || path.extname(to).slice(1)).toLowerCase(); // transformed custom media

        const customMediaJSON = toJSON(customMedia);

        if (type === 'css') {
          await writeCustomMediaToCssFile(to, customMediaJSON);
        }

        if (type === 'js') {
          await writeCustomMediaToCjsFile(to, customMediaJSON);
        }

        if (type === 'json') {
          await writeCustomMediaToJsonFile(to, customMediaJSON);
        }

        if (type === 'mjs') {
          await writeCustomMediaToMjsFile(to, customMediaJSON);
        }
      }
    }
  }));
}
/* Helper utilities
/* ========================================================================== */

const defaultCustomMediaToJSON = customMedia => {
  return Object.keys(customMedia).reduce((customMediaJSON, key) => {
    customMediaJSON[key] = String(customMedia[key]);
    return customMediaJSON;
  }, {});
};

const writeFile = (to, text) => new Promise((resolve, reject) => {
  fs.writeFile(to, text, error => {
    if (error) {
      reject(error);
    } else {
      resolve();
    }
  });
});

const escapeForJS = string => string.replace(/\\([\s\S])|(')/g, '\\$1$2').replace(/\n/g, '\\n').replace(/\r/g, '\\r');

const creator = opts => {
  // whether to preserve custom media and at-rules using them
  const preserve = 'preserve' in Object(opts) ? Boolean(opts.preserve) : false; // sources to import custom media from

  const importFrom = [].concat(Object(opts).importFrom || []); // destinations to export custom media to

  const exportTo = [].concat(Object(opts).exportTo || []); // promise any custom media are imported

  const customMediaPromise = getCustomMediaFromSources(importFrom);
  return {
    postcssPlugin: 'postcss-custom-media',
    Once: async root => {
      const customMedia = Object.assign(await customMediaPromise, getCustomMediaFromRoot(root, {
        preserve
      }));
      await writeCustomMediaToExports(customMedia, exportTo);
      transformAtrules(root, customMedia, {
        preserve
      });
    }
  };
};

creator.postcss = true;

export default creator;
//# sourceMappingURL=index.es.mjs.map
