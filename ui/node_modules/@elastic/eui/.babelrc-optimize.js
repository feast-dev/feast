// See `wiki/consuming.md` for rationale and consumer requirements.

const baseConfig = require('./.babelrc.js');
// Skip `propType` generation.
// Still removes type exports for the es builds
baseConfig.plugins.splice(
  baseConfig.plugins.indexOf(
    './scripts/babel/proptypes-from-ts-props'
  ),
  1,
  ['./scripts/babel/proptypes-from-ts-props', { generatePropTypes: false }]
);
// Transform runtimes using babel plugin.
// Requires consuming applications to use `@babel/runtime`.
baseConfig.plugins.push('@babel/plugin-transform-runtime');
baseConfig.env = {};
module.exports = baseConfig;