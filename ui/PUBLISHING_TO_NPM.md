# Publishing the Feast Package to NPM

The Feast UI is published as a module to NPM and can be found here: https://www.npmjs.com/package/@feast-dev/feast-ui

To publish a new version of the module, you will need to be part of the @feast-dev team in NPM. Ask Tony to add you if necessary. You will also need to [login to your NPM account on the command line](https://docs.npmjs.com/cli/v8/commands/npm-adduser).

## Steps for Publishing

1. Make sure tests are passing. Run tests with `yarn tests` in the ui directory.
2. Bump the version number in `package.json` as appropriate.
3. Package the modules for distributions. Run the library build script with `yarn build:lib`. We use [Rollup](https://rollupjs.org/) for building the module, and the configs are in the `rollup.config.js` file.
4. Publish the package to NPM. Run `npm publish`
5. [Check NPM to see that the package was properly publish](https://www.npmjs.com/package/@feast-dev/feast-ui).
