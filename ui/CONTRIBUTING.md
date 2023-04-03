<h2>Table of contents</h2>

- [General contributor notes](#general-contributor-notes)
  - [`feast ui` command](#feast-ui-command)
  - [NPM package project structure](#npm-package-project-structure)
  - [Tests](#tests)
  - [Yarn commands](#yarn-commands)
    - [`yarn install`](#yarn-install)
    - [`yarn start`](#yarn-start)
    - [`yarn test`](#yarn-test)
- [Release process](#release-process)
  - [(Advanced) Manually publishing the Feast Package to NPM](#advanced-manually-publishing-the-feast-package-to-npm)
    - [Requirements](#requirements)
    - [Steps for Publishing](#steps-for-publishing)

# General contributor notes
In this doc, we describe how to contribute both to the Feast Web UI NPM package as well as the embedded Feast UI in the Python SDK (i.e. what's run when you run `feast ui`)

## `feast ui` command
You can see the logic in [../sdk/python/feast/ui](../sdk/python/feast/ui/). This instance is loaded in [../sdk/python/feast/ui_server.py](../sdk/python/feast/ui_server.py). 

Under the hood, what happens is that the Feast SDK spins up a server which exposes an endpoint to the registry. It then mounts the UI on the server and points it to fetch data from that registry.

## NPM package project structure
The Web UI is powered by a JSON registry dump from Feast (running `feast registry-dump`). Running `yarn start` launches a UI
powered by test data. 
- `public/` contains assets as well as demo data loaded by the Web UI.
  - There is a `projects-list.json` which represents all Feast projects the UI shows. 
  - There is also a `registry.json` which is the registry dump for the feature repo.
- `feature_repo/` contains a sample Feast repo which generates the `registry.json`
- `src/` contains the Web UI source code. 
   - `src/contexts` has React context objects around project level metadata or registry path metadata to inject into pages. The contexts are static contexts provided by [FeastUISansProviders.tsx](src/FeastUISansProviders.tsx)
   - `src/parsers` parses the `registry.json` into in memory representations of Feast objects (feature views, data sources, entities, feature services). 
     - This has ~1:1 mappings to the protobuf objects in [feast/protos/feast/core](https://github.com/feast-dev/feast/tree/master/protos/feast/core). 
     - There are also "relationships" which create an in-memory lineage graph which can be used to construct links in pages. 
     - This generates state which pages will load via React queries (to the registry path).
   - `src/pages` has all individual web pages and their layouts. For any given Feast object (e.g. entity), there exist:
     - an **Index page** (which is the first page you hit when you click on that object). This loads using a React query the in memory representation of all objects (parsed from `src/parsers`) and embeds:
       - a **Listing page** (i.e. listing all the objects in the registry in a table). This creates links to the instance pages
     - an **Instance page** (which shows details for an individual entity, feature view, etc). This embeds:
       - a default Overview tab, which shows all the Feast metadata (e.g. for a given entity)
       - custom tabs from `src/custom-tabs`.
   - Other subdirectories:
     - `src/components` has common React components that are re-used across pages
     - `src/custom-tabs` houses custom tabs and a custom tab React context which exist on the core pages. There is a `TabsRegistryContext` which is also supplied by the [FeastUISansProviders.tsx](src/FeastUISansProviders.tsx), and if there are custom tabs, the Feast UI will embed them as a new tab in the corresponding page (e.g. feature view page).
     - `src/graphics` houses icons that are used throughout the UI
     - `src/hooks` has React hooks. The most complex hooks here define the bulk of the search / filter functionality.

## Tests
There are very few tests for this UI. There is a smoke test that ensures pages can load in [FeastUISansProviders.test.tsx](src/FeastUISansProviders.test.tsx)


## Yarn commands

If you would like to simply try things out and see how the UI works, you can simply run the code in this repo. 

> **Note**: there is an `.npmrc` which is setup for automatic releases. You'll need to comment out the line in there and continue

First:

### `yarn install` 

That will install the all the dependencies that the UI needs, as well as development dependencies. Then in the project directory, you can run:

### `yarn start`

Runs the app in the development mode.\
Open [http://localhost:3000](http://localhost:3000) to view it in the browser.

The page will reload if you make edits.\
You will also see any lint errors in the console.

### `yarn test`

Launches the test runner in the interactive watch mode.\
See the section about [running tests](https://facebook.github.io/create-react-app/docs/running-tests) for more information.

# Release process
There are a couple of components in Feast that are tied to the Web UI. These are all automatically handled during the release GitHub action:
1. the npm package
   - The release process for Feast automatically bumps the package version (see [bump_file_versions.py](../infra/scripts/release/bump_file_versions.py)) and releases the new NPM package (see [publish.yml](../.github/workflows/publish.yml) in the `publish-web-ui-npm` job)
2. the Feast Python SDK, which bundles in a compiled version of the Feast Web UI which is run on a `feast ui` CLI command.
   - The bundled Web UI in the Python SDK always compiles in the latest npm version

## (Advanced) Manually publishing the Feast Package to NPM

This generally should not be necessary, since new package versions are released with the overall Feast release workflow (see [publish.yml](../.github/workflows/publish.yml) in the `publish-web-ui-npm` job)

The Feast UI is published as a module to NPM and can be found here: https://www.npmjs.com/package/@feast-dev/feast-ui

### Requirements

To publish a new version of the module, you will need:
- to be part of the @feast-dev team in NPM. Ask `#feast-development` on http://slack.feast.dev to add you if necessary. 
- to [login to your NPM account on the command line](https://docs.npmjs.com/cli/v8/commands/npm-adduser).

### Steps for Publishing

1. Make sure tests are passing. Run tests with `yarn test` in the ui directory.
2. Bump the version number in `package.json` as appropriate.
3. Package the modules for distributions. Run the library build script with `yarn build:lib`. We use [Rollup](https://rollupjs.org/) for building the module, and the configs are in the `rollup.config.js` file.
4. Publish the package to NPM. Run `npm publish`
5. [Check NPM to see that the package was properly published](https://www.npmjs.com/package/@feast-dev/feast-ui).