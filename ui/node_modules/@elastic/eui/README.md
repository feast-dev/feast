<img src="https://repository-images.githubusercontent.com/107422373/b6180480-a1d7-11eb-8a3c-902086232aa7" />

# Elastic UI Framework

**The Elastic UI Framework is a collection of React UI components for quickly building user interfaces at Elastic.**

Check out our [full documentation site][docs] which contains many examples of components in the EUI framework aesthetic, and how to use them in your products. We also have a [FAQ][faq] that covers common usage questions. For other general questions regarding EUI, check out the [Discussions tab](https://github.com/elastic/eui/discussions).

The rest of this doc will detail how to run and contribute to the **EUI documentation** site locally.

## Running Locally

### Node

We depend upon the version of node defined in [.nvmrc](.nvmrc).

You will probably want to install a node version manager. [nvm](https://github.com/creationix/nvm) is recommended.

To install and use the correct node version with `nvm`:

```bash
nvm install
```

### Documentation

You can run the documentation locally at [http://localhost:8030/](http://localhost:8030/) by running the following.

```bash
yarn
yarn start
```

If another process is already listening on port 8030, the next free port will be used. Alternatively, you can specify a port:

```bash
yarn start --port 9000
```

## Goals

The primary goal of this library is to provide reusable UI components that can be used throughout
Elastic's web products. As React components, they remove CSS from the process of building UIs.
As a single source of truth, the framework allows our designers to make changes to our aesthetic
directly in the code. And unit test coverage for the UI components allows us to deliver a stable
"API for user interfaces".


## Wiki

### Consumption

* [Consuming EUI][consuming]
* [Using EUI with react-router](wiki/react-router.md)

### Maintenance / Contributing

[CONTRIBUTING.md](CONTRIBUTING.md)

* [Component design](wiki/component-design.md)
* [Component development](wiki/component-development.md)
  * [Creating components manually](wiki/creating-components-manually.md)
  * [Creating components with Yeoman](wiki/creating-components-yeoman.md)
* [Creating icons](wiki/creating-icons.md)
* [Testing](wiki/testing.md)
  * [Accessibility Testing](wiki/automated-accessibility-testing.md)
* [Documentation](wiki/documentation-guidelines.md)
* [Releasing versions](wiki/releasing-versions.md)

## License

[Dual-licensed under Elastic v2 and Server Side Public License, v 1][license] Read the [FAQ][faq] for details.

[license]: LICENSE.txt
[faq]: FAQ.md
[consuming]: wiki/consuming.md
[docs]: https://elastic.github.io/eui/
