# \[Beta] Web UI

**Warning**: This is an _experimental_ feature. To our knowledge, this is stable, but there are still rough edges in the experience. Contributions are welcome!

## Overview

The Feast Web UI allows users to explore their feature repository through a Web UI. It includes functionality such as:

* Browsing Feast objects (feature views, entities, data sources, feature services, and saved datasets) and their relationships
* Searching and filtering for Feast objects by tags

![Sample UI](../../ui/sample.png)

## Usage

There are several ways to use the Feast Web UI.

### Feast CLI

The easiest way to get started is to run the `feast ui` command within a feature repository:

Output of `feast ui --help`:

```bash
Usage: feast ui [OPTIONS]

Shows the Feast UI over the current directory

Options:
-h, --host TEXT                 Specify a host for the server [default: 0.0.0.0]
-p, --port INTEGER              Specify a port for the server [default: 8888]
-r, --registry_ttl_sec INTEGER  Number of seconds after which the registry is refreshed. Default is 5 seconds.
--help                          Show this message and exit.
```

This will spin up a Web UI on localhost which automatically refreshes its view of the registry every `registry_ttl_sec`

### Importing as a module to integrate with an existing React App

This is the recommended way to use Feast UI for teams maintaining their own internal UI for their deployment of Feast.

Start with bootstrapping a React app with `create-react-app`

```
npx create-react-app your-feast-ui
```

Then, in your app folder, install Feast UI and optionally its peer dependencies. Assuming you use yarn

```
yarn add @feast-dev/feast-ui
# For custom UI using the Elastic UI Framework (optional):
yarn add @elastic/eui
# For general custom styling (optional):
yarn add @emotion/react
```

Edit `index.js` in the React app to use Feast UI.

```js
import React from "react";
import ReactDOM from "react-dom";
import "./index.css";

import FeastUI from "@feast-dev/feast-ui";
import "@feast-dev/feast-ui/dist/feast-ui.css";

ReactDOM.render(
  <React.StrictMode>
    <FeastUI />
  </React.StrictMode>,
  document.getElementById("root")
);
```

When you start the React app, it will look for `projects-list.json` to find a list of your projects. The JSON should look something like this.

```json
{
  "projects": [
    {
      "name": "Credit Score Project",
      "description": "Project for credit scoring team and associated models.",
      "id": "credit_score_project",
      "registryPath": "/registry.json"
    }
  ]
}
```

* **Note** - `registryPath` only supports a file location or a url.

Then start the React App

```bash
yarn start
```

#### Customization

The advantage of importing Feast UI as a module is in the ease of customization. The `<FeastUI>` component exposes a `feastUIConfigs` prop thorough which you can customize the UI. Currently it supports a few parameters.

##### Fetching the Project List

You can use `projectListPromise` to provide a promise that overrides where the Feast UI fetches the project list from.

```jsx
<FeastUI
  feastUIConfigs={{
    projectListPromise: fetch(SOME_PATH, {
      headers: {
        "Content-Type": "application/json",
      },
    }).then((res) => {
      return res.json();
    })
  }}
/>
```

##### Custom Tabs

You can add custom tabs for any of the core Feast objects through the `tabsRegistry`.

```jsx
const tabsRegistry = {
  RegularFeatureViewCustomTabs: [
    {
      label: "Custom Tab Demo", // Navigation Label for the tab
      path: "demo-tab", // Subpath for the tab
      Component: RFVDemoCustomTab, // a React Component
    },
  ]
}

<FeastUI
  feastUIConfigs={{
    tabsRegistry: tabsRegistry,
  }}
/>
```

Examples of custom tabs can be found in the `ui/custom-tabs` folder.
