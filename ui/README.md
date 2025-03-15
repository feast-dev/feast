# [Experimental] Feast Web UI

![Sample UI](https://github.com/feast-dev/feast/blob/master/ui/sample.png)

This project was bootstrapped with [Create React App](https://github.com/facebook/create-react-app).

## Usage

There are three modes of usage:
- via the `feast ui` CLI to view the current feature repository
- importing the UI as a module
- running the entire build as a React app.

### Importing the UI as a module

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
    },
  ]
}
```

* **Note** - `registryPath` only supports a file location or a url.

```
// Start the React App
yarn start
```

#### Customization

The advantage of importing Feast UI as a module is in the ease of customization. The `<FeastUI>` component exposes a `feastUIConfigs` prop thorough which you can customize the UI. Currently it supports a few parameters.

##### Fetching the Project List

By default, the Feast UI fetches the project list from the app root path. You can use `projectListPromise` to provide a promise that overrides where it's fetched from.

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

Examples of custom tabs can be found in the `/custom-tabs` folder.

## Development

### On React and Create React App

This project was bootstrapped with Create React App, and uses its scripts to simplify UI development. You can learn more in the [Create React App documentation](https://facebook.github.io/create-react-app/docs/getting-started).

To learn React, check out the [React documentation](https://reactjs.org/).

### Code Formatting

The code is formatted using [Prettier](https://prettier.io/). IDEs typically have Prettier addons or extensions that you can use for formatting, but you can also run:

- `yarn format` to format all files
- `yarn format:check` to check if files are formatted correctly without modifying them (used in GitHub Actions checks)
