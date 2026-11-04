# Example Feast UI App

This is an example React App that imports the Feast UI module. 

See the module import in `src/index.js`. The main change this implements on top of a vanilla create-react-app is adding:

```tsx
import ReactDOM from "react-dom";
import FeastUI from "@feast-dev/feast-ui";
import "@feast-dev/feast-ui/dist/feast-ui.css";

ReactDOM.render(
  <React.StrictMode>
    <FeastUI />
  </React.StrictMode>,
  document.getElementById("root")
);
```

It is used by the `feast ui` command to scaffold a local UI server. The feast python package bundles in resources produced from `npm run build --omit=dev.` 

The `feast ui` command will generate the necessary `projects-list.json` file and initialize it for the UI to read.


**Note**: `yarn start` will not work on this because of the above dependency.

## Dev
To test with a locally built Feast UI package, do:

```bash
   make build-ui-local
   feast ui
   ```

OR

You can also do: 
1. `yarn link` in ui/ 
2. `yarn install` in ui/
3. `yarn link` in ui/node_modules/react
4. `yarn link` in ui/node_modules/react-dom
5. and then come here to do:
   ```bash
   yarn link "@feast-dev/feast"
   yarn link react
   yarn link react-dom
   yarn start
   ```

See also https://github.com/facebook/react/issues/14257.