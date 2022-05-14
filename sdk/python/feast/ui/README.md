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


**Note**: yarn start will not work on this because of the above dependency.
