# Example Feast UI App

This is an example React App that imports the Feast UI module and relies on a "/projects-list" endpoint to get projects.

See the module import in `src/index.js`. The main change this implements on top of a vanilla create-react-app is adding:

```tsx
import ReactDOM from "react-dom";
import FeastUI from "@feast-dev/feast-ui";
import "@feast-dev/feast-ui/dist/feast-ui.css";

ReactDOM.render(
  <React.StrictMode>
    <FeastUI
      feastUIConfigs={{
        projectListPromise: fetch("http://0.0.0.0:8888/projects-list", {
          headers: {
            "Content-Type": "application/json",
          },
        }).then((res) => {
          return res.json();
        })
      }}
    />
  </React.StrictMode>,
  document.getElementById("root")
);
```

It is used by the `feast ui` command to scaffold a local UI server. The feast python package bundles in resources produced from `npm run build --omit=dev


**Note**: yarn start will not work on this because of the above dependency.
