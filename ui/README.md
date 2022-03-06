# [WIP] Feast Web UI

![Sample UI](sample.png)

This project was bootstrapped with [Create React App](https://github.com/facebook/create-react-app).

## Project structure
The Web UI is powered by a JSON registry dump from Feast (running `feast registry-dump`). Running `yarn start` launches a UI
powered by test data. 
- `public/` contains assets as well as demo data loaded by the Web UI.
  - There is a `projects-list.json` which represents all Feast projects the UI shows. 
  - There is also a `registry.json` which is the registry dump for the feature repo.
- `feature_repo/` contains a sample Feast repo which generates the `registry.json`
- `src/` contains the Web UI source code. This parses the registry json blob in  `src/parsers` to make this data 
available for the rest of the UI.
- `src/custom-tabs` includes sample custom tabs. This is a WIP plugin system where users can inject their own tabs and 
data to the UI.


## Available Scripts

In the project directory, you can run:

### `yarn start`

Runs the app in the development mode.\
Open [http://localhost:3000](http://localhost:3000) to view it in the browser.

The page will reload if you make edits.\
You will also see any lint errors in the console.

### `yarn test`

Launches the test runner in the interactive watch mode.\
See the section about [running tests](https://facebook.github.io/create-react-app/docs/running-tests) for more information.

### `yarn build`

Builds the app for production to the `build` folder.\
It correctly bundles React in production mode and optimizes the build for the best performance.

The build is minified and the filenames include the hashes.\
Your app is ready to be deployed!

See the section about [deployment](https://facebook.github.io/create-react-app/docs/deployment) for more information.

### `yarn eject`

**Note: this is a one-way operation. Once you `eject`, you can’t go back!**

If you aren’t satisfied with the build tool and configuration choices, you can `eject` at any time. This command will remove the single build dependency from your project.

Instead, it will copy all the configuration files and the transitive dependencies (webpack, Babel, ESLint, etc) right into your project so you have full control over them. All of the commands except `eject` will still work, but they will point to the copied scripts so you can tweak them. At this point you’re on your own.

You don’t have to ever use `eject`. The curated feature set is suitable for small and middle deployments, and you shouldn’t feel obligated to use this feature. However we understand that this tool wouldn’t be useful if you couldn’t customize it when you are ready for it.

## Learn More

You can learn more in the [Create React App documentation](https://facebook.github.io/create-react-app/docs/getting-started).

To learn React, check out the [React documentation](https://reactjs.org/).
