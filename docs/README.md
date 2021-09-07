# Docs

This docs is built using [Docusaurus 2](https://docusaurus.io/) and hosted by GitHub Pages.

### Build

```
$ yarn build
```

This command generates static content into the `build` directory and can be served using any static contents hosting service.

### Test Locally

To test the build result, use `yarn serve`.
```
$ yarn serve
```


For local editing and developing, `yarn start` will automatically load changes.
```
$ yarn start
```

### Deployment

All static content (build result will be in special branch `gh-pages`).
Use below command to deploy if you have write permission.
(replace 'shawncao' with your own user name)

```
$ GIT_USER=shawncao yarn deploy
```
