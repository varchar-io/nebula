# Dev Notes On Nebula Web Tier
Nebula web tier is designed for supplying web faced layer to run user requests. 
Through this tier, Nebula provides supports for 
- Nebula UI via web requests (GET).
- Nebula REST API - defined by `api.js` extensions


## Some UI Dependencies In Use
- install grunt "npm install -g grunt-cli"
- use selectize for multi-values input
- go to "selectize/" and "npm install"
- build with plugins "grunt --plugins=remove_button,restore_on_backspace"
- we can also use it from CDNJS directly if no changes in the build
- https://cdnjs.cloudflare.com/ajax/libs/selectize.js/0.12.6/js/standalone/selectize.min.js
- https://cdnjs.cloudflare.com/ajax/libs/selectize.js/0.12.6/css/selectize.min.css


## Web tier is fully in ESM.
To embrace ESM is a good choice as modern browser and node.js (>v13) server are all supporting it.
This is good for 
- module reuse by both browser stack and web stack stack (node).
- good for future upgrade with optimizations.

Hence, we're declaring Nebula Web Tier is fully in ESM mode.
However, there are some legacy dependencies require us stay in `require` for some cases. So here are some detailed notes.
1.  We use webpack to build bundle for NebulaClient library, we keep this in `commonjs` still due to its own limitation.
2.  Because of #1, we have to use `createRequire` to ensure `require` is still available in ES module mode.
3.  We're enforcing using `named export` rather than `default export`, so please keep `export default xx` out of the code base.
4.  All Nebula ES modules will be organized in folder `_` for better identification, please follow this convension as well.
5.  If a module is used by both Web Server and Browser, we will `minify` it, we use VS Code extension `Minify` to do this work.

Nebula tier is designed to be a plugable platform so that any adopter can customize Nebula for its own appication. 
That is why I spent a lot of effort to enforce `Nebula Web` to be a consistent tech stack, above convensions are some basic requirements for nebula web advanced development.

## Use Webpack
Webpack is used to produce two bundles for both `browser` and `node.js`, follow these steps to refresh them whenver proto contracts updated in the service. 
1. $ ~/nebula/src/service/http/nebula > npm install (based on package.json - can be reused for all service)
2. ~/nebula/src/service/http/nebula > npx webpack
3. $ in dist/web/main.js, prepend "export" in it to export NebulaClient. (webpack doesn't generate `export` key)

