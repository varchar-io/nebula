/*
 * Copyright 2017-present Shawn Cao
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */


/** This entry is used to export all common library to its npm package */
import {
    neb
} from "/dist/web/main.js";

import {
    APIS
} from './_/api.js';
import {
    time
} from './_/time.js';
import {
    Handler
} from './_/handler.js';
import {
    bytes2utf8
} from './serde.js';

import {
    State
} from "./state.js";
import * as sdk from './sdk.js';
import {
    Charts
} from "./c/charts.js";

// nebula distribute library package
// add require in ES6 module type - remove for commonjs
import {
    createRequire
} from 'module';
const require = createRequire(
    import.meta.url);
const service = require('./dist/node/main.cjs');

export const n = {
    neb,
    APIS,
    time,
    Handler,
    bytes2utf8,
    service,
    State,
    sdk,
    Charts
};