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

// State js defines the object which represents current user state
// This state could be serialized and saved in local storage or from/to URL.
// Basically a nebula full URL is a hash object of this state.
// This state can be translated into a Nebula web service request to get response Nebula web reply.
export class State {
    constructor() {
        // initialize 
        this.state_ = {
            table: "",
            start: 0,
            end: 0,
            filter: {},
            keys: [],
            metrics: [],
            display: 0,
            window: 0,
            rollup: 0,
            sort: 0,
            limit: 0,
            code: "",
            customs: []
        };
    }
};