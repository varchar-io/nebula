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

/**
 * This is time utility covering time conversions.
 * Absolute time, relative time, time formats etc.
 * Supports: 
 *  time in unix time "UTC seconds", 
 *  time in standard string format "yyyy-mm-dd hh::mm::ss" or short versions.
 *  time in semantic statements like "-3days", "-3weeks", "-5minutes", or "now"
 */
const seconds = (ds) => (+ds == ds) ?
    Math.round(new Date(+ds).getTime() / 1000) :
    Math.round(new Date(ds).getTime() / 1000);

// export API
// consumer can use it like this
// const time= require();
// time.seconds("2020-07-07")
module.exports = {
    seconds: seconds
};