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

/** right now - seconds is the supported time granularity
 * mapping all units to seconds value
 */
const S_MIN = 60;
const S_HR = 60 * S_MIN;
const S_DAY = 24 * S_HR;
const S_WEEK = 7 * S_DAY;
const units = {
    // minute units
    minute: S_MIN,
    minutes: S_MIN,
    min: S_MIN,
    mins: S_MIN,
    m: S_MIN,

    // hour units
    hour: S_HR,
    hours: S_HR,
    hr: S_HR,
    hrs: S_HR,
    h: S_HR,

    // day units
    day: S_DAY,
    days: S_DAY,
    d: S_DAY,

    // week units
    week: S_WEEK,
    weeks: S_WEEK,
    wk: S_WEEK,
    wks: S_WEEK,
    w: S_WEEK
};

// the semantic string is either a 'now' or a minus value of a unit.
const pattern = /(?<now>now)|-(?<count>\d+)(?<unit>\w+)/i;
const k_now = 'now';
const k_count = 'count';
const k_unit = 'unit';

const seconds = (ds) => {
    if (ds === null || ds === undefined) {
        return 0;
    }

    const ms = (x) => Math.round(new Date(x).getTime() / 1000);
    const digsOnly = (+ds == ds);

    // digit only expression is just a utc unix time in seconds
    if (digsOnly) {
        return ms(+ds);
    }

    // relative time value needs to convert, try to detect it
    const str = `${ds}`.toLowerCase();
    const m = str.match(pattern);
    if (m) {
        // check if it is now
        if (m.groups[k_now]) {
            return ms(Date.now());
        }

        // check unit
        const u = m.groups[k_unit];
        if (u in units) {
            const delta = m.groups[k_count] * units[u];
            return ms(Date.now() - delta * 1000);
        }
    }

    // absolute time value in a time string like '2020-07-07 10:05:03'
    return ms(ds);
};

// same copy as time.js - 
// need a hack to make it work for both require and es6 import
export const time = {
    seconds: seconds
};