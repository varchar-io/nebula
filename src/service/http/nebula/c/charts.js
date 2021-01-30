/*
 * Copyright 2017-present varchar.io
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

import {
    neb
} from "../dist/web/main.js";

const d3 = neb.d3;
const ds = neb.d3.select;

// neb.bb is a module
const nb = neb.bb;
const bb = nb.bb;
const bar = nb.bar;
const pie = nb.pie;
const spline = nb.spline;
const area = nb.area;
const isTime = (c) => c === "_time_";
const pad2 = (v) => `${v}`.padStart(2, 0);
const EMPTY = "(null)";

export class Charts {
    constructor() {
        // using UTC time as backend using unix time stamp
        this.formatTime = (unix_ms) => {
            const date = new Date(unix_ms);
            const y = date.getUTCFullYear();
            // month is 0-based index
            const m = pad2(date.getUTCMonth() + 1);
            const d = pad2(date.getUTCDate());
            const h = pad2(date.getUTCHours())
            const mi = pad2(date.getUTCMinutes());
            const s = pad2(date.getUTCSeconds());
            return `${y}-${m}-${d} ${h}:${mi}:${s}`;
        };

        this.displayTable = (chartId, json) => {
            // Get Table headers and print 
            if (json.length > 0) {
                const area = ds(chartId);
                area.html("");
                const tb = area.append("table");
                tb.append("thead").append("tr").attr("id", "table_head");
                tb.append("tbody").attr("id", "table_content");

                // append header
                const keys = Object.keys(json[0]);
                const width = Math.round(100 / keys.length);
                ds('#table_head')
                    .selectAll("th")
                    .data(keys)
                    .enter()
                    .append('th')
                    .attr("width", `${width}%`)
                    .text(d => isTime(d) ? "[time]" : d);

                // Get table body and print 
                ds('#table_content').selectAll('tr').data(json).enter().append('tr')
                    .selectAll('td')
                    .data((row) => {
                        return keys.map((column) => {
                            return {
                                column: column,
                                value: isTime(column) ? this.formatTime(row[column] * 1000) : row[column]
                            };
                        });
                    })
                    .enter()
                    .append('td')
                    .text(d => d.value);
            }
        };

        // transform json like `{k1:1, k2:2, v1:3, v2:4}` into column values
        // like `[["k1", 1], ["k2", 2], ["v1", 3], ["v2", 4]]`
        this.columns = (json, keys, metrics, headless) => {
            const cols = [...keys, ...metrics];
            const numKeys = keys.length;
            const data = [];
            // push column name as headers (follow CSV convention)
            cols.forEach(c => data.push(headless ? [] : [c]));
            json.forEach(row => {
                // process keys - we do not like empty keys using default empty
                for (let i = 0; i < numKeys; ++i) {
                    data[i].push(`${row[cols[i]]}` || EMPTY);
                }

                // metrics
                for (let i = numKeys; i < cols.length; ++i) {
                    data[i].push(row[cols[i]]);
                }
            });

            return data;
        };

        this.key2x = (keys, option) => {
            // if only one key, use it as x-axis category
            if (keys.length == 1) {
                option.data.x = keys[0];
                option.axis = {
                    x: {
                        type: "category"
                    }
                };

                // groups all metrics? 
                // option.groups = [metrics];
            }
        };

        this.displayBar = (chartId, json, keys, metrics) => {
            // clear the area first
            ds(chartId).html("");
            const option = {
                data: {
                    columns: this.columns(json, keys, metrics),
                    type: bar()
                },
                bar: {
                    width: {
                        ratio: 0.6
                    }
                },
                bindto: chartId
            };

            // move key to x axis
            this.key2x(keys, option);

            // chart can call load/unload to refresh with new data
            const chart = bb.generate(option);
        };

        this.displayPie = (chartId, json, keys, metrics) => {
            // clear the area first
            ds(chartId).html("");
            const data = this.columns(json, keys, metrics, true);
            if (keys.length == 0) {
                data.unshift([metrics[0]]);
            }
            const option = {
                data: {
                    rows: data,
                    type: pie(),
                },
                bindto: chartId
            };

            const chart = bb.generate(option);
        };

        this.displayLine = (chartId, json, keys, metrics) => {
            // clear the area first
            ds(chartId).html("");
            const option = {
                data: {
                    columns: this.columns(json, keys, metrics),
                    type: spline(),
                },
                bindto: chartId
            };

            // move key to x axis
            this.key2x(keys, option);
            const chart = bb.generate(option);
        };

        this.displayFlame = (chartId, json, keys, metrics) => {
            if (json && json.length) {
                // set the dimensions and margins of the graph
                const margin = {
                    top: 20,
                    right: 60,
                    bottom: 20,
                    left: 60
                };

                // clear the area first
                const area = ds(chartId);
                area.html("");
                const width = area.node().scrollWidth - margin.left - margin.right;

                // TODO(cao) - support multiple keys?
                const key = keys.length == 0 ? null : keys[0];
                const value = metrics[0];


                for (var i = 0; i < json.length; ++i) {
                    const title = key ? json[i][key] : null;
                    const data = `${json[i][value]}`;
                    if (data.length == 0) {
                        continue;
                    }

                    // parse the valid flame data
                    const blob = JSON.parse(data);

                    // define flame graph routine
                    const flame = neb.flamegraph()
                        .width(width)
                        .sort(true)
                        .minFrameSize(1);

                    // if we have title
                    if (title) {
                        flame.title(title);
                    }

                    area.append("div")
                        .datum(blob)
                        .call(flame);
                }
            }
        };

        this.displayTimeline = (chartId, json, keys, metrics, timeCol, start) => {
            // clear the area first
            ds(chartId).html("");

            // assuming the data already sorted by timeCol (no need to sort again)
            // cols is keyed by column name
            const key = keys.length == 0 ? null : keys[0];
            const value = metrics[0];
            const times = [];
            const cols = {
                [timeCol]: times
            };
            json.forEach(row => {
                const variant = (key in row) ? (row[key] || EMPTY) : value;
                cols[variant] = cols[variant] || [];
                cols[variant].push(row[value]);
                // sync the vector the same size as time
                const time = row[timeCol] * 1000 + start;
                // check if this is new time, if it is new, 
                // we have to sync every vector to have the same length as times
                if (times.length == 0 || time != times[times.length - 1]) {
                    for (const v in cols) {
                        const column = cols[v];
                        while (column.length < times.length) {
                            // push empty value, use 0?
                            column.push(null);
                        }
                    }

                    // now, we can process the new time now
                    times.push(time);
                }
            });

            const option = {
                data: {
                    x: timeCol,
                    json: cols,
                    type: area()
                },
                axis: {
                    x: {
                        tick: {
                            fit: false,
                            count: 10
                        },
                        type: "timeseries",
                    }
                },
                tooltip: {
                    format: {
                        title: (x) => d3.timeFormat("%Y-%m-%d %H:%M:%S")(x)
                    }
                },
                zoom: {
                    enabled: true,
                    type: "drag"
                },
                point: {
                    focus: {
                        only: true
                    }
                },
                bindto: chartId
            };

            const chart = bb.generate(option);
        };
    }
};