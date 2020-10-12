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

import {
    neb
} from "../dist/web/main2.js";
import {
    Flame
} from "./flame.min.js";

const Chart = neb.cj.Chart;
const pad2 = (v) => `${v}`.padStart(2, 0);
const isTime = (c) => c === "_time_";

export class Charts {
    constructor() {
        this.id = 'viz';
        this.chart = null;

        // clear the show area
        this.cls = (chartId) => $(chartId).html(`<canvas id='${this.id}' height='100pt'/>`);

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
                const area = $(chartId);
                area.html("");
                const tb = $("<table/>").appendTo(area);
                const head = $("<tr/>").appendTo($("<thead/>").appendTo(tb));
                const content = $("<tbody/>").appendTo(tb);

                // append header
                const keys = Object.keys(json[0]);
                const width = Math.round(100 / keys.length);
                keys.forEach(k =>
                    $("<th/>").appendTo(head)
                    .attr("width", `${width}%`)
                    .text((isTime(k) ? "[time]" : k)));

                // Get table body and print 
                json.forEach(row => {
                    const r = $("<tr/>").appendTo(content);
                    keys.forEach(k => {
                        const v = row[k];
                        $("<td/>").appendTo(r).text(isTime(k) ? this.formatTime(v * 1000) : v);
                    });
                });
            }
        };

        this.color = (index) => {
            const palette = [
                "rgba(224, 170, 255, 0.9)",
                "rgba(199, 125, 255, 0.85)",
                "rgba(157, 78, 221, 0.8)",
                "rgba(123, 44, 191, 0.75)",
                "rgba(90, 24, 154, 0.7)",
                "rgba(60, 9, 108, 0.65)",
                "rgba(36, 0, 70, 0.6)",
                "rgba(16, 0, 43, 0.55)"
            ];
            return palette[(index + index % 3) % palette.length];
        };

        this.colorize = (op, ctx) => {
            // var v = ctx.dataset.data[ctx.dataIndex];
            return this.color(ctx.dataIndex);
        };

        // process raw JSON data organized by rows into desired format by the viz type
        // json = all row object
        // keys = all key columns
        // metrics = all metric columns
        // x = key name for single x-axis, other keys will become data series
        this.hyphen = ' - ';
        this.label = (row, keys) => {
            const kv = [];
            keys.forEach(k => {
                kv.push(`${row[k]}`);
            });

            return kv.join(this.hyphen);
        };

        this.process = (json, keys, metrics, x, xf) => {
            // by default, labels are generated from all key combinations
            let genLabel = (row) => this.label(row, keys);
            // by default - only one data series generated
            let genSeries = (row) => {
                return "";
            };

            // title
            let desc = keys.length > 0 ? `metrics by ${keys.join(this.hyphen)}` : null;

            // if single x-axis key is specified (pivoted by this column)
            if (x) {
                desc = `metrics by ${x}`;
                // update gen series comes from remaining keys
                // and genLabel comes from the single x-axis
                genSeries = genLabel;
                genLabel = (row) => {
                    return xf(row[x]);
                };
            }

            // align array to size by filling 0
            const align = (vector, size, filling) => {
                const val = filling || 0;
                while (vector.length < size) {
                    vector.push(val);
                }
            };

            // maybe multiple data sets splitted by each key
            // series will be data points keyed by all labels
            // all key values will be labels
            const buckets = [];
            const set = {};
            const genSet = (row) => {
                // x value
                const x = genLabel(row);
                let index = buckets.indexOf(x);
                if (index == -1) {
                    index = buckets.length;
                    buckets.push(x);
                }

                // support single metric for now
                const name = genSeries(row);

                // if name is valid, we use it as series name
                // otherwise we support all metrics as series name
                const series = name ? [name] : metrics;
                for (var i = 0; i < series.length; ++i) {
                    const s = series[i];
                    if (!(s in set)) {
                        set[s] = [];
                    }

                    // data points for this series 
                    const data = set[s];
                    // fill 0 till current index
                    align(data, index + 1);
                    data[index] = +row[metrics[i]];
                }
            };

            // process the whole JSON rows
            json.forEach(row => genSet(row));

            // align all data points in set to align as size
            for (const key in set) {
                const vector = set[key];
                align(vector, buckets.length);
            }

            return {
                title: desc,
                labels: buckets,
                series: set
            };
        };

        this.options = (model) => {
            const opts = {
                responsive: true,
                title: {
                    display: model.title,
                    text: model.title
                },
                legend: {
                    display: true,
                    labels: {
                        fontColor: 'rgba(147, 112, 219, 0.75)'
                    }
                },

                tooltips: {
                    callbacks: {
                        label: (item, data) => {
                            const ds = data.datasets[item.datasetIndex];
                            var label = ds.label || '';

                            if (label) {
                                label += ': ';
                            }

                            const val = `${ds.data[item.index]}`;
                            label += val;
                            if (model.pct) {
                                var total = ds.data.reduce((prev, cur, curIdx, arr) => {
                                    return prev + cur;
                                });
                                label += `, ${Math.floor(((val/total) * 100)+0.5)}%`;
                            }

                            return label;
                        }
                    }
                },

                scales: {
                    yAxes: [{
                        ticks: {
                            beginAtZero: true
                        }
                    }],
                }
            };

            // pie chart remove y-axis
            if (model.type === 'pie' || model.type === 'doughnut') {
                opts.scales.yAxes = [];
            }

            // if x-axis is time scale
            if (model.xtime) {
                opts.scales.xAxes = [{
                    type: 'time',
                    ticks: {
                        autoSkip: true,
                        maxTicksLimit: 20,
                        maxRotation: 0,
                        minRotation: 0
                    },
                    time: {
                        displayFormats: {
                            'millisecond': 'MMM DD',
                            'second': 'MMM DD',
                            'minute': 'MMM DD',
                            'hour': 'MMM DD',
                            'day': 'MMM DD',
                            'week': 'MMM DD',
                            'month': 'MMM DD',
                            'quarter': 'MMM DD',
                            'year': 'MMM DD',
                        }
                    }
                }];
            }

            return opts;
        };

        this.displayGeneric = (chartId, model) => {
            // process the data model
            const dataset = {
                labels: model.labels,
                datasets: []
            };

            // push all data sets
            let idx = 0;
            for (const name in model.series) {
                dataset.datasets.push({
                    label: name,
                    backgroundColor: this.color(idx++),
                    data: model.series[name]
                });
            }

            // set labels
            if (this.chart) {
                this.chart.destroy();
            }

            this.chart = new Chart(chartId, {
                type: model.type,
                data: dataset,
                options: this.options(model)
            });
        };

        // if data specified as pair [], it will be used as min/max display for each label
        // 'bar'=>column, 'horizontalBar' for normal bar
        this.displayBar = (chartId, json, keys, metrics) => {
            this.cls(chartId);
            const model = this.process(json, keys, metrics);
            model.type = 'bar';
            this.displayGeneric(this.id, model);
        };

        this.displayLine = (chartId, json, keys, metrics) => {
            this.cls(chartId);
            const model = this.process(json, keys, metrics);
            model.type = 'line';
            this.displayGeneric(this.id, model);
        };

        this.displayPie = (chartId, json, keys, metrics) => {
            this.cls(chartId);
            // clear the area first
            const model = this.process(json, keys, metrics);

            // pie chart display tooltip with percentage
            model.pct = true;
            model.type = 'pie';
            this.displayGeneric(this.id, model);
        };

        this.displayTimeline = (chartId, json, keys, metrics, timeCol, start) => {
            // clear the area first
            this.cls(chartId);

            // const time = row[timeCol] * 1000 + start;
            const model = this.process(json, keys, metrics, timeCol, (x) => this.formatTime(x * 1000 + start));
            model.xtime = true;
            model.type = 'line';
            this.displayGeneric(this.id, model);
        };

        this.displayFlame = (chartId, json, keys, metrics) => {
            if (metrics.length !== 1 || !metrics[0].endsWith('.TREEMERGE')) {
                return 'Flame view supports only single metric resulting by treemerge function';
            }

            // clear the display 
            const area = $(chartId);
            area.html("");

            let i = 0;
            json.forEach(row => {
                const title = this.label(row, keys);
                const stack = row[metrics[0]];

                // create a new flame
                const id = `fg_${i++}`;
                area.append(`<center>${title}</center>`);
                area.append(`<canvas id='${id}' width='${area.width()}'/>`);
                new Flame(id, JSON.parse(stack));
            });
        };
    }
};