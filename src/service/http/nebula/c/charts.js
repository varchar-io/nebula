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
    NebulaClient
} from "/dist/web/main.js";

// define jquery style selector 
const d3 = NebulaClient.d3;
const ds = NebulaClient.d3.select;
const color = (i) => d3.schemeCategory10[i % 10];
const symbols = ["circle", "diamond", "square", "triangle-up", "triangle-down", "cross"];
const symbol = (i) => d3.symbol().size(81).type(symbols[i % symbols.length]);
const isTime = (c) => c === "_time_";
const pad2 = (v) => `${v}`.padStart(2, 0);
const localTime = (unix_ms) => new Date(unix_ms).toLocaleString();
const isoTime = (unix_ms) => new Date(unix_ms).toISOString();

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

        // display a legend in given places
        this.legend = (svg, px, py, names, width = 0, vertical = true) => {
            const span = (width > 0 && names.length > 0) ? (width / (names.length)) : 60;
            const legend = svg.selectAll("g.legend")
                .data(names)
                .enter()
                .append("svg:g")
                .attr("class", "legend")
                .attr("transform",
                    (d, i) => vertical ?
                    `translate(${px}, ${py + i * 20 + 20})` :
                    `translate(${px + i*span}, ${py})`);

            // make a matching color rect
            legend.append("svg:rect")
                .attr("width", 15)
                .attr("height", 15)
                .attr("fill", (d, i) => color(i));

            legend.append("svg:text")
                .attr("text-anchor", "left")
                .attr("x", 18)
                .attr("y", 15)
                .text((d, i) => d);
        };

        this.displayTable = (json) => {
            // Get Table headers and print 
            if (json.length > 0) {
                const area = ds('#show');
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

        this.displayBar = (json, key, value) => {
            // clear the area first
            const area = ds('#show');
            area.html("");
            const margin = {
                top: 20,
                right: 20,
                bottom: 70,
                left: 40
            };
            const width = area.node().scrollWidth - margin.left - margin.right;
            const height = 600;

            const x = d3.scaleBand()
                .rangeRound([0, width])
                .paddingInner([0.2])
                .paddingOuter([0.4])
                .align([0.5]);
            const y = d3.scaleLinear().range([height, 0]);
            const xAxis = d3.axisBottom(x);
            const yAxis = d3.axisLeft(y).ticks(10);

            const svg = area.append("svg")
                .attr("width", width + margin.left + margin.right)
                .attr("height", height + margin.top + margin.bottom)
                .append("g")
                .attr("transform", `translate(${margin.left},${margin.top})`);

            // json is object list
            // set value range for both x and y axis
            x.domain(json.map(row => row[key]));
            y.domain([0, d3.max(json, row => row[value])]);

            svg.append("g")
                .attr("class", "x axis")
                .attr("transform", `translate(0, ${height})`)
                .call(xAxis)
                .selectAll("text")
                .style("text-anchor", "end")
                .attr("dx", "-.8em")
                .attr("dy", "-.55em")
                .attr("transform", "rotate(-45)");

            svg.append("g")
                .attr("class", "y axis")
                .call(yAxis)
                .append("text")
                .attr("transform", "rotate(-90)")
                .attr("y", 6)
                .attr("dy", ".71em")
                .style("text-anchor", "end")
                .text("Value ($)");

            svg.selectAll("bar")
                .data(json)
                .enter().append("rect")
                .style("fill", "steelblue")
                .attr("x", row => x(row[key]))
                .attr("y", row => y(row[value]))
                .attr("width", x.bandwidth())
                .attr("height", row => height - y(row[value]));
        };

        this.displayPie = (json, key, value) => {
            // clear the area first
            const area = ds('#show');
            area.html("");

            // we limit to display 10 values only, long tail will be aggregated as others
            json.sort((a, b) => b[value] - a[value]);

            const w = area.node().scrollWidth;
            const h = 400;
            const r = Math.min(w, h) / 2;

            const vis = area
                .append("svg:svg")
                .data([json])
                .attr("width", w)
                .attr("height", h)
                .append("svg:g")
                .attr("transform", `translate(${w/2},${h/2})`);

            const arc = d3.arc().outerRadius(r - 10).innerRadius(0);
            const pie = d3.pie().value(row => row[value]);

            const arcs = vis.selectAll("g.slice").data(pie).enter().append("svg:g").attr("class", "slice");
            arcs.append("svg:path")
                .attr("fill", (d, i) => color(i))
                .attr("d", arc);

            // display legends 
            // place each legend on the right and bump each one down 15 pixels
            // the posiiton of translate is relative to its parent, so the delta is computed to the center of the pie
            this.legend(vis, r + 100, -r, json.map(e => e[key]));
        };

        this.displayLine = (json, key, value) => {
            // clear the area first
            const area = ds('#show');
            area.html("");

            // set the dimensions and margins of the graph
            const margin = {
                top: 20,
                right: 60,
                bottom: 20,
                left: 60
            };

            const width = area.node().scrollWidth - margin.left - margin.right;
            const height = 400 - margin.top - margin.bottom;

            // set the ranges
            json.sort((a, b) => a[key] < b[key] ? -1 : 1);
            const x = d3.scaleBand().range([0, width]).domain(json.map(e => e[key]));
            const y = d3.scaleBand().range([0, height]).domain(json.map(e => +e[value]).sort((a, b) => b - a));

            // define the line
            var line = d3.line()
                .x((d) => x(d[key]))
                .y((d) => y(d[value]));

            // append the svg obgect to the body of the page
            // appends a 'group' element to 'svg'
            // moves the 'group' element to the top left margin
            var svg = area.append("svg")
                .attr("width", width + margin.left + margin.right)
                .attr("height", height + margin.top + margin.bottom)
                .append("g")
                .attr("transform", `translate(${margin.left}, ${margin.top})`);

            // Add the valueline path.
            svg.append("path")
                .data([json])
                .attr("class", "line")
                .attr("d", line);

            // Add the X Axis
            svg.append("g")
                .attr("transform", `translate(0, ${height})`)
                .call(d3.axisBottom(x));

            // Add the Y Axis
            svg.append("g").call(d3.axisLeft(y));
        };

        this.displayFlame = (data, group, key) => {
            if (data && data.length) {
                // set the dimensions and margins of the graph
                const margin = {
                    top: 20,
                    right: 60,
                    bottom: 20,
                    left: 60
                };

                // clear the area first
                const area = ds('#show');
                area.html("");
                const width = area.node().scrollWidth - margin.left - margin.right;

                for (var i = 0; i < data.length; ++i) {
                    const title = group ? data[i][group] : null;
                    const blob = JSON.parse(data[i][key]);

                    // define flame graph routine
                    const flame = NebulaClient.flamegraph()
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

        this.displayTimeline = (data, key, value, start) => {
            // clear the area first
            const area = ds('#show');
            area.html("");

            // set the dimensions and margins of the graph
            const margin = {
                top: 20,
                right: 60,
                bottom: 20,
                left: 60
            };

            const width = area.node().scrollWidth - margin.left - margin.right;
            const height = 400 - margin.top - margin.bottom;

            // append the svg obgect to the body of the page
            var svg = area.append("svg")
                .attr("width", width + margin.left + margin.right)
                .attr("height", height + margin.top + margin.bottom)
                .append("g")
                .attr("transform", `translate(${margin.left}, ${margin.top})`);

            // figure x/y min/max from all sub set
            const xmin = [];
            const xmax = [];
            const ymin = [];
            const ymax = [];
            const kds = {};
            for (var k in data) {
                const kd = data[k].sort((a, b) => a[key] - b[key]).map(e => {
                    let obj = {};
                    obj[key] = e[key] * 1000 + start;
                    obj[value] = +e[value];
                    return obj;
                });

                kds[k] = kd;
                xmin.push(d3.min(kd, d => d[key]));
                xmax.push(d3.max(kd, d => d[key]));
                ymin.push(d3.min(kd, d => d[value]));
                ymax.push(d3.max(kd, d => d[value]));
            }

            // define x and y values 
            const x = d3.scaleTime().range([0, width]).domain([d3.min(xmin), d3.max(xmax)]);
            const y = d3.scaleLinear().range([height, 0]).domain([d3.min(ymin), d3.max(ymax)]);

            // define the line
            var line = d3.line()
                .x((d) => x(new Date(d[key])))
                .y((d) => y(d[value]))
                .curve(d3.curveMonotoneX);

            // define a floating tooltip block
            var tooltip = d3.select("body").append("div")
                .attr("class", "tooltip")
                .style("opacity", 0);

            // draw each different line for each key in the data set
            let idx = 0;
            for (var k in kds) {
                // set the ranges
                const kd = kds[k];
                if (kd.length == 0) {
                    continue;
                }

                // Add the line path.
                svg.append("path")
                    .datum(kd)
                    .attr("class", "line")
                    .attr("stroke", color(idx++))
                    .attr("d", line);

                // for every data point, we draw circle on it
                // we can not draw too many points
                // the data needs to be range sampled, display maximum 1.5 * MAX_BASE
                const samples = [];
                const MAX_BASE = 10;
                const skip = Math.max(Math.round(kd.length / MAX_BASE), 1);
                for (var i = 0; i < kd.length; i += skip) {
                    samples.push(kd[i]);
                }
                svg.selectAll(`g-${idx}`)
                    .data(samples)
                    .enter().append("circle")
                    .attr("class", "dot")
                    .attr("cx", (d, i) => x(new Date(d[key])))
                    .attr("cy", (d) => y(d[value]))
                    .attr("r", 5)
                    .on("mouseover", (d) => {
                        tooltip.transition().duration(200).style("opacity", .8);
                        tooltip.html(`T: ${localTime(d[key])}<br/>V: ${d[value]}`)
                            .style("left", `${d3.event.pageX}px`)
                            .style("top", `${d3.event.pageY}px`);
                    })
                    .on("mouseout", (d) => {
                        tooltip.transition().duration(500).style("opacity", 0);
                    });
            }

            // put legend on the left top
            this.legend(svg, margin.left, 0, Object.keys(kds), width, false);

            // Add the X Axis
            svg.append("g")
                .attr("transform", `translate(0, ${height})`)
                .call(d3.axisBottom(x));

            // Add the Y Axis
            svg.append("g").call(d3.axisLeft(y));
        };
    }
};