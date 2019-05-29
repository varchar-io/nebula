import {
    NebulaClient
} from "/dist/web/main.js";

// define jquery style selector 
const d3 = NebulaClient.d3;
const ds = NebulaClient.d3.select;

export class Charts {
    constructor() {
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
                ds('#table_head').selectAll("th").data(keys).enter().append('th').attr("width", `${width}%`).text(d => d);

                // Get table body and print 
                ds('#table_content').selectAll('tr').data(json).enter().append('tr')
                    .selectAll('td')
                    .data((row) => {
                        return keys.map((column) => {
                            return {
                                column: column,
                                value: row[column]
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
            json.sort((a, b) => {
                return b[value] - a[value];
            });

            const top = 10;
            if (json.length > top) {
                json[top - 1][key] = 'others';
                for (let i = top; i < json.length; ++i) {
                    json[top - 1][value] += json[i][value];
                }

                json.length = top;
            }

            const w = area.node().scrollWidth;
            const h = 400;
            const r = Math.min(w, h) / 2;
            const color = (i) => d3.schemeCategory10[i % 10];

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
            const legend = vis.selectAll("g.legend").data(pie).enter().append("svg:g").attr("class", "legend")
                .attr("transform", (d, i) => `translate(${r+100},${-r + i * 20 + 20})`);

            // make a matching color rect
            legend.append("svg:rect")
                .attr("width", 15)
                .attr("height", 15)
                .attr("fill", (d, i) => color(i));

            legend.append("svg:text")
                .attr("text-anchor", "left")
                .attr("x", 18)
                .attr("y", 15)
                .text((d, i) => json[i][key]);
        };

        this.displayLine = (json, value, format) => {
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
            const tickWidth = 80;

            const width = area.node().scrollWidth - margin.left - margin.right;
            const height = 400 - margin.top - margin.bottom;
            let ticks = Math.ceil(width / tickWidth);
            let scale = Math.floor(json.length / ticks);
            if (scale < 1) {
                scale = 1;
                ticks = json.length - 1;
            }
            console.log(`Total ticks: ${ticks} -> ${scale}`);

            // set the ranges
            const x = d3.scaleLinear().range([0, width]).domain([0, json.length - 1]);
            const y = d3.scaleLinear().range([height, 0]).domain([0, d3.max(json, (d) => d[value])]);

            // define the line
            var line = d3.line()
                .x((d, i) => x(i))
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
                .call(
                    d3.axisBottom(x)
                    .ticks(ticks)
                    .tickFormat(format(scale)));

            // Add the Y Axis
            svg.append("g").call(d3.axisLeft(y));
        };

        this.displayTimeline = (json, key, value, start, end, window, ts, tf) => {
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
            const x = d3.scaleTime().range([0, width]).domain([start, end]);
            const y = d3.scaleLinear().range([height, 0]).domain([0, d3.max(json, (d) => d[value])]);

            // define the line
            var line = d3.line()
                .x((d, i) => x(new Date(+start + d[key] * window)))
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
                .call(
                    d3.axisBottom(x)
                    .ticks(ts)
                    .tickFormat(tf));

            // Add the Y Axis
            svg.append("g").call(d3.axisLeft(y));
        };
    }
};