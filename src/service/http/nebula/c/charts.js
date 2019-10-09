import {
    NebulaClient
} from "/dist/web/main.js";

// define jquery style selector 
const d3 = NebulaClient.d3;
const ds = NebulaClient.d3.select;
const color = (i) => d3.schemeCategory10[i % 10];
const symbols = ["circle", "diamond", "square", "triangle-up", "triangle-down", "cross"];
const symbol = (i) => d3.symbol().size(81).type(symbols[i % symbols.length]);

export class Charts {
    constructor() {
        // display a legend in given places
        this.legend = (svg, px, py, names) => {
            const legend = svg.selectAll("g.legend")
                .data(names)
                .enter()
                .append("svg:g")
                .attr("class", "legend")
                .attr("transform", (d, i) => `translate(${px},${py + i * 20 + 20})`);

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
                .y((d) => y(d[value]));

            // draw each different line for each key in the data set
            // we only provide 4 styles for now, see styles.css
            let idx = 0;
            for (var k in kds) {
                // set the ranges
                const kd = kds[k];
                if (kd.length == 0) {
                    continue;
                }

                // Add the line path.
                svg.append("path")
                    .data([kd])
                    .attr("class", "line")
                    .attr("stroke", color(idx++))
                    .attr("d", line);
            }

            // put legend on the left top
            this.legend(svg, margin.left, 0, Object.keys(kds));

            // Add the X Axis
            svg.append("g")
                .attr("transform", `translate(0, ${height})`)
                .call(d3.axisBottom(x));

            // Add the Y Axis
            svg.append("g").call(d3.axisLeft(y));
        };
    }
};