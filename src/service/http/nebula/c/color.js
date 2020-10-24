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

// define color schema across functions
export class Color {
    // reference this one https://nagix.github.io/chartjs-plugin-colorschemes/colorchart.html
    static system = "office.Infusion6";
    static schemes = {
        "brewer.BuPu5": ['#edf8fb', '#b3cde3', '#8c96c6', '#8856a7', '#810f7c'],
        "brewer.PRGn8": ['#762a83', '#9970ab', '#c2a5cf', '#e7d4e8', '#d9f0d3',
            '#a6dba0', '#5aae61', '#1b7837'
        ],
        "brewer.SetOne5": ['#e41a1c', '#377eb8', '#4daf4a', '#984ea3', '#ff7f00'],
        "office.Infusion6": ['#8c73d0', '#c2e8c4', '#c5a6e8', '#b45ec7', '#9fdafb', '#95c5b0'],
    };
    static get(i) {
        // i==0, avoid using random
        const index = ((i + 1) || Math.floor(Math.random() * 255)) - 1;
        const c = Color.schemes[Color.system];
        return c[index % c.length];
    };
};