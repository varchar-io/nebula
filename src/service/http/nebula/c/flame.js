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
    Color
} from "./color.min.js";

export class Flame {
    /**
     *
     * @param {id} id  - the HTML element canva ID.
     * @param {json} json - the recursive stack object in json format tree example:
     * {name: "", value:100, children: [{name: "a", value:50}, {name: "b", value:50, children: [...]}]}
     * @param {reverse} reverse flame (bottom-up) view or icicle view (top-down), default as icicle.
     */
    constructor(id, json, reverse) {
        // font size and style
        const fontSize = 16;
        const font = `1rem Segoe UI`;
        const paddingV = 10;
        const paddingH = 5;
        const frameH = 20;
        const ratio = devicePixelRatio || 1;
        this.reverse = reverse || false;
        this.canvas = document.getElementById(id);
        this.ctx = this.canvas.getContext('2d');
        this.maxLevel = 1;
        // prepare the size of the canvas and be responsive
        {
            const canvasWidth = this.canvas.offsetWidth;
            this.canvas.style.width = canvasWidth + 'px';
            this.canvas.width = canvasWidth * ratio;
            this.ctx.font = font;
            this.ratio = ratio;
        }

        // menu height and buttons
        const menuHeight = 30;
        // each button is defined by text, bound and click handler
        const buttons = [{
            name: "[Reverse Focus]",
            coord: {
                x: 0,
                y: 0,
                w: 100,
                h: menuHeight
            },
            handler: () => {
                if (!this.selected || this.selected.name === this.stack.name) {
                    alert('No non-root node selected.');
                    return;
                }

                // iterate into all branches and use selected name as root
                // make a copy from stack object
                const focus = {
                    name: this.selected.name,
                    value: 0,
                    children: []
                };
                const path = [{
                    o: this.stack,
                    i: 0
                }];

                // merge a valid path into a focus tree
                const merge = (p, f) => {
                    console.log(`get a path: ${p[p.length-1].o.name} - depth: ${p.length}`);
                    // going backwards adding this path into the new focus tree
                    let index = p.length - 1;
                    f.value += p[index].o.value;
                    let last = f;
                    while (index-- > 0) {
                        const node = p[index].o;
                        let existing = last.children.find(e => e.name === node.name);
                        if (!existing) {
                            existing = {
                                name: node.name,
                                value: node.value,
                                children: []
                            };
                            last.children.push(existing);
                        }

                        // assign current to last
                        last = existing;
                    }
                };

                // BFS the search path
                let pop = false;
                while (path.length > 0) {
                    // check if we need pop
                    if (pop) {
                        const item = path.splice(path.length - 1, 1)[0];
                        if (path.length > 0) {
                            const parent = path[path.length - 1].o;
                            const nextId = item.i + 1;
                            if (nextId < parent.children.length) {
                                path.push({
                                    o: parent.children[nextId],
                                    i: nextId
                                });
                                pop = false;
                            }
                        }
                        continue;
                    }

                    // check if current item match target.
                    const last = path[path.length - 1].o;
                    if (last.name === this.selected.name) {
                        merge(path, focus);
                        pop = true;
                        continue;
                    }

                    // check if current node can go further
                    if (!last.children || last.children.length === 0) {
                        pop = true;
                        continue;
                    }

                    path.push({
                        o: last.children[0],
                        i: 0
                    });
                    pop = false;
                }

                // render the new view for this new focus
                this.stack = focus;
                this.init();
                this.paint();
            }
        }, {
            name: "[Reset]",
            coord: {
                x: 120,
                y: 0,
                w: 80,
                h: menuHeight
            },
            handler: () => {
                this.reset();
            }
        }];

        this.updateH = (height) => {
            this.canvas.style.height = height + 'px';
            this.canvas.height = height * ratio;
            this.ctx.scale(ratio, ratio);
        };

        this.color = (name) => {
            // move to more bright color band in the pallete
            const offset = 5;

            // pretty much JAVA DRIVEN - needs to figure out universal way to attach color for different language
            let type = 0;
            if (name == 'all') {
                type = 4;
            } else if (this.selected && this.selected.name == name) {
                return '#9370d8';
            } else if (name.endsWith("_[j]") || name.endsWith("_[i]") || name.endsWith("_[k]")) {
                type = 1;
            } else if (name.startsWith('java.') || name.startsWith('scala.') || name.startsWith('sun.')) {
                type = 2;
            } else if (name.includes("::") || name.startsWith("-[") || name.startsWith("+[")) {
                type = 3;
            }

            return Color.get(type + offset);
        }

        this.yp = (c) => this.reverse ? (this.height - c.y - frameH) : c.y;

        this.paintMenu = () => {
            // draw a background
            this.ctx.fillStyle = '#091982';
            this.ctx.fillRect(paddingH, 0, this.canvas.offsetWidth - paddingH * 2, menuHeight);

            // paint all buttons one by one
            this.ctx.fillStyle = '#FFFFFF';
            this.ctx.font = font;
            buttons.forEach(e => {
                this.ctx.fillText(e.name, e.coord.x + paddingH * 2, 20, e.width);
            });
        };

        this.draw = (frame) => {
            const name = frame.name || "all";
            const width = frame.width;
            const c = frame.coord;
            // render frame only if its width is more than 1px
            if (c.x < this.canvas.offsetWidth && width > 1) {
                const y = this.yp(c);
                this.ctx.fillStyle = this.color(name);
                this.ctx.fillRect(c.x, y, width, frameH);

                // render text in the rect
                if (width >= 21) {
                    const chars = Math.floor(width / 7);
                    const title = name.length <= chars ? name : name.substring(0, chars - 2) + '..';
                    this.ctx.fillStyle = '#000000';
                    this.ctx.font = font;
                    this.ctx.fillText(title, c.x + 3, y + fontSize, width - 6);
                }
            }

            // requestAnimationFrame(this.draw);
        };

        // root is the target root to show the graph
        // it does: 
        // 1. recording every node's parent during the traverse.
        // 2. before hitting 'root', everything is marked as invisible
        // 3. when hitting 'root', circle back and mark the chain as visible
        // 4. starting from 'root', mark all it's descendents as visible
        this.touch = (frame, show) => {
            frame.show = show;
            const children = (frame.children || []);
            const level = frame.level + 1;
            if (show && level > this.maxLevel) {
                this.maxLevel = level;
            }
            children.forEach(c => {
                c.show = show;
                c.parent = frame;
                c.level = level;
            });
            return children;
        };

        // mark every node according its relationship to the selected focus
        this.mark = (frame, root) => {
            // locate any node (not including all)
            if (root) {
                // mark current and its children as invisible
                const queue = [...this.touch(frame, false)];

                // if found root, traverse back and mark the whole chain as visible
                while (queue.length > 0) {
                    if (queue.includes(root)) {
                        let p = root;
                        while (p) {
                            p.show = true;
                            p = p.parent;
                        }

                        break;
                    }

                    // pick next one
                    queue.push(...this.touch(queue.shift(), false));
                }
            }

            // start from root, mark everything under root as visible
            const queue = [];
            queue.push(...this.touch(root || frame, true));
            while (queue.length > 0) {
                queue.push(...this.touch(queue.shift(), true));
            }
        };

        // process the stack by tree walking the object
        // root is node where it starts to expand
        // it means will root the tree from this node
        this.visit = (frame) => {
            // flame.name -> stack flame
            // flame.value -> count value
            // children -> all children
            // render related props - updated before render
            // flame.show -> display this flame or not
            // flame.left -> left position to draw this flame
            // flame.width -> width to display this flame

            // if this flame is no-show, we can skip it
            if (!frame.show) {
                return;
            }

            // draw myself on the canvas
            this.draw(frame);

            // if my child's level has root, let's mark only that one as show, and the rest as non-show
            const childY = frame.coord.y + frameH + 2;
            const children = (frame.children || []).filter(e => e.show);
            // ?? isn't this is a bug?
            const total = children.reduce((a, b) => a + b.value, 0);
            let lastX = frame.coord.x;
            const endX = lastX + frame.width;
            children.forEach(c => {
                const pct = c.value / total;
                c.width = Math.floor(pct * frame.width);
                const overflow = lastX + c.width - endX;
                if (overflow > 0) {
                    c.width -= overflow;
                }
                c.coord = {
                    x: lastX,
                    y: childY
                };
                lastX += c.width + 1;
                this.visit(c);
            });
        };

        // repaint the whole canvas
        this.paint = (root) => {
            this.selected = root;
            this.maxLevel = 0;
            this.mark(this.stack, root);

            // update the height
            this.height = (this.maxLevel + 1) * (frameH + 2) + menuHeight;
            if (!root) {
                this.updateH(this.height);
            }

            // clear the canvas
            this.ctx.fillStyle = '#ffffff';
            this.ctx.fillRect(0, 0, this.canvas.offsetWidth, this.canvas.offsetHeight);

            // paint the menu at the top
            this.paintMenu();

            this.visit(this.stack, root);
        };

        // initialize the root rendering props
        this.init = () => {
            this.stack.width = this.canvas.offsetWidth - 2 * paddingH;
            this.stack.level = 0;
            this.stack.coord = {
                x: paddingH,
                y: paddingV + menuHeight
            };
        }

        this.reset = () => {
            this.stack = JSON.parse(json);
            this.init();
            this.paint();
        };

        // parse the stack object from json data
        this.reset();

        // handle navigation events
        // find frame which has coord
        this.find = (frame, coord) => {
            // find the band first
            const c = frame.coord;
            const y = this.yp(c);
            if (frame.show &&
                coord.y >= y &&
                coord.y <= (y + frameH) &&
                coord.x >= c.x &&
                coord.x <= c.x + frame.width) {
                return frame;
            }

            const children = (frame.children || []);
            for (let i = 0; i < children.length; ++i) {
                const found = this.find(children[i], coord);
                if (found) {
                    return found;
                }
            }

            return null;
        };

        this.canvas.onmousemove = (event) => {
            const c = this.canvas;
            const point = {
                x: event.pageX - c.offsetLeft,
                y: event.pageY - c.offsetTop
            };
            const frame = this.find(this.stack, point);

            // if found, cast a opaque overlay to it
            if (frame) {
                c.style.cursor = 'pointer';
                const pct = (frame.width * 100 / this.stack.width).toPrecision(3);
                c.title = `name: ${frame.name}, value: ${frame.value}, ratio: ${pct}%`;
                c.onclick = () => {
                    this.paint(frame);
                };
                return;
            }

            // check if it hits a button in menu
            if (point.y < menuHeight) {
                const btn = buttons.find(b => point.x >= b.coord.x && point.x < (b.coord.x + b.coord.w));
                if (btn) {
                    c.style.cursor = 'pointer';
                    c.title = `click action: ${btn.name}`;
                    c.onclick = () => {
                        btn.handler();
                    };
                    return;
                }
            }

            // if not hit anything
            this.canvas.onmouseout();
        };

        this.canvas.onmouseout = () => {
            const c = this.canvas;
            c.title = '';
            c.style.cursor = '';
            c.onclick = '';
        };
    }
};