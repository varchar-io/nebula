export class Flame {
    constructor(id, stack, reverse) {
        const fontSize = 16;
        const font = `1rem Segoe UI`;
        const paddingV = 10;
        const paddingH = 5;
        const frameH = 20;
        this.reverse = reverse || false;
        this.canvas = document.getElementById(id);
        this.ctx = this.canvas.getContext('2d');
        this.maxLevel = 1;
        // prepare the size of the canvas and be responsive
        {
            const ratio = devicePixelRatio || 1;
            const canvasWidth = this.canvas.offsetWidth;
            const canvasHeight = this.canvas.offsetHeight;
            this.canvas.style.width = canvasWidth + 'px';
            this.canvas.width = canvasWidth * ratio;
            this.canvas.height = canvasHeight * ratio;
            this.ctx.scale(ratio, ratio);
            this.ctx.font = font;
            this.ratio = ratio;
        }

        this.palette = [
            [0x9370db, 20, 30, 30],
            [0x9370db, 20, 30, 10],
            [0x9370db, 20, 10, 30],
            [0x9370db, 20, 20, 10],
            [0x9370db, 20, 10, 10]
        ];

        this.getColor = (p) => {
            const v = Math.random();
            return '#' + (p[0] + ((p[1] * v) << 16 | (p[2] * v) << 8 | (p[3] * v))).toString(16);
        };

        this.color = (name) => {
            // pretty much JAVA DRIVEN - needs to figure out universal way to attach color for different language
            let type = 4;
            if (name.endsWith("_[j]")) {
                type = 0;
            } else if (name.endsWith("_[i]")) {
                type = 1;
            } else if (name.endsWith("_[k]")) {
                type = 2;
            } else if (name.includes("::") || name.startsWith("-[") || name.startsWith("+[")) {
                type = 3;
            }
            return this.getColor(this.palette[type]);
        }

        this.draw = (frame) => {
            const name = frame.name || "all";
            const width = frame.width;
            const c = frame.coord;
            // render frame only if its width is more than 1px
            if (c.x < this.canvas.offsetWidth && width > 1) {
                const y = this.reverse ? (this.height - c.y) : c.y;
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
            // clear
            this.ctx.fillStyle = '#ffffff';
            this.ctx.fillRect(0, 0, this.canvas.offsetWidth, this.canvas.offsetHeight);
            this.maxLevel = 0;
            this.mark(stack, root);
            // update the height
            this.height = this.maxLevel * (frameH + 2);

            this.visit(stack, root);
        };

        // initialize the root rendering props
        stack.width = this.canvas.offsetWidth - 2 * paddingH;
        stack.level = 0;
        stack.coord = {
            x: paddingH,
            y: paddingV
        };
        this.paint();

        // handle navigation events
        // find frame which has coord
        this.find = (frame, coord) => {
            // find the band first
            const c = frame.coord;
            const y = this.reverse ? (this.height - c.y) : c.y;
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
            const frame = this.find(stack, {
                x: event.clientX - c.offsetLeft,
                y: event.clientY - c.offsetTop
            });

            // if found, cast a opaque overlay to it
            if (frame) {
                c.style.cursor = 'pointer';
                const ratio = (frame.width * 100 / stack.width).toPrecision(3);
                c.title = `name: ${frame.name}, value: ${frame.value}, ratio: ${ratio}%`;
                c.onclick = () => {
                    this.paint(frame);
                };
                return;
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