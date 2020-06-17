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
 * This is a component to build up constraints.
 */
const DF = (v, d) => v ? v : d;
const ID = (id) => $(`#${id}`);

// can replace these 2 with icons
const IADD = "&plus;";
const IREMOVE = "&minus;";

// we're using single variable to reduce JSON size
// since we're using directly serialization
class Rule {
    constructor(col, op, values) {
        this.c = col;
        this.o = op;
        this.v = values;
    }
};

class Group {
    constructor(logic, rules, groups) {
        this.l = logic;
        this.r = rules;
        this.g = groups;
    }
};

export class Constraints {
    constructor(nested, containerId, columns, ops, group) {
        this.nested_ = nested;
        this.container_ = $(`#${containerId}`);
        // reset the element
        this.container_.html("");

        this.columns_ = columns;
        this.ops_ = ops;

        // sequence ID
        this.id_ = 0;

        // by default place a group in the container
        this.new_rule = (g, r) => {
            const id = `r${this.id_++}`;
            g.append(
                `<div id="${id}" class="crule">
                    <select class="cols"></select>
                    <select class="ops"></select>
                    <select class="values" multiple></select>
                </div>`);

            // selectize this rule values
            const rule = ID(id);
            const values = $($(rule).find('.values')[0]);
            // populate the options before selectizing it
            if (r) {
                r.v.map(e => values.append(`<option value="${e}">${e}</option>`));
            }
            const sdv = values.selectize({
                plugins: ['restore_on_backspace', 'remove_button'],
                persist: false,
                create: input => {
                    return {
                        value: input,
                        text: input
                    };
                }
            });

            // populate values for columns selection
            // columns is a list of column name as ARRAY
            const columns = $($(rule).find('.cols')[0]);
            this.columns_.map(e => columns.append(`<option value="${e}">${e}</option>`));

            // populate operators
            // ops is a key-value pair object
            const ops = $($(rule).find('.ops')[0]);
            for (var v in this.ops_) {
                ops.append(`<option value="${v}">${this.ops_[v]}</option>`);
            }

            // append function icons to remove, add-AND, add-OR
            ID(id).append(`<a class="del">${IREMOVE}</a>`);
            const delr = $($(rule).find('.del')[0]);
            delr.on("click", () => {
                ID(id).remove();
            });

            // r is a Rule, if present, populates its data
            if (r) {
                // set column
                columns.val(r.c);

                // set op
                ops.val(r.o);

                // set values
                sdv[0].selectize.setValue(r.v);
            }

            return rule;
        };

        // within a group, all rules are either combined by AND or OR, no mixed
        // this simplifies a bit and user can achieve any operations
        // p=parent, g=Group
        this.new_group = (p, g) => {
            const parent = DF(p, this.container_);
            const top = parent === this.container_;
            const indent = top ? "" : "indent";
            const id = `g${this.id_++}`;
            const hasData = !!g;
            parent.append(`<div id="${id}" class="cgroup ${indent}"></div>`);
            // every group will have a default rule
            const group = ID(id);

            // Every group can be removed except the top one
            if (!top) {
                group.prepend(`<a class="del">${IREMOVE}Group</a>`);
                const delr = $($(group).find('.del')[0]);
                delr.on("click", () => {
                    ID(id).remove();
                });
            }

            // GROUP append function
            group.prepend(`<a class="addr">${IADD}Rule</a>`);
            const addR = $($(group).find('.addr')[0]);
            addR.on("click", () => {
                this.new_rule(ID(id));
            });

            // add sub-group
            if (this.nested_) {
                addR.after(`<a class="addg">${IADD}Group</a>`);

                const addG = $($(group).find('.addg')[0]);
                addG.on("click", () => {
                    this.new_group(ID(id));
                });
            }

            // add group logic property at beginning
            const name = `ct_${id}`;
            group.prepend(`<input type="radio" name="${name}" value="AND" checked>and 
            <input type="radio" name="${name}" value="OR">or`);

            // if group has data
            if (hasData) {
                // mark the logic of this group
                $(`input[name='${name}'][value='${g.l}']`).prop('checked', true);

                // populate all rule and group
                $.each(g.r, (i, r) => {
                    // create rule under this group with given data r
                    this.new_rule(group, r);
                });

                // populate all sub groups of the value
                $.each(g.g, (i, gg) => {
                    this.new_group(group, gg);
                });
            } else {
                // create a new default rule
                this.new_rule(group);
            }

            return group;
        };

        ///////////////////////////////////////////////////////////////////////////////////////////
        // Deserialization
        ///////////////////////////////////////////////////////////////////////////////////////////
        // if a group object is provided, we will initialize the UI with this object
        if (group) {
            this.new_group(null, group);
        } else {
            // create a default group
            this.new_group();
        }

        ///////////////////////////////////////////////////////////////////////////////////////////
        // Serialization
        ///////////////////////////////////////////////////////////////////////////////////////////

        // output an object tree
        this.expr = () => {
            // visit all children
            const root = this.container_.children().first();
            return this.group(root);
        };

        // group element to group data object
        this.group = (eg) => {
            const gid = $(eg).attr("id");

            // logic of this group
            const ct = $(`input[name='ct_${gid}']:checked`).val();

            // JQuery supports ">" to find direct children
            // get all rules
            const rules = [];
            $(eg).find("> .crule").each((i, r) => {
                rules.push(this.rule(r));
            });

            // get all groups
            const groups = []
            $(eg).find("> .cgroup").each((i, g) => {
                groups.push(this.group(g));
            });

            return new Group(ct, rules, groups);
        };

        // rule element to rule object
        this.rule = (er) => {
            // get column
            const col = $(er).find(".cols option:selected").val();

            // get ops
            const ops = $(er).find(".ops option:selected").val();

            // values
            const values = [];
            $(er).find(".values option:selected").each((i, v) => {
                values.push($(v).val());
            });

            // wrap an object
            return new Rule(col, ops, values);
        };
    }
};