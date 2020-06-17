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

/*
 * SDK.js provides web client SDK for end user to program interactions with Nebula
 * It will support the basic code sample like below, its main purpose is to translate the code into a nebula request.
 *     
 * 
   //const values
   const name = "test";
   const schema = "ROW<a:int, b:string, c:bigint>";

   // define an customized column
   const colx = () => nebula.column("a") % 3;
   nebula.def("colx", nebula.Types.INT, expr);

   // get a data set from data set stored in HTTPS or S3
   const data = nebula.csv("https://varchar.io/test/data.csv", schema)
                    .table(name)
                    .select("colx", count(1).as("count"))
                    .where(col("_time_") >= 104598762 && col("_time_") <= 108598762)
                    .groupby(1)
                    .orderby(2, nebula.SORT.DESC);

   // render the data result with PIE chart with x-axis from "colx" and y-axis from "count"
   data.pie("colx", "count");
*/

// name this class as SDK instead?
export class Nebula {
    constructor() {

        // table specify a table name
        this.table = () => {
            return this;
        };
    }
};