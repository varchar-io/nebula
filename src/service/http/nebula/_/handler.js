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

/**
 * Query API
 * start: 2019-04-01
 * end: 2019-06-20
 * fc: user_id
 * op: 0=, 1!=, 2>,3<, 4like
 * fv: 23455554444
 * cols: ["user_id", "pin_id"]
 * limit: 1000
 * res: http response
 */
import {
    Readable
} from 'stream';
const compressBar = 8 * 1024;

export class Handler {
    // static error message - do not use lambda since some runtime does not support it
    static error(msg) {
        return JSON.stringify({
            error: msg,
            duration: 0
        });
    };

    static log(msg) {
        console.log(`[${Date.now()}] ${msg}`);
    }

    // handler constructor
    constructor(response, heads, encoding, encoder) {
        this.response = response;
        this.heads = heads;
        this.encoding = encoding;
        this.encoder = encoder;
        this.metadata = {};
        this.onError = (err) => {
            this.response.writeHead(200, this.heads);
            this.response.write(Handler.error(`${err}`));
            this.response.end();
        };
        this.onNull = () => {
            this.response.writeHead(200, this.heads);
            this.response.write(Handler.error("Failed to get reply"));
            this.response.end();
        };
        this.flush = (buf) => {
            // write heads, data and end it
            this.response.writeHead(200, this.heads);
            this.response.write(buf);
            this.response.end();
        };
        this.onSuccess = (data) => {
            // comress data only when it's more than the compression bar
            if (this.encoder && data.length > compressBar) {
                var s = new Readable();
                s.push(data);
                s.push(null);
                let bufs = [];
                s.pipe(this.encoder).on('data', (c) => {
                    bufs.push(c);
                }).on('end', () => {
                    const buf = Buffer.concat(bufs);
                    this.heads["Content-Encoding"] = this.encoding;
                    this.heads["Content-Length"] = buf.length;
                    Handler.log(`Data compressed: before=${data.length}, after=${buf.length}`);
                    this.flush(buf);
                });
            } else {
                // uncompressed and sync approach
                this.flush(data);
            }
        };
        this.endWithMessage = (message) => {
            this.response.writeHead(200, this.heads);
            this.response.write(Handler.error(message));
            this.response.end();
        }
    }
};