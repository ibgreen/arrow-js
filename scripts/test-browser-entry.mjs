// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

import assert from 'node:assert/strict';
import {mkdirSync, mkdtempSync, rmSync, symlinkSync} from 'node:fs';
import {tmpdir} from 'node:os';
import {join} from 'node:path';
import {fileURLToPath} from 'node:url';
import {gzipSync} from 'node:zlib';
import {build} from 'esbuild';

const consumer = mkdtempSync(join(tmpdir(), 'arrow-browser-entry-'));
const packageRoot = fileURLToPath(new URL('../targets/apache-arrow/', import.meta.url));

/** Bundle a browser consumer against the generated package exports. */
async function probe(contents, label) {
    const {outputFiles, metafile} = await build({
        stdin: {contents, resolveDir: consumer},
        outfile: join(consumer, 'browser-entry-probe.js'),
        platform: 'browser',
        format: 'esm',
        target: 'es2020',
        bundle: true,
        minify: true,
        metafile: true,
        write: false
    });
    const {contents: bytes} = outputFiles[0];
    console.log(`${label}: ${bytes.length} minified bytes, ${gzipSync(bytes).length} gzip bytes`);
    const retained = Object.values(metafile.outputs)[0].inputs;
    const exports = await import(`data:text/javascript;base64,${Buffer.from(bytes).toString('base64')}`);
    return {retained, exports};
}

try {
    mkdirSync(join(consumer, 'node_modules'));
    symlinkSync(packageRoot, join(consumer, 'node_modules', 'apache-arrow'), 'junction');
    const simpleConsumer = `export {Table, Int32, makeVector} from 'apache-arrow';`;
    await probe(simpleConsumer.replace("'apache-arrow'", "'apache-arrow/dom'"), 'DOM-initialized baseline');
    const lean = await probe(simpleConsumer, 'In-memory Arrow');
    assert(!Object.keys(lean.retained).some(path => /ipc\/(reader|writer)|io\/whatwg/.test(path)));

    const ipc = await probe(`
        import {tableFromArrays, tableFromIPC, tableToIPC} from 'apache-arrow';
        const bytes = tableToIPC(tableFromArrays({x: Int32Array.of(1, 2, 3)}));
        export const values = tableFromIPC(bytes).getChild('x').toArray();
        export async function fromStream() {
            const source = new ReadableStream({
                start(controller) {
                    controller.enqueue(bytes);
                    controller.close();
                }
            });
            return (await tableFromIPC(source)).getChild('x').toArray();
        }
    `, 'IPC round trip');
    assert.deepEqual(ipc.exports.values, Int32Array.of(1, 2, 3));
    assert.deepEqual(await ipc.exports.fromStream(), ipc.exports.values);

    const dom = await probe(`
        import 'apache-arrow/dom';
        import {Table, RecordBatchReader, Int32, makeVector} from 'apache-arrow';
        import {Table as DOMTable} from 'apache-arrow/dom';
        export const shared = Table === DOMTable;
        export const stream = RecordBatchReader.throughDOM();
        export {Int32, makeVector};
    `, 'Explicit DOM stream support');
    assert(dom.exports.shared);
    assert(dom.exports.stream.readable instanceof ReadableStream);
    assert(dom.exports.stream.writable instanceof WritableStream);
    assert(Object.keys(dom.retained).some(path => /Arrow\.dom\.mjs/.test(path)));
} finally {
    rmSync(consumer, {recursive: true, force: true});
}
