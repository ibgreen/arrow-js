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
async function probe(contents, label, platform = 'browser') {
    const {outputFiles, metafile} = await build({
        stdin: {contents, resolveDir: consumer},
        outfile: join(consumer, 'browser-entry-probe.js'),
        platform,
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
    const reader = await probe(`
        export {recordBatchReaderThroughDOMStream as createReader} from 'apache-arrow/web-streams';
    `, 'Standalone DOM reader');
    const legacyReader = await probe(`
        import {RecordBatchReader} from 'apache-arrow/dom';
        export const createReader = () => RecordBatchReader.throughDOM();
    `, 'Registered DOM reader');
    assert(!Object.keys(reader.retained).some(path => /io\/whatwg\/(builder|writer)|ipc\/writer/.test(path)));
    assert(Object.keys(legacyReader.retained).some(path => /io\/whatwg\/writer/.test(path)));

    const writer = await probe(`
        import {RecordBatchStreamWriter} from 'apache-arrow';
        import {createRecordBatchWriterDOMStream} from 'apache-arrow/web-streams';
        export const createWriter = () => createRecordBatchWriterDOMStream(RecordBatchStreamWriter);
        export {tableFromArrays} from 'apache-arrow';
    `, 'Standalone DOM writer');
    assert(!Object.keys(writer.retained).some(path => /io\/whatwg\/(builder|reader)|ipc\/reader/.test(path)));
    const legacyWriter = await probe(`
        import {RecordBatchStreamWriter} from 'apache-arrow/dom';
        export const createWriter = () => RecordBatchStreamWriter.throughDOM();
        export {tableFromArrays} from 'apache-arrow';
    `, 'Registered DOM writer');

    for (const adapters of [writer.exports, legacyWriter.exports]) {
        const encoder = adapters.createWriter();
        const decoder = reader.exports.createReader();
        const table = adapters.tableFromArrays({x: Int32Array.of(1, 2, 3)});
        const batches = (async () => {
            const result = [];
            for await (const batch of encoder.readable.pipeThrough(decoder)) result.push(batch);
            return result;
        })();
        const sink = encoder.writable.getWriter();
        for (const batch of table.batches) {
            await sink.write(batch);
        }
        await sink.close();
        assert.deepEqual((await batches)[0].getChild('x').toArray(), Int32Array.of(1, 2, 3));
    }

    const nodeReader = await probe(`
        export {recordBatchReaderThroughNodeStream as createReader} from 'apache-arrow/node-streams';
    `, 'Standalone Node reader', 'node');
    await probe(`
        import {RecordBatchReader} from 'apache-arrow';
        export const createReader = () => RecordBatchReader.throughNode();
    `, 'Registered Node reader', 'node');
    assert(!Object.keys(nodeReader.retained).some(path => /io\/whatwg|io\/node\/(builder|writer)|ipc\/writer/.test(path)));

    const nodeIPC = await probe(`
        import {RecordBatchStreamWriter, tableFromArrays} from 'apache-arrow/Arrow';
        import {createRecordBatchWriterNodeStream, recordBatchReaderThroughNodeStream} from 'apache-arrow/node-streams';
        const writer = createRecordBatchWriterNodeStream(RecordBatchStreamWriter);
        const reader = writer.pipe(recordBatchReaderThroughNodeStream());
        writer.end(tableFromArrays({x: Int32Array.of(1, 2, 3)}).batches[0]);
        export async function values() {
            const result = [];
            for await (const batch of reader) result.push(...batch.getChild('x').toArray());
            return result;
        }
    `, 'Standalone Node IPC round trip', 'node');
    assert.deepEqual(await nodeIPC.exports.values(), [1, 2, 3]);

    const ipcReader = await probe("export {readIPCBytes} from 'apache-arrow/ipc/reader';", 'IPC bytes reader');
    await probe("export {tableFromIPC} from 'apache-arrow';", 'Generic tableFromIPC');
    const ipcWriter = await probe("export {writeIPCStream} from 'apache-arrow/ipc/writer';", 'IPC stream writer');
    await probe("export {tableToIPC} from 'apache-arrow';", 'Generic tableToIPC');
    assert(!Object.keys(ipcReader.retained).some(path => /ipc\/writer/.test(path)));
    assert(!Object.keys(ipcWriter.retained).some(path => /ipc\/reader/.test(path)));
    const streamConverter = await probe("export {toDOMStream} from 'apache-arrow/web-streams';", 'Iterable to DOM stream');
    assert(!Object.keys(streamConverter.retained).some(path => /ipc\/(reader|writer)/.test(path)));
    assert.deepEqual(await streamConverter.exports.toDOMStream([1, 2]).getReader().read(), {value: 1, done: false});
    await probe("export {makeField} from 'apache-arrow';", 'Standalone Field factory');
    await probe("export {Field} from 'apache-arrow';", 'Field class');
    await probe("export {encodeIPCMessage} from 'apache-arrow/ipc/metadata/message';", 'Standalone metadata encoder');
    await probe("export {Message} from 'apache-arrow';", 'IPC Message class');

    const directIPC = await probe(`
        import {tableFromArrays, makeField, Int32, Field, Message} from 'apache-arrow';
        import {encodeIPCMessage, decodeIPCMessage} from 'apache-arrow/ipc/metadata/message';
        import {readIPCBytes, readIPCBatches} from 'apache-arrow/ipc/reader';
        import {writeIPCStream, writeIPCFile, writeIPCJSON} from 'apache-arrow/ipc/writer';

        const table = tableFromArrays({x: Int32Array.of(1, 2, 3)});
        export const stream = readIPCBytes(writeIPCStream(table)).readAll()[0].getChild('x').toArray();
        export const file = readIPCBytes(writeIPCFile(table)).readAll()[0].getChild('x').toArray();
        export const json = JSON.parse(writeIPCJSON(table)).batches.length;
        export const readers = Array.from(readIPCBatches(writeIPCStream(table)), reader => reader.readAll().length);
        export const decodedField = decodeIPCMessage(encodeIPCMessage(Message.from(table.schema))).header().fields[0].name;
        export const field = makeField({name: 'x', type: new Int32()}) instanceof Field;
    `, 'Direct IPC and field round trips');
    assert.deepEqual(directIPC.exports.stream, Int32Array.of(1, 2, 3));
    assert.deepEqual(directIPC.exports.file, directIPC.exports.stream);
    assert.deepEqual(directIPC.exports.readers, [1]);
    assert.equal(directIPC.exports.json, 1);
    assert(directIPC.exports.field);
    assert.equal(directIPC.exports.decodedField, 'x');
} finally {
    rmSync(consumer, {recursive: true, force: true});
}
