import { createWriteStream } from 'node:fs';
import { pipeline } from 'node:stream/promises';
import ndjson from 'ndjson';

import * as pipelines from './pipeline.const';
import { runPipeline } from './pipeline.service';

it('query', async () => {
    const stream = pipelines.Events.get();

    return pipeline(stream, ndjson.stringify(), createWriteStream('x.txt'));
});

it('run-pipeline', async () => {
    return runPipeline(pipelines.PlaidIds)
        .then((result) => {
            console.log({ result });
        })
        .catch((error) => {
            console.error({ error });
            throw error;
        });
}, 100_000_000);
