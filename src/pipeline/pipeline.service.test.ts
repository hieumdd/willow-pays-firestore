import { createWriteStream } from 'node:fs';
import { pipeline } from 'node:stream/promises';
import ndjson from 'ndjson';

import { Events } from './pipeline.const';
import { runPipeline } from './pipeline.service';

it('query', async () => {
    const stream = Events.get();

    return pipeline(stream, ndjson.stringify(), createWriteStream('x.txt'));
});

it('run-pipeline', async () => {
    return runPipeline(Events)
        .then((result) => {
            console.log({ result });
        })
        .catch((error) => {
            console.error({ error });
            throw error;
        });
});
