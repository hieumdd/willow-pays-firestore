import { program } from 'commander';

import * as pipelines from './pipeline/pipeline.const';
import { runPipeline } from './pipeline/pipeline.service';

program.option('-p, --pipeline <name>');
program.parse(process.argv);

(async () => {
    const { pipeline } = program.opts<{ pipeline: keyof typeof pipelines | undefined }>();

    if (!pipeline) {
        return;
    }

    runPipeline(pipelines[pipeline])
        .then((result) => console.log({ result }))
        .catch((error) => console.log({ error }));
})();
