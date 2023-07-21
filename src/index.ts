import cron from 'node-cron';

import * as pipelines from './pipeline/pipeline.const';
import { runPipeline } from './pipeline/pipeline.service';

(async () => {
    await runPipeline(pipelines.CustomerAccounts)
        .then((result) => console.log(result))
        .catch((error) => console.log({ error }));
})();
