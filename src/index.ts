import cron from 'node-cron';

import { logger } from './logging.service';
import * as pipelines from './pipeline/pipeline.const';
import { runPipeline } from './pipeline/pipeline.service';

Object.values(pipelines).forEach((pipeline) => {
    cron.schedule('0 */4 * * *', () => {
        runPipeline(pipeline)
            .then((result) => logger.info(result))
            .catch((error) => logger.error(error));
    });
});
