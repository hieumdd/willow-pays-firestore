import process from 'process';
import cron from 'node-cron';

import { logger } from './logging.service';
import * as pipelines from './pipeline/pipeline.const';
import { runPipeline } from './pipeline/pipeline.service';

Object.values(pipelines).forEach((pipeline) => {
    logger.info({ action: 'setting-schedule', pipeline: pipeline.table });
    cron.schedule('0 */4 * * *', () => {
        runPipeline(pipeline);
    });
});

process.on('SIGINT', () => {
    logger.info({ action: 'interupt' });
    process.exit(0);
});
