import { pipeline } from 'node:stream/promises';
import ndjson from 'ndjson';

import { createLoadStream } from '../bigquery.service';
import * as pipelines from './pipeline.const';
import { logger } from '../logging.service';

export const runPipeline = async (pipeline_: pipelines.Pipeline) => {
    logger.info({ action: 'start', value: pipeline_.table });

    return pipeline(pipeline_.get(), ndjson.stringify(), createLoadStream(pipeline_))
        .then(() => logger.info({ action: 'done', value: pipeline_.table }))
        .catch((error) => logger.error(error));
};

export type CreatePipelineTasksOptions = {
    start?: string;
    end?: string;
};
