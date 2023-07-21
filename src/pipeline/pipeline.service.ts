import { pipeline } from 'node:stream/promises';
import ndjson from 'ndjson';

import { createLoadStream } from '../bigquery.service';
import * as pipelines from './pipeline.const';

export const runPipeline = async (pipeline_: pipelines.Pipeline) => {
    return pipeline(pipeline_.get(), ndjson.stringify(), createLoadStream(pipeline_)).then(() => ({
        pipeline: pipeline_.table,
    }));
};

export type CreatePipelineTasksOptions = {
    start?: string;
    end?: string;
};
