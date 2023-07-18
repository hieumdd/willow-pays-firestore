import { pipeline } from 'node:stream/promises';
import ndjson from 'ndjson';

import { createLoadStream } from '../bigquery.service';
import { createTasks } from '../cloud-tasks.service';
import * as pipelines from './pipeline.const';

export const runPipeline = async (pipeline_: pipelines.Pipeline) => {
    return pipeline(pipeline_.get(), ndjson.stringify(), createLoadStream(pipeline_));
};

export type CreatePipelineTasksOptions = {
    start?: string;
    end?: string;
};

export const createPipelineTasks = async () => {
    return createTasks(
        Object.keys(pipelines).map((pipeline_) => ({ pipeline: pipeline_ })),
        (task) => task.pipeline,
    );
};
