import express from 'express';
import { http } from '@google-cloud/functions-framework';
import Joi from 'joi';

import * as pipelines from './pipeline/pipeline.const';
import { runPipeline, createPipelineTasks } from './pipeline/pipeline.service';

const app = express();

app.use(({ path, body }, res, next) => {
    const log = { path, body };
    console.log(JSON.stringify(log));
    next();
});

app.use('/task', (req, res) => {
    createPipelineTasks()
        .then((result) => {
            res.status(200).json({ result });
        })
        .catch((error) => {
            console.error(JSON.stringify(error));
            res.status(500).json({ error });
        });
});

type RunPipelineBody = {
    pipeline: keyof typeof pipelines;
};

app.use('/', (req, res) => {
    Joi.object<RunPipelineBody>({
        pipeline: Joi.string(),
    })
        .validateAsync(req.body)
        .then(({ pipeline }) => {
            runPipeline(pipelines[pipeline])
                .then((result) => {
                    res.status(200).json({ result });
                })
                .catch((error) => {
                    console.error(JSON.stringify(error));
                    res.status(500).json({ error });
                });
        })

        .catch((error) => {
            console.error(JSON.stringify(error));
            res.status(500).json({ error });
        });
});

http('main', app);
