import express from 'express';
import dayjs from 'dayjs';
import utc from 'dayjs/plugin/utc';
import { http } from '@google-cloud/functions-framework';
import Joi from 'joi';

import * as pipelines from './facebook/pipeline.const';
import { runPipeline, createPipelineTasks } from './pipeline/pipeline.service';

dayjs.extend(utc);

const app = express();

app.use(({ path, body }, res, next) => {
    const log = { path, body };
    console.log(JSON.stringify(log));
    next();
});

type CreatePipelineTasksBody = {
    start: string;
    end: string;
};

app.use('/task', (req, res) => {
    Joi.object<CreatePipelineTasksBody>({
        start: Joi.string()
            .optional()
            .empty(null)
            .allow(null)
            .default(dayjs.utc().subtract(7, 'day').format('YYYY-MM-DD')),
        end: Joi.string()
            .optional()
            .empty(null)
            .allow(null)
            .default(dayjs.utc().format('YYYY-MM-DD')),
    })
        .validateAsync(req.body)
        .then((body) =>
            createPipelineTasks(body)
                .then((result) => {
                    res.status(200).json({ result });
                })
                .catch((error) => {
                    console.error(JSON.stringify(error));
                    res.status(500).json({ error });
                }),
        )
        .catch((error) => {
            console.error(JSON.stringify(error));
            res.status(500).json({ error });
        });
});

type RunPipelineBody = {
    accountId: string;
    start: string;
    end: string;
    pipeline: keyof typeof pipelines;
};

app.use('/', (req, res) => {
    Joi.object<RunPipelineBody>({
        accountId: Joi.string(),
        start: Joi.string(),
        end: Joi.string(),
        pipeline: Joi.string(),
    })
        .validateAsync(req.body)
        .then(async ({ pipeline, accountId, start, end }) => {
            return runPipeline({ accountId, start, end }, pipelines[pipeline])
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
