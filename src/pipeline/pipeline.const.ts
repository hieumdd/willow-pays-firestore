import { Readable, Transform } from 'node:stream';
import { DocumentSnapshot, Timestamp } from '@google-cloud/firestore';
import Joi from 'joi';

import { firestore } from '../firestore.service';
import { isArray } from 'lodash';

export type Pipeline = {
    get: () => Readable;
    table: string;
    schema: any[];
};

export const Events: Pipeline = {
    get: () => {
        const events = firestore.collection('events').stream();

        const payments = new Transform({
            objectMode: true,
            transform: (row: DocumentSnapshot, _, callback) => {
                row.ref
                    .collection('PAYMENT_EVENT')
                    .listDocuments()
                    .then((refs) => Promise.all(refs.map((ref) => ref.get())))
                    .then((snapshots) => {
                        callback(null, {
                            id: row.id,
                            data: {
                                ...row.data(),
                                paymentEvents: snapshots.map((snapshot) => ({
                                    id: snapshot.id,
                                    data: snapshot.data(),
                                })),
                            },
                        });
                    })
                    .catch((error) => callback(error));
            },
        });

        const schema = Joi.object({
            id: Joi.string(),
            data: Joi.object({
                eventCount: Joi.number().unsafe(),
                paymentEvents: Joi.array().items({
                    id: Joi.string(),
                    data: Joi.object({
                        batchName: Joi.string(),
                        billId: Joi.string(),
                        customerAccountId: Joi.string(),
                        date: Joi.custom((value: Timestamp) => value.seconds),
                        repaymentSequence: Joi.custom((value) => {
                            return isArray(value) ? value : [value];
                        }),
                        stripeReceiptUrl: Joi.string(),
                        success: Joi.boolean(),
                        type: Joi.string(),
                    }),
                }),
                successfulPayments: Joi.number().unsafe(),
                totalCollected: Joi.number().unsafe(),
            }),
        });

        const transform = new Transform({
            objectMode: true,
            transform: (row: any, _, callback) => {
                const { value, error } = schema.validate(row, { stripUnknown: true });
                value ? callback(null, value) : callback(error);
            },
        });

        return events.pipe(payments).pipe(transform);
    },
    table: 'Events',
    schema: [
        { name: 'id', type: 'STRING' },
        {
            name: 'data',
            type: 'RECORD',
            fields: [
                { name: 'eventCount', type: 'NUMERIC' },
                {
                    name: 'paymentEvents',
                    type: 'RECORD',
                    mode: 'REPEATED',
                    fields: [
                        { name: 'id', type: 'STRING' },
                        {
                            name: 'data',
                            type: 'RECORD',
                            fields: [
                                { name: 'batchName', type: 'STRING' },
                                { name: 'billId', type: 'STRING' },
                                { name: 'customerAccountId', type: 'STRING' },
                                { name: 'date', type: 'TIMESTAMP' },
                                { name: 'repaymentSequence', type: 'NUMERIC', mode: 'REPEATED' },
                                { name: 'stripeReceiptUrl', type: 'STRING' },
                                { name: 'success', type: 'BOOLEAN' },
                                { name: 'type', type: 'STRING' },
                            ],
                        },
                    ],
                },
                { name: 'successfulPayments', type: 'NUMERIC' },
                { name: 'totalCollected', type: 'NUMERIC' },
            ],
        },
    ],
};
