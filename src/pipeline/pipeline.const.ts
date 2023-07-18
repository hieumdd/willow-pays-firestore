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

const timestamp = Joi.custom((value: Timestamp) => value.seconds);

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
                        date: timestamp,
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

export const CustomerAccounts: Pipeline = {
    get: () => {
        const stream = firestore.collection('customerAccounts').stream();

        const parse = new Transform({
            objectMode: true,
            transform: (row: DocumentSnapshot, _, callback) => {
                callback(null, { id: row.id, data: row.data() });
            },
        });

        const schema = Joi.object({
            id: Joi.string(),
            data: Joi.object({
                created: timestamp,
                displayId: Joi.string(),
                displayName: Joi.string(),
                email: Joi.string(),
                firstName: Joi.string(),
                id: Joi.string(),
                lastName: Joi.string(),
                mobilePhoneNumber: Joi.string(),
                modified: timestamp,
                stripeDefaultCard: Joi.string(),
                stripeId: Joi.string(),
                verifiedMobilePhoneNumber: Joi.string(),
                willowCreditLimit: Joi.number().unsafe(),
            }),
        });

        const transform = new Transform({
            objectMode: true,
            transform: (row: any, _, callback) => {
                const { value, error } = schema.validate(row, { stripUnknown: true });
                value ? callback(null, value) : callback(error);
            },
        });

        return stream.pipe(parse).pipe(transform);
    },
    table: 'CustomerAccounts',
    schema: [
        { name: 'id', type: 'STRING' },
        {
            name: 'data',
            type: 'RECORD',
            fields: [
                { name: 'created', type: 'TIMESTAMP' },
                { name: 'displayId', type: 'STRING' },
                { name: 'displayName', type: 'STRING' },
                { name: 'email', type: 'STRING' },
                { name: 'firstName', type: 'STRING' },
                { name: 'id', type: 'STRING' },
                { name: 'lastName', type: 'STRING' },
                { name: 'mobilePhoneNumber', type: 'STRING' },
                { name: 'modified', type: 'TIMESTAMP' },
                { name: 'stripeDefaultCard', type: 'STRING' },
                { name: 'stripeId', type: 'STRING' },
                { name: 'verifiedMobilePhoneNumber', type: 'STRING' },
                { name: 'willowCreditLimit', type: 'NUMERIC' },
            ],
        },
    ],
};

export const PlaidIds: Pipeline = {
    get: () => {
        const stream = firestore.collection('plaidIds').stream();

        const parse = new Transform({
            objectMode: true,
            transform: (row: DocumentSnapshot, _, callback) => {
                callback(null, { id: row.id, data: row.data() });
            },
        });

        const schema = Joi.object({
            id: Joi.string(),
            data: Joi.object({
                active: Joi.boolean(),
                createdAt: timestamp,
                customerAccountId: Joi.string(),
                plaidId: Joi.string(),
                plaidToken: Joi.string(),
            }),
        });

        const transform = new Transform({
            objectMode: true,
            transform: (row: any, _, callback) => {
                const { value, error } = schema.validate(row, { stripUnknown: true });
                value ? callback(null, value) : callback(error);
            },
        });

        return stream.pipe(parse).pipe(transform);
    },
    table: 'PlaidIds',
    schema: [
        { name: 'id', type: 'STRING' },
        {
            name: 'data',
            type: 'RECORD',
            fields: [
                { name: 'active', type: 'BOOLEAN' },
                { name: 'createdAt', type: 'TIMESTAMP' },
                { name: 'customerAccountId', type: 'STRING' },
                { name: 'plaidId', type: 'STRING' },
                { name: 'plaidToken', type: 'STRING' },
            ],
        },
    ],
};

export const Scoring: Pipeline = {
    get: () => {
        const stream = firestore.collection('scoring').stream();

        const parse = new Transform({
            objectMode: true,
            transform: (row: DocumentSnapshot, _, callback) => {
                callback(null, { id: row.id, data: row.data() });
            },
        });

        const schema = Joi.object({
            id: Joi.string(),
            data: Joi.object({
                billId: Joi.string(),
                customerAccountId: Joi.string(),
                modelName: Joi.string(),
                modelType: Joi.string(),
                resolution: Joi.string(),
                score: Joi.number().unsafe(),
                scoringDate: timestamp,
            }),
        });

        const transform = new Transform({
            objectMode: true,
            transform: (row: any, _, callback) => {
                const { value, error } = schema.validate(row, { stripUnknown: true });
                value ? callback(null, value) : callback(error);
            },
        });

        return stream.pipe(parse).pipe(transform);
    },
    table: 'Scoring',
    schema: [
        { name: 'id', type: 'STRING' },
        {
            name: 'data',
            type: 'RECORD',
            fields: [
                { name: 'billId', type: 'STRING' },
                { name: 'customerAccountId', type: 'STRING' },
                { name: 'modelName', type: 'STRING' },
                { name: 'modelType', type: 'STRING' },
                { name: 'resolution', type: 'STRING' },
                { name: 'score', type: 'NUMERIC' },
                { name: 'scoringDate', type: 'TIMESTAMP' },
            ],
        },
    ],
};
