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

        const subcollections = new Transform({
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

        return events.pipe(subcollections).pipe(transform);
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
        let count = 0;

        const stream = firestore.collection('customerAccounts').stream();

        const subcollections = new Transform({
            objectMode: true,
            transform: async (row: DocumentSnapshot, _, callback) => {
                const collections = [
                    'bankTransactionSummaries',
                    'customerBills',
                    'events',
                ] as const;

                Promise.all(
                    collections.map((collection) => {
                        return row.ref
                            .collection(collection)
                            .listDocuments()
                            .then((refs) => Promise.all(refs.map((ref) => ref.get())));
                    }),
                )
                    .then((snapshots) => {
                        const [bankTransactionSummaries, customerBills, events] = snapshots;

                        callback(null, {
                            id: row.id,
                            data: {
                                ...row.data(),
                                bankTransactionSummaries: bankTransactionSummaries.map(
                                    (snapshot) => ({
                                        id: snapshot.id,
                                        data: snapshot.data(),
                                    }),
                                ),
                                customerBills: customerBills.map((snapshot) => ({
                                    id: snapshot.id,
                                    data: snapshot.data(),
                                })),
                                events: events.map((snapshot) => ({
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
                bankTransactionSummaries: Joi.array().items({
                    id: Joi.string(),
                    data: Joi.object({
                        balanceDelta: Joi.number().unsafe(),
                        code: Joi.string(),
                        customerAccountId: Joi.string(),
                        depositAmountTotal: Joi.number().unsafe(),
                        depositCount: Joi.number().unsafe(),
                        endDate: timestamp,
                        overdraft: Joi.boolean(),
                        salariesAmountTotal: Joi.number().unsafe(),
                        startDate: timestamp,
                        transactionCount: Joi.number().unsafe(),
                        withdrawalAmountTotal: Joi.number().unsafe(),
                        withdrawalCount: Joi.number().unsafe(),
                    }),
                }),
                customerBills: Joi.array().items({
                    id: Joi.string(),
                    data: Joi.object({
                        billAmount: Joi.number().unsafe(),
                        billDueDate: timestamp,
                        billPayToName: Joi.string(),
                        billRepaymentMethod: Joi.string(),
                        billRepaymentsSelection: Joi.number().unsafe(),
                        billStatus: Joi.string(),
                        billType: Joi.string(),
                        created: timestamp,
                        customerAccountId: Joi.string(),
                        displayId: Joi.string(),
                        displayName: Joi.string(),
                        id: Joi.string(),
                        modified: timestamp,
                        numberOfPayments: Joi.number().unsafe(),
                    }),
                }),
                events: Joi.array().items({
                    id: Joi.string(),
                    data: Joi.object({
                        customerAccountId: Joi.string(),
                        emailResult: Joi.string(),
                        timestamp: timestamp,
                        trigger: Joi.string(),
                    }),
                }),
            }),
        });

        const transform = new Transform({
            objectMode: true,
            transform: (row: any, _, callback) => {
                count = count + 1;
                console.log(count);
                const { value, error } = schema.validate(row, { stripUnknown: true });
                value ? callback(null, value) : callback(error);
            },
        });

        return stream.pipe(subcollections).pipe(transform);
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
                {
                    name: 'bankTransactionSummaries',
                    type: 'RECORD',
                    mode: 'REPEATED',
                    fields: [
                        { name: 'id', type: 'STRING' },
                        {
                            name: 'data',
                            type: 'RECORD',
                            fields: [
                                { name: 'balanceDelta', type: 'NUMERIC' },
                                { name: 'code', type: 'STRING' },
                                { name: 'customerAccountId', type: 'STRING' },
                                { name: 'depositAmountTotal', type: 'NUMERIC' },
                                { name: 'depositCount', type: 'NUMERIC' },
                                { name: 'endDate', type: 'TIMESTAMP' },
                                { name: 'overdraft', type: 'BOOLEAN' },
                                { name: 'salariesAmountTotal', type: 'NUMERIC' },
                                { name: 'startDate', type: 'TIMESTAMP' },
                                { name: 'transactionCount', type: 'NUMERIC' },
                                { name: 'withdrawalAmountTotal', type: 'NUMERIC' },
                                { name: 'withdrawalCount', type: 'NUMERIC' },
                            ],
                        },
                    ],
                },
                {
                    name: 'customerBills',
                    type: 'RECORD',
                    mode: 'REPEATED',
                    fields: [
                        { name: 'id', type: 'STRING' },
                        {
                            name: 'data',
                            type: 'RECORD',
                            fields: [
                                { name: 'billAmount', type: 'NUMERIC' },
                                { name: 'billDueDate', type: 'TIMESTAMP' },
                                { name: 'billPayToName', type: 'STRING' },
                                { name: 'billRepaymentMethod', type: 'STRING' },
                                { name: 'billRepaymentsSelection', type: 'STRING' },
                                { name: 'billStatus', type: 'STRING' },
                                { name: 'billType', type: 'STRING' },
                                { name: 'created', type: 'TIMESTAMP' },
                                { name: 'customerAccountId', type: 'STRING' },
                                { name: 'displayId', type: 'STRING' },
                                { name: 'displayName', type: 'STRING' },
                                { name: 'id', type: 'STRING' },
                                { name: 'modified', type: 'TIMESTAMP' },
                                { name: 'numberOfPayments', type: 'NUMERIC' },
                            ],
                        },
                    ],
                },
                {
                    name: 'events',
                    type: 'RECORD',
                    mode: 'REPEATED',
                    fields: [
                        { name: 'id', type: 'STRING' },
                        {
                            name: 'data',
                            type: 'RECORD',
                            fields: [
                                { name: 'customerAccountId', type: 'STRING' },
                                { name: 'emailResult', type: 'STRING' },
                                { name: 'timestamp', type: 'TIMESTAMP' },
                                { name: 'trigger', type: 'STRING' },
                            ],
                        },
                    ],
                },
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
                revocationReason: Joi.string(),
                revokedAt: timestamp,
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
                { name: 'revocationReason', type: 'STRING' },
                { name: 'revokedAt', type: 'TIMESTAMP' },
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

export const Stripe: Pipeline = {
    get: () => {
        const stream = firestore.collection('stripe').stream();

        const parse = new Transform({
            objectMode: true,
            transform: (row: DocumentSnapshot, _, callback) => {
                callback(null, { id: row.id, data: row.data() });
            },
        });

        const schema = Joi.object({
            id: Joi.string(),
            data: Joi.object({
                customerAccountId: Joi.string(),
                disputed: Joi.number(),
                failedCharges: Joi.number(),
                paidCharges: Joi.number(),
                refunded: Joi.number(),
                stripeId: Joi.string(),
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

        return stream.pipe(parse).pipe(transform);
    },
    table: 'Stripe',
    schema: [
        { name: 'id', type: 'STRING' },
        {
            name: 'data',
            type: 'RECORD',
            fields: [
                { name: 'customerAccountId', type: 'STRING' },
                { name: 'disputed', type: 'NUMERIC' },
                { name: 'failedCharges', type: 'NUMERIC' },
                { name: 'paidCharges', type: 'NUMERIC' },
                { name: 'refunded', type: 'NUMERIC' },
                { name: 'stripeId', type: 'STRING' },
                { name: 'totalCollected', type: 'NUMERIC' },
            ],
        },
    ],
};
