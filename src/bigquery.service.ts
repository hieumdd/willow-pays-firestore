import { BigQuery, TableSchema } from '@google-cloud/bigquery';

const client = new BigQuery();

const DATASET = 'Firestore2';

type CreateLoadStreamOptions = {
    table: string;
    schema: Record<string, any>[];
};

export const createLoadStream = (options: CreateLoadStreamOptions) => {
    return client
        .dataset(DATASET)
        .table(options.table)
        .createWriteStream({
            schema: { fields: options.schema } as TableSchema,
            sourceFormat: 'NEWLINE_DELIMITED_JSON',
            createDisposition: 'CREATE_IF_NEEDED',
            writeDisposition: 'WRITE_TRUNCATE',
        });
};
