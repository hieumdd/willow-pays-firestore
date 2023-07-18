import { CloudTasksClient, protos } from '@google-cloud/tasks';
import HttpMethod = protos.google.cloud.tasks.v2.HttpMethod;
import { v4 as uuidv4 } from 'uuid';

const LOCATION = 'us-central1';
const QUEUE = 'fb-ads-insights';

const URL = process.env.PUBLIC_URL || '';

export const createTasks = async <P>(payloads: P[], nameFn: (p: P) => string) => {
    const client = new CloudTasksClient();

    const [projectId, serviceAccountEmail] = await Promise.all([
        client.getProjectId(),
        client.auth.getCredentials().then((credentials) => credentials.client_email),
    ]);

    const tasks = payloads.map((p) => ({
        parent: client.queuePath(projectId, LOCATION, QUEUE),
        task: {
            name: client.taskPath(projectId, LOCATION, QUEUE, `${nameFn(p)}-${uuidv4()}`),
            httpRequest: {
                httpMethod: HttpMethod.POST,
                headers: { 'Content-Type': 'application/json' },
                url: URL,
                oidcToken: { serviceAccountEmail },
                body: Buffer.from(JSON.stringify(p)).toString('base64'),
            },
        },
    }));

    const requests = await Promise.all(tasks.map((r) => client.createTask(r)));

    const results = requests.map(([res]) => res.name);

    return results.length;
};
