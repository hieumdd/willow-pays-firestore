import { createLogger, format, transports } from 'winston';
const { combine, metadata, printf } = format;

export const logger = createLogger({
    format: combine(
        metadata(),
        printf(({ level, message, metadata }) => {
            return JSON.stringify({ severity: level, message, metadata });
        }),
    ),
    transports: [new transports.Console()],
});
