import { createLogger, format, transports } from 'winston';
const { combine, printf, timestamp } = format;

export const logger = createLogger({
    format: combine(
        timestamp(),
        printf(({ level, message, timestamp }) => {
            return JSON.stringify({ severity: level, timestamp, message });
        }),
    ),
    transports: [new transports.Console()],
});
