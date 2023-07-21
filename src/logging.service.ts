enum Severity {
    DEBUG = 'DEBUG',
    INFO = 'INFO',
    WARN = 'WARN',
    ERROR = 'ERROR',
}

const log = (severity: Severity) => {
    return (message: any) => console.log(JSON.stringify({ severity, message }));
};

export const debug = log(Severity.DEBUG);

export const info = log(Severity.INFO);

export const warn = log(Severity.WARN);

export const error = log(Severity.ERROR);
