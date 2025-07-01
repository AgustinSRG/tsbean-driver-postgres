// Driver implementation

"use strict";

import { Pool } from "pg";
import Cursor from "pg-cursor";
import { DataSourceDriver, DataSource, GenericKeyValue, GenericRow, SortDirection, GenericFilter, QueryExtraOptions } from "tsbean-orm";
import { filterToSQL } from "./filtering";
import { normalizeSQLResults, toCamelCase, toPostgresTemplate, toSnakeCase, toSQLCompatibleValue } from "./utils";

const CURSOR_READ_AMOUNT = 100;

interface NameConversion {
    parseResults: (r: GenericRow[]) => GenericRow[];
    toSQL: (n: string) => string;
    toBean: (n: string) => string;
}

/**
 * PostgreSQL source configuration
 */
export interface PostgreSQLSourceConfiguration {
    host: string;
    port?: number;
    user?: string;
    password?: string,
    connections?: number;
    database: string;
    disableIdentifierConversion?: boolean;
    customIdentifierConversion?: NameConversion;
}

/**
 * Driver class
 */
export class PostgreSQLDriver implements DataSourceDriver {

    /**
     * Creates a data source for this driver
     * @param config 
     * @returns The data source
     */
    public static createDataSource(config: PostgreSQLSourceConfiguration): DataSource {
        const driver = new PostgreSQLDriver(config);
        return new DataSource("tsbean.driver.postgres", driver);
    }

    public pool: Pool;
    public idConversion: NameConversion;

    // private connection: Connection;

    constructor(config: PostgreSQLSourceConfiguration) {
        this.pool = new Pool({
            /* Single connection for sequential workers, multiple connections for server workers */
            max: config.connections || 4,
            host: config.host,
            port: config.port || 5432,
            user: config.user,
            password: config.password,
            database: config.database,
        });
        if (config.customIdentifierConversion) {
            this.idConversion = config.customIdentifierConversion;
        } else if (config.disableIdentifierConversion) {
            this.idConversion = {
                parseResults: a => a,
                toSQL: a => a,
                toBean: a => a,
            };
        } else {
            this.idConversion = {
                parseResults: normalizeSQLResults,
                toSQL: toSnakeCase,
                toBean: toCamelCase,
            };
        }
    }

    /**
     * Runs a custom SQL query
     * @param sentence The SQL sentence
     * @param values The values to replace
     * @returns The results
     */
    public customQuery(sentence: string, values: any[]): Promise<any> {
        return new Promise<{ results: any, fileds: any[] }>(function (resolve, reject) {
            this.pool.query(sentence, values, function (error, results) {
                if (error) {
                    return reject(error);
                }
                resolve(results);
            }.bind(this));
        }.bind(this));
    }

    /**
     * Finds a row by primary key
     * @param table Table or collection name
     * @param keyName Name of the key
     * @param keyValue Value of the key
     */
    findByKey(table: string, keyName: string, keyValue: any): Promise<GenericRow> {
        const sentence = "SELECT * FROM \"" + table + "\" WHERE \"" + this.idConversion.toSQL(keyName) + "\" = $1";
        const values = [keyValue];
        return new Promise<any>(function (resolve, reject) {
            this.pool.query(sentence, values, function (error, results) {
                if (error) {
                    return reject(error);
                }
                if (results.rows && results.rows.length > 0) {
                    resolve(this.idConversion.parseResults(results.rows)[0]);
                } else {
                    resolve(null);
                }
            }.bind(this));
        }.bind(this));
    }

    private generateSelectSentence(table: string, filter: GenericFilter, sortBy: string, sortDir: SortDirection, skip: number, limit: number, projection: Set<string>, queryExtraOptions: QueryExtraOptions): { sql: string, values: any[] } {
        let sentence = "SELECT ";
        const values = [];

        if (projection) {
            const toProject = projection.keys();
            const proj = [];

            for (const f of toProject) {
                proj.push("\"" + this.idConversion.toSQL(f) + "\"");
            }

            sentence += proj.join(", ");
        } else {
            sentence += "*";
        }

        sentence += " FROM \"" + table + "\"";

        const cond1 = filterToSQL(filter, this.idConversion.toSQL);

        if (cond1.query.length > 0) {
            sentence += " WHERE " + cond1.query;
            for (const v of cond1.values) {
                values.push(v);
            }
        }

        if (sortBy) {
            sentence += " ORDER BY \"" + this.idConversion.toSQL(sortBy) + "\" " + (sortDir === "desc" ? "DESC" : "ASC");
        }

        if (limit !== null && limit >= 0) {
            sentence += " LIMIT " + limit;
        }

        if (skip !== null && skip >= 0) {
            sentence += " OFFSET " + skip;
        }

        return { sql: toPostgresTemplate(sentence), values: values };
    }

    /**
     * Finds rows
     * @param table Table or collection name
     * @param filter Filter to apply
     * @param sortBy Sort results by this field. Leave as null for default sorting
     * @param sortDir "asc" or "desc". Leave as null for default sorting
     * @param skip Number of rows to skip. Leave as -1 for no skip
     * @param limit Limit of results. Leave as -1 for no limit
     * @param projection List of fields to fetch from the table. Leave as null to fetch them all.
     * @param queryExtraOptions Additional query options
     */
    find(table: string, filter: GenericFilter, sortBy: string, sortDir: SortDirection, skip: number, limit: number, projection: Set<string>, queryExtraOptions: QueryExtraOptions): Promise<GenericRow[]> {
        const sentenceAndValues = this.generateSelectSentence(table, filter, sortBy, sortDir, skip, limit, projection, queryExtraOptions);
        const sentence = sentenceAndValues.sql;
        const values = sentenceAndValues.values;

        return new Promise<any[]>(function (resolve, reject) {
            this.pool.query(sentence, values, function (error, results) {
                if (error) {
                    return reject(error);
                }
                if (results.rows) {
                    resolve(this.idConversion.parseResults(results.rows));
                } else {
                    resolve([]);
                }
            }.bind(this));
        }.bind(this));
    }

    /**
     * Counts the number of rows matching a condition
     * @param table Table or collection name
     * @param filter Filter to apply
     * @param queryExtraOptions Additional query options
     */
    count(table: string, filter: GenericFilter, queryExtraOptions: QueryExtraOptions): Promise<number> {
        let sentence = "SELECT COUNT(*) AS \"count\" FROM \"" + table + "\"";
        const values = [];
        const cond1 = filterToSQL(filter, this.idConversion.toSQL);

        if (cond1.query.length > 0) {
            sentence += " WHERE " + cond1.query;
            for (const v of cond1.values) {
                values.push(v);
            }
        }

        return new Promise<number>(function (resolve, reject) {
            this.pool.query(toPostgresTemplate(sentence), values, function (error, results) {
                if (error) {
                    return reject(error);
                }
                if (results.rows && results.rows.length > 0) {
                    resolve(parseInt(results.rows[0].count, 10) || 0);
                } else {
                    resolve(0);
                }
            }.bind(this));
        }.bind(this));
    }

    /**
     * Finds rows (stream mode). You can parse each row with an ASYNC function
     * @param table Table or collection name
     * @param filter Filter to apply
     * @param sortBy Sort results by this field. Leave as null for default sorting
     * @param sortDir "asc" or "desc". Leave as null for default sorting
     * @param skip Number of rows to skip. Leave as -1 for no skip
     * @param limit Limit of results. Leave as -1 for no limit
     * @param projection List of fields to fetch from the table. Leave as null to fetch them all.
     * @param queryExtraOptions Additional query options
     * @param each Function to parse each row
     */
    findStream(table: string, filter: GenericFilter, sortBy: string, sortDir: SortDirection, skip: number, limit: number, projection: Set<string>, queryExtraOptions: QueryExtraOptions, each: (row: GenericRow) => Promise<void>): Promise<void> {
        const sentenceAndValues = this.generateSelectSentence(table, filter, sortBy, sortDir, skip, limit, projection, queryExtraOptions);
        const sentence = sentenceAndValues.sql;
        const values = sentenceAndValues.values;

        return new Promise<void>(async function (resolve, reject) {
            let client;

            try {
                client = await this.pool.connect();
            } catch (ex) {
                return reject(ex);
            }

            const cursor = client.query(new Cursor(sentence, values));

            let resultsEnded = false;
            while (!resultsEnded) {
                const partialResults: any[] = await (new Promise<any[]>(function (resolve) {
                    cursor.read(CURSOR_READ_AMOUNT, (err, rows) => {
                        if (err) {
                            return resolve([]);
                        }
                        resolve(rows);
                    });
                }));

                if (partialResults.length > 0) {
                    for (const row of partialResults) {
                        await each(this.idConversion.parseResults([row])[0]);
                    }
                } else {
                    resultsEnded = true;
                }
            }

            resolve();

            cursor.close(() => {
                client.release();
            })
        }.bind(this));
    }


    /**
     * Finds rows (stream mode). You can parse each row with a SYNC function
     * @param table Table or collection name
     * @param filter Filter to apply
     * @param sortBy Sort results by this field. Leave as null for default sorting
     * @param sortDir "asc" or "desc". Leave as null for default sorting
     * @param skip Number of rows to skip. Leave as -1 for no skip
     * @param limit Limit of results. Leave as -1 for no limit
     * @param projection List of fields to fetch from the table. Leave as null to fetch them all.
     * @param queryExtraOptions Additional query options
     * @param each Function to parse each row
     */
    findStreamSync(table: string, filter: GenericFilter, sortBy: string, sortDir: SortDirection, skip: number, limit: number, projection: Set<string>, queryExtraOptions: QueryExtraOptions, each: (row: any) => void): Promise<void> {
        const sentenceAndValues = this.generateSelectSentence(table, filter, sortBy, sortDir, skip, limit, projection, queryExtraOptions);
        const sentence = sentenceAndValues.sql;
        const values = sentenceAndValues.values;

        return new Promise<void>(async function (resolve, reject) {
            let client;

            try {
                client = await this.pool.connect();
            } catch (ex) {
                return reject(ex);
            }

            const cursor = client.query(new Cursor(sentence, values));

            let resultsEnded = false;
            while (!resultsEnded) {
                const partialResults: any[] = await (new Promise<any[]>(function (resolve) {
                    cursor.read(CURSOR_READ_AMOUNT, (err, rows) => {
                        if (err) {
                            return resolve([]);
                        }
                        resolve(rows);
                    });
                }));

                if (partialResults.length > 0) {
                    for (const row of partialResults) {
                        each(this.idConversion.parseResults([row])[0]);
                    }
                } else {
                    resultsEnded = true;
                }
            }

            resolve();

            cursor.close(() => {
                client.release();
            })
        }.bind(this));
    }

    /**
     * Inserts a row
     * @param table Table or collection name
     * @param row Row to insert
     * @param key The name of the primary key (if any)
     * @param callback Callback to set the value of the primary key after inserting (Optional, only if auto-generated key)
     */
    insert(table: string, row: GenericRow, key: string, callback?: (value: GenericKeyValue) => void): Promise<void> {
        let sentence = "INSERT INTO \"" + table + "\"(";
        const keys = Object.keys(row);
        const sqlKeys = [];
        const values = [];
        const qm = [];
        let insertReturns = false;

        for (const k of keys) {
            if (key === k && (row[k] === null || row[k] === undefined)) {
                continue;
            }
            sqlKeys.push("\"" + this.idConversion.toSQL(k) + "\"");
            values.push(toSQLCompatibleValue(row[k]));
            qm.push("?");
        }

        sentence += sqlKeys.join(",");

        sentence += ") VALUES (";

        sentence += qm.join(",");

        sentence += ")";

        if (key && (row[key] === null || row[key] === undefined)) {
            // Auto-generated key
            sentence += " RETURNING \"" + this.idConversion.toSQL(key) + "\"";
            insertReturns = true;
        }

        return new Promise<void>(function (resolve, reject) {
            this.pool.query(toPostgresTemplate(sentence), values, function (error, results) {
                if (error) {
                    return reject(error);
                }
                if (insertReturns) {
                    if (results.rows && results.rows.length > 0) {
                        callback(results.rows[0][this.idConversion.toSQL(key)]);
                    }
                }
                resolve();
            }.bind(this));
        }.bind(this));
    }

    /**
     * Inserts many rows
     * @param table Table or collection name
     * @param rows List of rows to insert
     */
    async batchInsert(table: string, rows: GenericRow[]): Promise<void> {
        for (const row of rows) {
            await this.insert(table, row, null);
        }
    }

    /**
     * Updates a row
     * @param table Table or collection name
     * @param keyName Name of the key
     * @param keyValue Value of the key
     * @param updated Updated row
     */
    update(table: string, keyName: string, keyValue: GenericKeyValue, updated: GenericRow): Promise<void> {
        const keys = Object.keys(updated);

        if (keys.length === 0) {
            return; // Nothing to update
        }

        let sentence = "UPDATE \"" + table + "\" SET ";
        const values = [];
        let first = true;

        for (const key of keys) {
            if (first) {
                first = false;
            } else {
                sentence += ", ";
            }

            sentence += "\"" + this.idConversion.toSQL(key) + "\" = ?";
            values.push(toSQLCompatibleValue(updated[key]));
        }

        sentence += " WHERE \"" + this.idConversion.toSQL(keyName) + "\" = ?";
        values.push(keyValue);

        return new Promise<void>(function (resolve, reject) {
            this.pool.query(toPostgresTemplate(sentence), values, function (error, results) {
                if (error) {
                    return reject(error);
                }
                resolve();
            }.bind(this));
        }.bind(this));
    }

    /**
     * Updates many rows
     * @param table Table or collection name
     * @param filter Filter to apply
     * @param updated Updated row
     * @returns The number of affected rows
     */
    updateMany(table: string, filter: GenericFilter, updated: GenericRow): Promise<number> {
        const keys = Object.keys(updated);

        if (keys.length === 0) {
            return; // Nothing to update
        }

        let sentence = "UPDATE \"" + table + "\" SET ";
        const values = [];
        let first = true;

        for (const key of keys) {
            if (first) {
                first = false;
            } else {
                sentence += ", ";
            }

            if (typeof updated[key] === "object" && updated[key] !== null) {
                if (updated[key].update === "set") {
                    sentence += "\"" + this.idConversion.toSQL(key) + "\" = ?";
                    values.push(toSQLCompatibleValue(updated[key].value));
                } else if (updated[key].update === "inc") {
                    sentence += "\"" + this.idConversion.toSQL(key) + "\" = \"" + this.idConversion.toSQL(key) + "\" + ?";
                    values.push(toSQLCompatibleValue(updated[key].value));
                } else {
                    sentence += "\"" + this.idConversion.toSQL(key) + "\" = ?";
                    values.push(toSQLCompatibleValue(updated[key]));
                }
            } else {
                sentence += "\"" + this.idConversion.toSQL(key) + "\" = ?";
                values.push(toSQLCompatibleValue(updated[key]));
            }
        }

        const cond1 = filterToSQL(filter, this.idConversion.toSQL);

        if (cond1.query.length > 0) {
            sentence += " WHERE " + cond1.query;
            for (const v of cond1.values) {
                values.push(v);
            }
        }

        return new Promise<number>(function (resolve, reject) {
            this.pool.query(toPostgresTemplate(sentence), values, function (error, results) {
                if (error) {
                    return reject(error);
                }
                resolve(results.rowCount);
            }.bind(this));
        }.bind(this));
    }

    /**
     * Deletes a row
     * @param table Table or collection name
     * @param keyName Name of the key
     * @param keyValue Value of the key
     * @returns true if the row was deleted, false if the row didn't exists
     */
    delete(table: string, keyName: string, keyValue: GenericKeyValue): Promise<boolean> {
        const sentence = "DELETE FROM \"" + table + "\" WHERE \"" + this.idConversion.toSQL(keyName) + "\" = $1";
        const values = [keyValue];
        return new Promise<boolean>(function (resolve, reject) {
            this.pool.query(sentence, values, function (error, results) {
                if (error) {
                    return reject(error);
                }
                resolve(results.rowCount !== 0);
            }.bind(this));
        }.bind(this));
    }

    /**
     * Deletes many rows
     * @param table Table or collection name
     * @param filter Filter to apply
     * @returns The number of affected rows
     */
    deleteMany(table: string, filter: GenericFilter): Promise<number> {
        let sentence = "DELETE FROM \"" + table + "\"";
        const values = [];

        const cond1 = filterToSQL(filter, this.idConversion.toSQL);

        if (cond1.query.length > 0) {
            sentence += " WHERE " + cond1.query;
            for (const v of cond1.values) {
                values.push(v);
            }
        }

        return new Promise<number>(function (resolve, reject) {
            this.pool.query(toPostgresTemplate(sentence), values, function (error, results) {
                if (error) {
                    return reject(error);
                }
                resolve(results.rowCount);
            }.bind(this));
        }.bind(this));
    }

    /**
     * Summatory of many rows
     * @param table Table or collection name
     * @param filter Filter to apply
     * @param id Name of the primary key
     * @param field Name of the field to aggregate
     */
    sum(table: string, filter: GenericFilter, id: string, field: string): Promise<number> {
        let sentence = "SELECT SUM(\"" + this.idConversion.toSQL(field) + "\") AS \"" + this.idConversion.toSQL(field) + "\" FROM \"" + table + "\"";
        const values = [];

        const cond1 = filterToSQL(filter, this.idConversion.toSQL);

        if (cond1.query.length > 0) {
            sentence += " WHERE " + cond1.query;
            for (const v of cond1.values) {
                values.push(v);
            }
        }

        return new Promise<number>(function (resolve, reject) {
            this.pool.query(toPostgresTemplate(sentence), values, function (error, results) {
                if (error) {
                    return reject(error);
                }
                const normalized = this.idConversion.parseResults(results.rows);
                if (normalized && normalized.length) {
                    resolve(parseInt(normalized[0][field], 10) || 0);
                } else {
                    resolve(0);
                }
            }.bind(this));
        }.bind(this));
    }

    /**
     * Atomic increment
     * @param table Table or collection name
     * @param keyName The name of the key
     * @param keyValue The value ofthe key
     * @param prop The field to increment
     * @param inc The amount to increment
     */
    increment(table: string, keyName: string, keyValue: GenericKeyValue, prop: string, inc: number): Promise<void> {
        const sentence = "UPDATE \"" + table + "\" SET \"" + this.idConversion.toSQL(prop) + "\" = \"" + this.idConversion.toSQL(prop) + "\" + $1 WHERE \"" + this.idConversion.toSQL(keyName) + "\" = $2";
        const values = [inc, keyValue];
        return new Promise<any>(function (resolve, reject) {
            this.pool.query(sentence, values, function (error, results) {
                if (error) {
                    return reject(error);
                }
                resolve();
            }.bind(this));
        }.bind(this));
    }
}
