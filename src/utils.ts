// Utils

"use strict";

/**
 * Converts to snake case
 * @param camel String in camel case
 * @returns String in snake case
 */
export function toSnakeCase(camel: string) {
    let result = "";
    for (let i = 0; i < camel.length; i++) {
        const c = camel.charAt(i);

        if (c.toLowerCase() !== c) {
            result += "_" + c.toLowerCase();
        } else {
            result += c;
        }
    }
    return result;
}

/**
 * Converts to camel case
 * @param snake String in snake case
 * @returns String in camel case
 */
export function toCamelCase(snake: string) {
    let result = "";
    let nextUpper = false;
    for (let i = 0; i < snake.length; i++) {
        const c = snake.charAt(i);
        if (c === "_") {
            nextUpper = true;
        } else {
            if (nextUpper) {
                result += c.toUpperCase();
            } else {
                result += c.toLowerCase();
            }
            nextUpper = false;
        }
    }
    return result;
}

/**
 * Replaces LIKE wilcards to prevent injection
 * @param str Original string
 * @returns escaped string
 */
export function replaceLikeWildcards(str): string {
    return str.replace(/[\%]/g, "\\%").replace(/[\_]/g, "\\_");
}

/**
 * Translates value into something MySQL DBMs can understand
 * @param val The original value
 * @returns Parsed value
 */
export function toSQLCompatibleValue(val: any) {
    if (val === null || val === undefined) {
        return null;
    } else if (typeof val === "number" || typeof val === "string" || val instanceof Date) {
        return val;
    } else if (typeof val === "object") {
        return JSON.stringify(val);
    } else if (typeof val === "bigint") {
        return val.toString(10);
    } else if (typeof val === "boolean") {
        return val ? 1 : 0;
    } else {
        return null;
    }
}

/**
 * Normalizes results. Puts keys from snake case to camel case
 * @param results Results from database
 * @returns Normalized results
 */
export function normalizeSQLResults(results: any[]) {
    const trueResults = [];

    for (const result of results) {
        const r: any = Object.create(null);

        for (const key of Object.keys(result)) {
            r[toCamelCase(key)] = result[key];
        }

        trueResults.push(r);
    }

    return trueResults;
}

/**
 * Reverses regular expression to string
 * @param exp The regular expression
 * @returns Generator string
 */
export function reverseRegexp(exp: RegExp): string {
    const source = exp.source;
    let result = "";
    let nextEscape = false;
    for (let i = 0; i < source.length; i++) {
        const c = source.charAt(i);

        if (c === "\\") {
            if (nextEscape) {
                nextEscape = false;
                result += c;
            } else {
                nextEscape = true;
            }
        } else {
            nextEscape = false;
            result += c;
        }
    }
    return result;
}

/**
 * Converts query template to pg-compatible
 * @param queryTemplate Original template
 * @returns PostgreSQL template
 */
export function toPostgresTemplate(queryTemplate: string): string {
    let i = 1;
    while (queryTemplate.indexOf("?") >= 0) {
        queryTemplate = queryTemplate.replace("?", "$" + i);
        i++;
    }
    return queryTemplate;
}
