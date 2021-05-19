// Filtering

"use strict";

import { GenericFilter } from "tsbean-orm";
import { replaceLikeWildcards, reverseRegexp } from "./utils";

/**
 * Parses filter and creates a SQL expression for It
 * @param filter Generic filter
 * @returns The sql query and the list of values
 */
export function filterToSQL(filter: GenericFilter, idConverter: (id: string) => string): { query: string; values: any[] } {
    let query = "";
    const values = [];

    if (!filter) {
        return { query: "", values: [] };
    }

    let first: boolean;

    switch (filter.operation) {
    case "and":
        first = true;
        for (const subCondition of filter.children) {
            const child = filterToSQL(subCondition, idConverter);

            if (child.query.length > 0) {

                if (first) {
                    first = false;
                } else {
                    query += " AND ";
                }

                query += "( " + child.query + " )";
                for (const sv of child.values) {
                    values.push(sv);
                }
            }
        }
        break;
    case "or":
        first = true;
        for (const subCondition of filter.children) {
            const child = filterToSQL(subCondition, idConverter);

            if (child.query.length > 0) {

                if (first) {
                    first = false;
                } else {
                    query += " OR ";
                }

                query += "( " + child.query + " )";
                for (const sv of child.values) {
                    values.push(sv);
                }
            }
        }
        break;
    case "not":
        {
            const child = filterToSQL(filter.child, idConverter);

            if (child.query.length > 0) {
                query = "NOT( " + child.query + " )";
                for (const sv of child.values) {
                    values.push(sv);
                }
            }
        }
        break;
    case "regex":
        {
            const field = filter.key;
            const val = filter.regexp;
            const rStr = reverseRegexp(val);

            if (val.flags.indexOf("i") >= 0) {
                query += "UPPER(\"" + idConverter(field) + "\") LIKE UPPER(?)";
            } else {
                query += "\"" + idConverter(field) + "\" LIKE ?";
            }

            if (rStr.startsWith("^")) {
                // Starts with
                values.push(replaceLikeWildcards(rStr.substr(1)) + "%");
            } else if (rStr.endsWith("$")) {
                // Starts with
                values.push("%" + replaceLikeWildcards(rStr.substr(0, rStr.length - 1)));
            } else {
                // Contains
                values.push("%" + replaceLikeWildcards(rStr) + "%");
            }
        }
        break;
    case "in":
        {
            const field = filter.key;
            let subquery = "";
            let first = true;
            for (const v of filter.values) {
                if (first) {
                    first = false;
                } else {
                    subquery += " OR ";
                }
                subquery += "(\"" + idConverter(field) + "\" = ?)";
                values.push(v);
            }
            if (subquery.length > 0) {
                query += "(" + subquery + ")";
            }
        }
        break;
    case "exists":
        if (filter.exists) {
            query += "\"" + idConverter(filter.key) + "\" IS NOT NULL";
        } else {
            query += "\"" + idConverter(filter.key) + "\" IS NULL";
        }
        break;
    case "eq":
        if (filter.value === null || filter.value === undefined) {
            query += "\"" + idConverter(filter.key) + "\" IS NULL";
        } else {
            query += "\"" + idConverter(filter.key) + "\" = ?";
            values.push(filter.value);
        }
        break;
    case "ne":
        if (filter.value === null || filter.value === undefined) {
            query += "\"" + idConverter(filter.key) + "\" IS NOT NULL";
        } else {
            query += "\"" + idConverter(filter.key) + "\" != ?";
            values.push(filter.value);
        }
        break;
    case "gt":
        query += "\"" + idConverter(filter.key) + "\" > ?";
        values.push(filter.value);
        break;
    case "lt":
        query += "\"" + idConverter(filter.key) + "\" < ?";
        values.push(filter.value);
        break;
    case "gte":
        query += "\"" + idConverter(filter.key) + "\" >= ?";
        values.push(filter.value);
        break;
    case "lte":
        query += "\"" + idConverter(filter.key) + "\" <= ?";
        values.push(filter.value);
        break;
    }

    return { query, values };
}

