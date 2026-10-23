// Filtering tests

"use strict";

import { expect } from 'chai';
import { DataFilter } from 'tsbean-orm';
import { filterToSQL } from "../src/filtering";

const idConverter = (id: string) => id;

function likeValue(filter: DataFilter): string {
    const result = filterToSQL(filter.query, idConverter);
    expect(result.query).to.be.equal("\"name\" LIKE ? ESCAPE '!'");
    expect(result.values.length).to.be.equal(1);
    return result.values[0];
}

describe("Filtering: regex to LIKE conversion", () => {
    it("Contains, starts with and ends with", () => {
        expect(likeValue(DataFilter.contains("name", "foo"))).to.be.equal("%foo%");
        expect(likeValue(DataFilter.startsWith("name", "foo"))).to.be.equal("foo%");
        expect(likeValue(DataFilter.endsWith("name", "foo"))).to.be.equal("%foo");
    });

    it("Escapes LIKE wildcards and the escape character", () => {
        expect(likeValue(DataFilter.contains("name", "50%"))).to.be.equal("%50!%%");
        expect(likeValue(DataFilter.contains("name", "a_b"))).to.be.equal("%a!_b%");
        expect(likeValue(DataFilter.contains("name", "hey!"))).to.be.equal("%hey!!%");
    });

    it("Keeps backslashes as literal characters", () => {
        expect(likeValue(DataFilter.contains("name", "C:\\temp"))).to.be.equal("%C:\\temp%");
        expect(likeValue(DataFilter.contains("name", "back\\"))).to.be.equal("%back\\%");
        expect(likeValue(DataFilter.endsWith("name", "back\\"))).to.be.equal("%back\\");
    });

    it("Does not confuse literal ^ and $ with anchors", () => {
        expect(likeValue(DataFilter.contains("name", "^foo"))).to.be.equal("%^foo%");
        expect(likeValue(DataFilter.contains("name", "price$"))).to.be.equal("%price$%");
        expect(likeValue(DataFilter.startsWith("name", "x$"))).to.be.equal("x$%");
        expect(likeValue(DataFilter.endsWith("name", "^x"))).to.be.equal("%^x");
    });

    it("Handles both anchors as exact match", () => {
        expect(likeValue(new DataFilter({ operation: "regex", key: "name", regexp: /^a\.b$/ }))).to.be.equal("a.b");
    });

    it("Case insensitive", () => {
        const result = filterToSQL(DataFilter.contains("name", "Foo", true).query, idConverter);
        expect(result.query).to.be.equal("UPPER(\"name\") LIKE UPPER(?) ESCAPE '!'");
        expect(result.values).to.be.deep.equal(["%Foo%"]);
    });
});
