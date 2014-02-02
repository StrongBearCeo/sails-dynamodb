/*globals describe, it, beforeEach, afterEach */
var sinon = require('sinon'),
    adapter = require('../.'),
    expect = require('chai').expect,
    should = require('chai').should();

var collectionName = "collectionName";

//fake of dynamodb
var dynamodb = {
    createTable: function (params, cb) { cb(new Error("Must be stubbed")); }
};

describe('define', function () {
    var sandbox,
        collection,
        collections,
        collectionName;

    before(function () {
        collectionName = "collectionName";
        collection = {
            config: {
                ReadCapacityUnits: 8,
                WriteCapacityUnits: 9
            }
        };
        collections = {};
        collections[collectionName] = collection;
        adapter.setDynamoDb(dynamodb);
        adapter.setCollections(collections);
    });

    beforeEach(function () {
        sandbox = sinon.sandbox.create();
    });

    afterEach(function () {
        sandbox.restore();
    });

    it('strings with a hashKey', function (done) {
        var definition = {
            simpleString: "string",
            complexString: {
                index: "hash",
                type: "string"
            }
        };

        sandbox.mock(dynamodb).expects("createTable").once()
            .withArgs({
                AttributeDefinitions: [{ AttributeName: "complexString", AttributeType: "S" }],
                KeySchema: [{ AttributeName: "complexString", KeyType: "HASH" }],
                ProvisionedThroughput: { ReadCapacityUnits: 8, WriteCapacityUnits: 9 },
                TableName: collectionName
            })
            .callsArgWithAsync(1, null, []);

        adapter.define(collectionName, definition, function (err, data) {
            should.not.exist(err);
            done(err, data);
        })
    });

    it('strings with a hashKey and rangeKey', function (done) {
        var definition = {
            simpleString: "string",
            complexHashString: {
                index: "hash",
                type: "string"
            },
            complexRangeString: {
                index: "range",
                type: "string"
            }
        };

        sandbox.mock(dynamodb).expects("createTable").once()
            .withArgs({
                AttributeDefinitions: [
                    { AttributeName: "complexHashString", AttributeType: "S" },
                    { AttributeName: "complexRangeString", AttributeType: "S" }
                ],
                KeySchema: [
                    { AttributeName: "complexHashString", KeyType: "HASH" },
                    { AttributeName: "complexRangeString", KeyType: "RANGE" }
                ],
                ProvisionedThroughput: { ReadCapacityUnits: 8, WriteCapacityUnits: 9 },
                TableName: collectionName
            })
            .callsArgWithAsync(1, null, []);

        adapter.define(collectionName, definition, function (err, data) {
            should.not.exist(err);
            done(err, data);
        })
    });

    it('strings with a hashKey and rangeKey', function (done) {
        var definition = {
            simpleString: "string",
            complexHashInteger: {
                index: "hash",
                type: "integer"
            },
            complexRangeDatetime: {
                index: "range",
                type: "datetime"
            }
        };

        sandbox.mock(dynamodb).expects("createTable").once()
            .withArgs({
                AttributeDefinitions: [
                    { AttributeName: "complexHashInteger", AttributeType: "N" },
                    { AttributeName: "complexRangeDatetime", AttributeType: "N" }
                ],
                KeySchema: [
                    { AttributeName: "complexHashInteger", KeyType: "HASH" },
                    { AttributeName: "complexRangeDatetime", KeyType: "RANGE" }
                ],
                ProvisionedThroughput: { ReadCapacityUnits: 8, WriteCapacityUnits: 9 },
                TableName: "collectionName"
            })
            .callsArgWithAsync(1, null, []);

        adapter.define(collectionName, definition, function (err, data) {
            should.not.exist(err);
            done(err, data);
        })
    });
});
