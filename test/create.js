/*globals describe, it, beforeEach, afterEach */
var sinon = require('sinon'),
    adapter = require('../.'),
    expect = require('chai').expect,
    should = require('chai').should();

var definition = {
    simpleString: "string",
    complexString: {
        type: "string"
    }
};

var collectionName = 'collectionName';
var collection = {
    identity: collectionName,
    definition: definition,
    hashKey: 'uid'
};
var collections = {};
collections[collectionName] = collection;

//fake of dynamodb
var dynamodb = {
    putItem: function (params, cb) { cb(new Error("Must be stubbed")); }
};

describe('create', function () {
    var sandbox,
        collectionName = 'collectionName';

    before(function () {
        adapter.setDynamoDb(dynamodb);
        adapter.setCollections(collections);
    });

    beforeEach(function () {
        sandbox = sinon.sandbox.create();
    });

    afterEach(function () {
        sandbox.restore();
    });

    it('empty values and empty definition should create item', function (done) {
        var values = {};
        collection.definition = {};

        sandbox.mock(dynamodb).expects("putItem").once()
            .withArgs({
                TableName: collectionName,
                Item: {}
            })
            .callsArgWithAsync(1, null, []);

        adapter.create(collectionName, values, function (err, data) {
            should.not.exist(err);
            done(err, data);
        })
    });

    it('create object of strings', function (done) {
        var values = {something: "something"};
        collection.definition = {
            simpleString: "string",
            complexString: {
                type: "string"
            }
        };

        sandbox.mock(dynamodb).expects("putItem").once()
            .withArgs({
                Item: {
                    complexString: { S: undefined },
                    simpleString: { S: undefined }
                },
                TableName: "collectionName"
            })
            .callsArgWithAsync(1, null, []);

        adapter.create(collectionName, values, function (err, data) {
            should.not.exist(err);
            done();
        })
    });

    it('create object with required but undefined string is an error', function (done) {
        var values = {something: "something"};
        collection.definition = {
            requiredString: {
                type: "string",
                required: true
            }
        };

        sandbox.mock(dynamodb).expects("putItem").once()
            .withArgs({
                Item: {
                    complexString: { S: undefined },
                    simpleString: { S: undefined }
                },
                TableName: "collectionName"
            })
            .callsArgWithAsync(1, null, []);

        adapter.create(collectionName, values, function (err, data) {
            err.should.contain.instanceof(Error);
            should.not.exist(data);
            done();
        })
    });
});
