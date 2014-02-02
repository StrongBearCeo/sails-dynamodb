/*globals describe, it, beforeEach, afterEach */
var sinon = require('sinon'),
    adapter = require('../.');

var collectionName = 'collectionName';
var collection = {
    identity: collectionName,
    definition: {},
    hashKey: 'uid'
};
var collections = {};
collections[collectionName] = collection;

//fake of dynamodb
var dynamodb = {
    scan: function (params, cb) { cb(new Error("Must be stubbed")); },
    query: function (params, cb) { cb(new Error("Must be stubbed")); },
    getItem: function (params, cb) { cb(new Error("Must be stubbed")); }
};



describe('find', function () {
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

    it('empty options should scan table for everything', function (done) {
        var options = {};

        sandbox.mock(dynamodb).expects("scan").once()
            .withArgs({TableName: collectionName})
            .callsArgWithAsync(1, null, []);

        adapter.find(collectionName, options, function (err, data) {
            done(err, data);
        })
    });

    it('where hashKey is set', function (done) {
        var options = {where: {uid: 1}};

        sandbox.mock(dynamodb).expects("query").once()
            .withArgs({
                KeyConditions: { uid: { AttributeValueList: [{ N: "1" }], ComparisonOperator: "EQ" } },
                TableName: "collectionName"
            })
            .callsArgWithAsync(1, null, []);

        adapter.find(collectionName, options, function (err, data) {
            done(err, data);
        })
    });

    it('where hashKey is set by object', function (done) {
        var options = {where: {uid: {'=': 1}}};

        sandbox.mock(dynamodb).expects("query").once()
            .withArgs({
                KeyConditions: { uid: { AttributeValueList: [{ N: "1" }], ComparisonOperator: "EQ" } },
                TableName: "collectionName"
            })
            .callsArgWithAsync(1, null, []);

        adapter.find(collectionName, options, function (err, data) {
            done(err, data);
        })
    });

    it('where hashKey is set by object', function (done) {
        var options = { where: {email: 'test@test.com'} };
        collection.hashKey = "email";

        sandbox.mock(dynamodb).expects("query").once()
            .withArgs({
                KeyConditions: {
                    email: { AttributeValueList: [{ S: "test@test.com" }], ComparisonOperator: "EQ" }
                },
                TableName: "collectionName"
            })
            .callsArgWithAsync(1, null, []);

        adapter.find(collectionName, options, function (err, data) {
            done(err, data);
        })
    });
});



