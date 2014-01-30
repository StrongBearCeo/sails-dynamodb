/*globals describe, it, beforeEach, afterEach */
var sinon = require('sinon'),
    adapter = require('../.'),
    expect = require('chai').expect,
    should = require('chai').should();

var collectionName = 'collectionName';
var collection = {
    identity: collectionName,
    hashKey: 'uid'
};
var collections = {};
collections[collectionName] = collection;

//fake of dynamodb
var dynamodb = {
    batchWriteItems: function (params, cb) { cb(new Error("Must be stubbed")); },
    deleteItem: function (params, cb) { cb(new Error("Must be stubbed")); },
    query: function (params, cb) { cb(new Error("Must be stubbed")); }
};



describe('destroy', function () {
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

    it('empty values should delete everything', function (done) {
        var values = {};
        collection.definition = {};

        sandbox.mock(dynamodb).expects("deleteItem").never();
        sandbox.mock(dynamodb).expects("query").once()
            .withArgs({ TableName: collectionName })
            .callsArgWithAsync(1, null, {Items: [{uid: {"S": "1"}}, {uid: {"S": "2"}}]});
        sandbox.mock(dynamodb).expects("batchWriteItems").once()
            .withArgs({
                RequestItems: {
                    collectionName: [
                        { DeleteRequest: { Key: { uid: { S: "1" } } } },
                        { DeleteRequest: { Key: { uid: { S: "2" } } } }]
                }
            })
            .callsArgWithAsync(1, null, [{}, {}, {}]);


        adapter.destroy(collectionName, values, function (err, data) {
            should.not.exist(err);
            should.exist(data);
            done(err, data);
        })
    });

    it('single options.where.id should delete only that id', function (done) {
        var options = { where: {uid: 1}};
        collection.definition = {};

        sandbox.mock(dynamodb).expects("batchWriteItems").never();
        sandbox.mock(dynamodb).expects("query").once()
            .withArgs({ TableName: collectionName })
            .callsArgWithAsync(1, null, {Items: [{uid: {"S": "1"}}, {uid: {"S": "2"}}]});
        sandbox.mock(dynamodb).expects("deleteItem").once()
            .withArgs({ Key: { uid: { S: 1 } }, ReturnValues: "ALL_OLD", TableName: "collectionName" })
            .callsArgWithAsync(1, null, []);


        adapter.destroy(collectionName, options, function (err, data) {
            should.not.exist(err);
            should.exist(data);
            done(err, data);
        })
    });

    it('single options.where.id should error on GT', function (done) {
        var options = { where: {uid: 1}};
        collection.definition = {};

        sandbox.mock(dynamodb).expects("batchWriteItems").never();
        sandbox.mock(dynamodb).expects("query").once()
            .withArgs({ TableName: collectionName })
            .callsArgWithAsync(1, null, {Items: [{uid: {"S": "1"}}, {uid: {"S": "2"}}]});
        sandbox.mock(dynamodb).expects("deleteItem").once()
            .withArgs({ Key: { uid: { S: 1 } }, ReturnValues: "ALL_OLD", TableName: "collectionName" })
            .callsArgWithAsync(1, null, []);


        adapter.destroy(collectionName, options, function (err, data) {
            should.not.exist(err);
            should.exist(data);
            done(err, data);
        })
    });
});
