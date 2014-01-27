/*globals describe, it, beforeEach, afterEach */
var mocha = require('mocha'),
    sinon = require('sinon'),
    adapter = require('../.');

var collectionName = 'collectionName';
var collection = {
    identity: collectionName,
    definition: {}
};
var collections = {};
collections[collectionName] = collection;

//fake of dynamodb
var dynamodb = {
    scan: function (params, cb) { cb(new Error("Must be stubbed")); }
};

adapter.setDynamoDb(dynamodb);
adapter.setCollections(collections);

describe('find', function () {
    var sandbox,
        collectionName = 'collectionName';

    beforeEach(function () {
        sandbox = sinon.sandbox.create();
    });

    afterEach(function () {
        sandbox.restore();
    });

    it('empty options should scan table for everything', function (done) {
        var options = {},
            mock = sandbox.mock(dynamodb).expects("scan").once()
            .withArgs({TableName: collectionName})
            .callsArgWithAsync(1, null, []);
        adapter.find(collectionName, options, function (err, data) {
            done(err, data);
        })
    });

    it('options.where.id is set', function (done) {
        var options = {where: {id: 1}},
            mock = sandbox.mock(dynamodb).expects("scan").once()
            .withArgs({ IndexName: 1, TableName: collectionName})
            .callsArgWithAsync(1, null, []);
        adapter.find(collectionName, options, function (err, data) {
            done(err, data);
        })
    });
});



