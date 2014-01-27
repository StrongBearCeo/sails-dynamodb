var AWS = require('aws-sdk'),
    _ = require('underscore');

module.exports = (function() {

    var collections = {};
    var _dbPools = {};

    var dynamodb = new AWS.DynamoDB();

    //Tell me what environment variables exists
    console.log("AWS environment variables available:")
    _.each(process.env, function (item, key) {
        if (key.indexOf('AWS') > -1) {
            console.log("\t", key, '\t', item);
        }
    })

    //For some reason, AWS is not detecting my env variables.
    AWS.config.update({accessKeyId:process.env.AWS_ACCESS_KEY, secretAccessKey:process.env.AWS_SECRET_ACCESS_KEY});
    AWS.config.update({region:process.env.AWS_REGION});


    /**
     * @param {[]} awsObjs
     * @param {{}} definition
     * @returns {{}}
     * @private
     */
    function condenseItems (awsObjs, definition) {
        return _.map(awsObjs, function (awsObj) {
            return condenseItem(awsObj, definition);
        });
    }

    /**
     * @param {{}} awsObj
     * @param {{}} definition
     * @returns {{}}
     */
    function condenseItem (awsObj, definition) {
        var obj = {};
        _.each(awsObj, function (value, key) {
            var x, defined = definition[key];
            switch(defined && defined.type) {
                case 'string':
                    obj[key] = (value.S || value.N).toString(); break;
                case 'integer':
                    obj[key] = parseInt(value.S || value.N); break;
                case 'datetime':
                    if (value.N) {
                        obj[key] = new Date(parseInt(value.N));
                    } else if (value.S) {
                        obj[key] = new Date(value.S)
                    }
                    break;
                case 'boolean':
                    obj[key] = !!value.N || !!value.S; break;
                default:
                    console.error("Unhandled AWS DynamoDB type", key, value);
                    break;
            }
        });
        return obj;
    }

    // Expose adapter definition
    return {
        syncable: true, // to track schema internally

        defaults: {
            schema: true,
            nativeParser: false,
            safe: true,
            ReadCapacityUnits: 100,
            WriteCapacityUnits: 10
        },

        setDynamoDb: function (mock) {
            dynamodb = mock;
        },

        setCollections: function (mock) {
            collections = mock;
        },

        /**
         * Save a collection (table) locally so we can save information about it
         * @param  {{identity:string}} collection
         * @param  {Function} cb callback
         */
        registerCollection: function (collection, cb) {
            collections[collection.identity] = collection;
            cb();
        },

        /**
         * Fired when a model is unregistered, typically when the server
         * is killed. Useful for tearing-down remaining open connections,
         * etc.
         */
        teardown: function (cb) {
            cb(new Error("Not supported on purpose. Use AWS Management Console instead."));
        },


        /**
         *
         * REQUIRED method if integrating with a schemaful
         * (SQL-ish) database.
         *
         * @param  {[type]}   collectionName [description]
         * @param  {[type]}   definition     [description]
         * @param  {Function} cb             [description]
         * @return {[type]}                  [description]
         */
        define: function (collectionName, definition, cb) {
            console.log("define", collectionName, definition, cb);
            //create a new table with the definition

            collections[collectionName].definition = definition;

            function getType (type) {
                switch(type) {
                    case 'integer': return 'N';
                    case 'string': return 'S';
                    case 'datetime': return 'N';
                    default: return 'S';
                }
            }

            var params = {
                TableName: collectionName,
                AttributeDefinitions: [],
                KeySchema: [],
                ProvisionedThroughput: {
                    ReadCapacityUnits: this.config.ReadCapacityUnits,
                    WriteCapacityUnits: this.config.WriteCapacityUnits
                }
            };

            _.each(definition, function (value, key) {
                if (value.primaryKey) {
                    params.AttributeDefinitions.push({
                        AttributeName: key,
                        AttributeType: getType(value.type)
                    });
                    params.KeySchema.push({
                        AttributeName: key,
                        KeyType: "HASH"
                    });
                }
            });
            params.AttributeDefinitions.push({
                AttributeName: "createdAt",
                AttributeType: getType("datetime")
            });
            params.KeySchema.push({
                AttributeName: "createdAt",
                KeyType: "RANGE"
            });

            console.log("params", params);
            dynamodb.createTable(params, function (err, data) {
                collections[collectionName].creation = data;
                console.log("define", err, data);
                cb(err, data);
            });
        },

        /**
         *
         * Ask if table exists, and if so, what is it?
         *
         * @param  {[type]}   collectionName [description]
         * @param  {Function} cb             [description]
         * @return {[type]}                  [description]
         */
        describe: function (collectionName, cb) {
            console.log("describe", collectionName, cb);
            var attributes = collections[collectionName];

            //We should ask if the table exists

            var params = {
                TableName: collectionName
            };

            dynamodb.describeTable(params, function (err, data) {
                if (err && err.code === "ResourceNotFoundException") {
                    //ignore this error, since we're just checking if it exists
                    err = null;
                }
                if (data) {
                    collections[collectionName].serverDescription = data;
                }
                console.log("describe results", err, data);
                cb(err, data);
            });
        },


        /**
         *
         *
         * REQUIRED method if integrating with a schemaful
         * (SQL-ish) database.
         *
         * @param  {[type]}   collectionName [description]
         * @param  {[type]}   relations      [description]
         * @param  {Function} cb             [description]
         * @return {[type]}                  [description]
         */
        drop: function (collectionName, relations, cb) {
            cb(new Error("Not supported on purpose. Use AWS Management Console instead."));
        },


        // OVERRIDES NOT CURRENTLY FULLY SUPPORTED FOR:
        //
        // alter: function (collectionName, changes, cb) {},
        // addAttribute: function(collectionName, attrName, attrDef, cb) {},
        // removeAttribute: function(collectionName, attrName, attrDef, cb) {},
        // alterAttribute: function(collectionName, attrName, attrDef, cb) {},
        // addIndex: function(indexName, options, cb) {},
        // removeIndex: function(indexName, options, cb) {},


        /**
         * Always respond with null or an array.
         * @param  {[type]} collectionName
         * @param  {[type]} options
         * @param  {Function} cb
         */
        find: function (collectionName, options, cb) {
            var params = {
                    TableName: collectionName
                },
                collection = collections[collectionName];

            if (!collection) { cb(new Error("Missing collection")) }
            if (!collection.definition) { cb(new Error("Missing collection.definition")) }

            // Options object is normalized for you:
            //
            // options.where
            // options.limit
            // options.skip
            // options.sort

            // Filter, paginate, and sort records from the datastore.
            // You should end up w/ an array of objects as a result.
            // If no matches were found, this will be an empty array.

            if (options.limit === 1 && options.where.id) {
                params.Key = {"id": {"N": options.where.id}};

                dynamodb.getItem(params, function (err, data) {
                    var item = data && data.Item &&
                        condenseItem(data.Item, collection.definition);
                    console.log("limit 1 found ", err || data);
                    cb(err, item && [item]);
                });
            } else {


                if (options.where) {
                    params.IndexName = options.where.id;
                }

                if (options.limit) {
                    params.Limit = options.limit;
                }

                dynamodb.scan(params, function (err, data) {
                    var items = data && condenseItems(data.Items, collection.definition);
                    cb(err, items);
                });
            }
        },

        /**
         * Create a new object.
         * @param  {string} collectionName
         * @param  {{}} values
         * @param  {Function} cb
         */
        create: function (collectionName, values, cb) {
            console.log('create', collectionName, values, cb);
            // Create a single new model (specified by `values`)
            var item = {},
                collection = collections[collectionName],
                definition = collection.definition;

            item.id = {"N": generateUid().toString()};


            _.each(values, function (value, key) {
                switch(definition[key].type) {
                    case 'string':
                        item[key] = {'S': value.toString()};
                        break;
                    case 'integer':
                        item[key] = {'N': value.toString()};
                        break;
                    case 'datetime':
                        item[key] = {'N': value.getTime().toString()};
                        break;
                    default:
                        console.error("Unhandled type", definition[key].type);
                        break;
                }
            })
            var params = {
                    TableName: collectionName,
                    Item: item,
                    Expected: {
                        id: { Exists: false}
                    }
                };
            dynamodb.putItem(params, function (err, data) {
                if (err && err.code == "ConditionalCheckFailedException") {
                    console.log("Found item with that id already.");
                } else {

                }
                console.log("create", err, data);
                cb(err, data);
            });
        },

        //

        /**
         *
         *
         * REQUIRED method if users expect to call Model.update()
         *
         * @param  {[type]}   collectionName [description]
         * @param  {[type]}   options        [description]
         * @param  {[type]}   values         [description]
         * @param  {Function} cb             [description]
         */
        update: function (collectionName, options, values, cb) {

            // If you need to access your private data for this collection:
            var collection = collections[collectionName];

            // 1. Filter, paginate, and sort records from the datastore.
            //    You should end up w/ an array of objects as a result.
            //    If no matches were found, this will be an empty array.
            //
            // 2. Update all result records with `values`.
            //
            // (do both in a single query if you can-- it's faster)

            cb(new Error("Not supported yet.  Will be."));
        },

        /**
         *
         * REQUIRED method if users expect to call Model.destroy()
         *
         * @param  {[type]}   collectionName [description]
         * @param  {[type]}   options        [description]
         * @param  {Function} cb             [description]
         */
        destroy: function (collectionName, options, cb) {
            cb(new Error("Not supported yet.  Will be."));
        }
    };

})();