var AWS = require('aws-sdk'),
    _ = require('underscore');

module.exports = (function () {

    var collections = {};
    var _dbPools = {};

    var dynamodb = new AWS.DynamoDB();

    function getAWSType(type) {
        switch (type) {
            case 'integer':
                return 'N';
            case 'string':
                return 'S';
            case 'datetime':
                return 'N';
            default:
                return 'S';
        }
    }

    //Tell me what environment variables exists
    console.log("AWS environment variables available:")
    _.each(process.env, function (item, key) {
        if (key.indexOf('AWS') > -1) {
            console.log("\t", key, '\t', item);
        }
    });

    //For some reason, AWS is not detecting my env variables.
    AWS.config.update({accessKeyId: process.env.AWS_ACCESS_KEY, secretAccessKey: process.env.AWS_SECRET_ACCESS_KEY});
    AWS.config.update({region: process.env.AWS_REGION});

    /**
     * @param {[]} awsObjs
     * @param {{}} definition
     * @returns {{}}
     * @private
     */
    function condenseItems(awsObjs, definition) {
        return _.map(awsObjs, function (awsObj) {
            return condenseItem(awsObj, definition);
        });
    }

    /**
     * @param {{}} awsObj
     * @param {{}} definition
     * @returns {{}}
     */
    function condenseItem(awsObj, definition) {
        var obj = {};
        _.each(awsObj, function (value, key) {
            var x, defined = definition[key];
            switch (defined && defined.type) {
                case 'string':
                    obj[key] = (value.S || value.N).toString();
                    break;
                case 'integer':
                    obj[key] = parseInt(value.S || value.N);
                    break;
                case 'datetime':
                    if (value.N) {
                        obj[key] = new Date(parseInt(value.N));
                    } else if (value.S) {
                        obj[key] = new Date(value.S)
                    }
                    break;
                case 'boolean':
                    obj[key] = !!value.N || !!value.S;
                    break;
                default:
                    //best effort
                    obj[key] = parseFloat(value.N) || value.S
                    break;
            }
        });
        return obj;
    }

    function expandItem (item) {
        var awsItem = {};
        _.each(item, function (value, key) {
            switch(typeof value) {
                //string
                case 'string': awsItem[key] = {'S': value.toString()}; break;
                //number
                case 'number': awsItem[key] = {'N': value.toString()}; break;
                //datetime
                case 'object':
                    if (value instanceof Date) {
                        awsItem[key] = {'N': value.getTime().toString()};
                    } else {
                        awsItem[key] = {'S': JSON.stringify(value)};
                    }
                    break;
                default:
                    console.error("Cannot handle type of " + value + " (" + (typeof value) + ")");
            }
        });
        return awsItem;
    }

    function find(collection, options, cb) {
        var hashKey = collection.hashKey,
            params = {
                TableName: collection.identity
            };

        // Options object is normalized for you:
        //
        // options.where
        // options.limit
        // options.skip
        // options.sort

        // Filter, paginate, and sort records from the datastore.
        // You should end up w/ an array of objects as a result.
        // If no matches were found, this will be an empty array.

        if (options.where && options.where[hashKey] && !_.isObject(options.where[hashKey])) {
            findOne(params, collection, options, cb);
        } else {
            findMany(params, collection, options, cb);
        }
    }

    function findOne(params, collection, options, cb) {
        var hashKey = collection.hashKey;
        params.Key = {};
        params.Key[hashKey] = {"S": options.where[hashKey].toString()};

        dynamodb.getItem(params, function (err, data) {
            var item;
            if (data && data.Item) {
                item = condenseItem(data.Item, collection.definition);
            }
            //must be array
            cb(err, item && [item]);
        });
    }



    function convertSymbolOpToAwsOp(op) {
        switch(op) {
            case '=': return 'EQ';
            case '<': return 'LT';
            case '<=': return 'LE';
            case '>': return 'GT';
            case '>=': return 'GE';
            default: return op;
        }
    }

    function findMany(params, collection, options, cb) {
        var errors = [],
            definition = collection.definition,
            hashKey = definition.hashKey;

        /**
         * Key Conditions
         *
         * For a query on a table, you can only have conditions on the
         * table primary key attributes.  The hash key must be an EQ
         * condition, and optionally the range attribute.
         *
         * For a query on an index, you can only have conditions on the
         * index key attributes.  The hash key must be an EQ condition,
         * and optionally the range attribute.
         */
        _.each(options.where, function (value, key, list) {
            var keyOp, preKeyOp,
                keyDef = definition[key];
            if (!keyDef) {
                errors.push("Key " + key + " is not defined in schema.");
            } else if (!_.isObject(value)) {
                errors.push("Value " + value + "must be object.");
            } else if ((keyDef.hashKey) && (value.EQ || value['='])) {
                params.KeyConditions = params.KeyConditions || {};
                params.KeyConditions[key] = expandItem({'EQ': (value.EQ || value['='])});
            } else if (keyDef.rangeKey && _.isObject(value) && Object.keys(value).length === 1) {
                preKeyOp = Object.keys(value)[0];
                keyOp = convertSymbolOpToAwsOp(preKeyOp);
                params.KeyConditions = params.KeyConditions || {};
                params.KeyConditions[key] = {};
                params.KeyConditions[key][keyOp] = value[preKeyOp];
                params.KeyConditions[key] = expandItem(params.KeyConditions[key]);
            } else {
                errors.push("Key " + key + " is not an index.")
            }
        });

        if (options.attributesToGet) {
            params.AttributesToGet = options.attributesToGet;
        }

        if (options.limit) {
            params.Limit = options.limit;
        }

        dynamodb.query(params, function (err, data) {
            var items;
            if (data && data.Items) {
                items = data && condenseItems(data.Items, collection.definition);
            }
            cb(err, items);
        });
    }

    function destroy(collection, options, cb) {
        var hashKey = collection.hashKey;
        if (options.where && options.where[hashKey] && !_.isObject(options.where[hashKey])) {
            destroyOne(collection, options, cb);
        } else {
            destroyMany(collection, options, cb);
        }
    }

    /**
     * Destroy a single item by id
     * @param collection
     * @param options
     * @param cb
     */
    function destroyOne(collection, options, cb) {
        var params = {
                TableName: collection.identity,
                Key: {},
                ReturnValues: "ALL_OLD"
            },
            hashKey = collection.hashKey;

        params.Key[hashKey] = {"S": options.where[hashKey]};

        dynamodb.deleteItem(params, function (err, data) {
            cb(err, data);
        });
    }

    function destroyMany(collection, options, cb) {
        find(collection, options, function (err, items) {
            if (err) {
                cb(err, items);
            } else {
                var params = {
                        RequestItems: {}
                    },
                    ops = [];

                params.RequestItems[collection.identity] = ops;

                _.each(items, function (item) {
                    var key = {};
                    key[collection.hashKey] = {"S": item[collection.hashKey]};
                    ops.push({DeleteRequest: {Key: key }});
                });

                batchWriteItems(params, cb);
            }
        });

    }

    function batchWriteItems(params, cb) {
        dynamodb.batchWriteItems(params, function (err, data) {
            if (data) {
                if (data.UnprocessedItems) {
                    params.RequestItems = data.UnprocessedItems;
                    batchWriteItems(params, cb);
                } else {
                    cb(err, data);
                }
            } else {
                cb(err, data);
            }
        });
    }

    /**
     * @param {{}} params
     * @param {string} key
     * @param {{}} value
     * @param {"HASH"|"RANGE"} awsKeyType
     */
    function pushKeyDefinition(params, key, value, awsKeyType) {
        params.AttributeDefinitions.push({
            AttributeName: key,
            AttributeType: getAWSType(value.type)
        });
        params.KeySchema.push({
            AttributeName: key,
            KeyType: awsKeyType
        });
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

        setConfig: function (mock) {

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
         * Creates a new table (since it didn't exist)
         * @param  {string} collectionName
         * @param  {{}} definition
         * @param  {function} cb
         */
        define: function (collectionName, definition, cb) {
            var collection = collections[collectionName],
                config = collection.config || {};
            collection.definition = definition;

            var params = {
                TableName: collectionName,
                AttributeDefinitions: [],
                KeySchema: [],
                ProvisionedThroughput: {
                    ReadCapacityUnits: config.ReadCapacityUnits,
                    WriteCapacityUnits: config.WriteCapacityUnits
                }
            };

            _.each(definition, function (value, key) {
                if (value.hashKey) {
                    definition.hashKey = key;
                    pushKeyDefinition(params, key, value, "HASH");
                } else if (value.rangeKey) {
                    definition.rangeKey = key;
                    pushKeyDefinition(params, key, value, "RANGE");
                }
            });

            if (!definition.hashKey) {
                cb(new Error("Must define hashKey."))
            } else {
                dynamodb.createTable(params, function (err, data) {
                    collections[collectionName].creation = data;
                    cb(err, data);
                });
            }
        },

        /**
         *
         * Ask if table exists, and if so, what is it?
         *
         * @param  {[type]}   collectionName [description]
         * @param  {Function} cb             [description]
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
            var collection = collections[collectionName];

            if (!collection) {
                cb(new Error("Missing collection"));
            }

            if (!collection.definition) {
                cb(new Error("Missing collection.definition"));
            }

            find(collection, options, cb);
        },

        /**
         * Create a single new model (specified by `values`)
         * @param  {string} collectionName
         * @param  {{}} values
         * @param  {Function} cb
         */
        create: function (collectionName, values, cb) {
            var item = {},
                errors = [],
                collection = collections[collectionName],
                definition = collection.definition;

            _.each(definition, function (valueDef, key) {
                var value = values[key] || valueDef.defaultsTo;

                if ((_.isUndefined(value) || _.isNull(value)) && valueDef.required) {
                    errors.push("Missing value for required " + key + " type " + valueDef.type || valueDef);
                } else {
                    switch (valueDef.type || valueDef) {
                        case 'string':
                            item[key] = {'S': value && value.toString()};
                            break;
                        case 'integer':
                            item[key] = {'N': value && value.toString()};
                            break;
                        case 'datetime':
                            item[key] = {'N': value && value.getTime().toString()};
                            break;
                        default:
                            errors.push("Unhandled type " + (valueDef.type || valueDef));
                            break;
                    }
                }
            });

            if (!_.isEmpty(errors)) {
                return cb(new Error(errors));
            }



            var params = {
                TableName: collectionName,
                Item: item
            };
            dynamodb.putItem(params, function (err, data) {
                if (err && err.code == "ConditionalCheckFailedException") {
                    console.log("Found item with that id already.");
                }
                cb(err, data);
            });
        },

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
            destroy(collections[collectionName], options, cb);
        }
    };

})();