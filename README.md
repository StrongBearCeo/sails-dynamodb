# Sails-DynamoDB

A sails adapter for interacting with Amazon's DynamoDB.

Mostly unit tested (happy path only so far).

Instead of setting a primaryKey, set a hashKey and optionally a rangeKey like so:

    attributes: {

       email: {
         type: 'string',
         email: true,
         index: 'hash'
       },

       geoHash: {
         type: 'string',
         index: 'range'
       }
    }

Will create table in database if it doesn't exist yet.

Does not assume 'id' is a primaryKey, since there is never a good use-case for doing that with DynamoDB.

Doesn't support altering the table in the DB, since DynamoDB doesn't support that.

Does not respect 'unique' property on keys, since that's not supported on DynamoDB either.

## Future Work

Doesn't support updates (PUTs) yet, which would be very useful.

More unit tests to verify reliability.

Better query support even after results returned (we can filter the results).

Allow repeated queries or edits if AWS says there is more information available.
