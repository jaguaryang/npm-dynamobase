# DynamoBase

Query DynamoDB like MongoDB. A lightweight JSON model that simplifies DynamoDB's obscure query operations. It retains all original parameters and supports DynamoDB versions and future upgrades.

# What can be done:

1. Query condition is JSON format
2. No need to consider complex expressions no matter query or update
3. Column names can be any reserved keywords
4. Return the correct int, float, number types

# What can't be done:

<del>Create, modify, and drop DynamoDB tables</del>

# Install

```
npm i dynamobase
```

```
yarn add dynamobase
```

# Configuration

```
import { dynamobase_config } from "dynamobase";
dynamobase_config({ region: "us-east-1" })
```

# Use

## Basic try

```
import { get_first, get_item, get_items } from "dynamobase";
const result = await get_first({
  TableName: "table_name",
  query: { _id: "xxxx-xxxx" },
  ScanIndexForward: false,
});
```

# APIs

All "GET" operations support: = | <= | < | >= | > | begins_with | between

## get_item

### parameters

| Name  | Type | Example     | description                              |
| ----- | ---- | ----------- | ---------------------------------------- |
| query | dict | {"id": 123} | query must be primary key (and sort key) |

### return: Dict or None

## get_items

### parameters: The parameters supported by `get_items` and `get_first` are as follows:

| Name                   | Type                           | description                             |
| ---------------------- | ------------------------------ | --------------------------------------- |
| query                  | dict                           | query can be primary key or GSI columns |
| filter                 | dict                           | any fields and any values               |
| IndexName              | String                         | required if query is GSI or LSI         |
| Select                 | String                         |
| AttributesToGet        | List                           |
| Limit                  | int                            |
| ConsistentRead         | String                         |
| KeyConditions          | dict                           |
| QueryFilter            | dict                           |
| ConditionalOperator    | String                         |
| ScanIndexForward       | boolean                        |
| ExclusiveStartKey      | dict                           |
| ReturnConsumedCapacity | String                         |
| ProjectionExpression   | String                         |
| FilterExpression       | boto3.dynamodb.conditions.Attr |

### return: List<Dict> or None

## get_first

### parameters: same as get_items

### return: Dict or None

## put_item

### parameters

| Name | Type | Example     |
| ---- | ---- | ----------- |
| Item | dict | {"id": 123} |

## update_item

### parameters

| Name  | Type | Example                 | description                              |
| ----- | ---- | ----------------------- | ---------------------------------------- |
| query | dict | {"id": 123}             | query must be primary key (and sort key) |
| Item  | dict | {"field": "some value"} | --                                       |

## delete_item

### parameters

| Name  | Type | Example     | description                              |
| ----- | ---- | ----------- | ---------------------------------------- |
| query | dict | {"id": 123} | query must be primary key (and sort key) |

# DynamoDB docs

[DynamoDB Query Parameters](https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services/dynamodb.html#DynamoDB.Client.query)
