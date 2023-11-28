import 'server-only'
import { DynamoDBClient } from "@aws-sdk/client-dynamodb";
import { DynamoDBDocumentClient, GetCommand, PutCommand, QueryCommand, ScanCommand, DeleteCommand, UpdateCommand } from "@aws-sdk/lib-dynamodb";

class DynamoDB {
  private static _instance: DynamoDB;
  private constructor() { }
  public static get Instance() {
    return this._instance || (this._instance = new this());
  }

  public dynamodb;
  public config = (options) => {
    const client = new DynamoDBClient(options);
    this.dynamodb = DynamoDBDocumentClient.from(client);
  }
}

export const dynamobase_config = (options) => DynamoDB.Instance.config(options)

export const get_item = async (params) => {
  const command = new GetCommand({ TableName: params["TableName"], Key: params["query"] });
  const result = await DynamoDB.Instance.dynamodb.send(command);
  return result ? result.Item : null;
};

export const get_items = async (params) => {
  const options = { TableName: params["TableName"] };
  let ExpressionAttributeNames = {};
  let ExpressionAttributeValues = {};

  if ("IndexName" in params) {
    options["IndexName"] = params["IndexName"];
  }

  if ("query" in params) {
    const exp = _KeyConditionExpression(params["query"]);
    options["KeyConditionExpression"] = exp["Expression"].join(" and ");
    ExpressionAttributeNames = Object.assign({}, ExpressionAttributeNames, exp["ExpressionAttributeNames"]);
    ExpressionAttributeValues = Object.assign({}, ExpressionAttributeValues, exp["ExpressionAttributeValues"]);
  }

  if ("filter" in params) {
    const exp = _KeyConditionExpression(params["filter"]);
    options["FilterExpression"] = exp["Expression"].join(" and ");
    ExpressionAttributeNames = Object.assign({}, ExpressionAttributeNames, exp["ExpressionAttributeNames"]);
    ExpressionAttributeValues = Object.assign({}, ExpressionAttributeValues, exp["ExpressionAttributeValues"]);
  }

  if ("ProjectionExpression" in params) {
    const [exp, pro] = _ProjectionAttributeNames(params["ProjectionExpression"])
    options["ProjectionExpression"] = pro;
    ExpressionAttributeNames = Object.assign({}, ExpressionAttributeNames, exp);
  }

  if ("ScanIndexForward" in params) {
    options["ScanIndexForward"] = params["ScanIndexForward"];
  }

  options["Limit"] = "Limit" in params ? params["Limit"] : 1000;

  if (Object.keys(ExpressionAttributeNames).length > 0) {
    options["ExpressionAttributeNames"] = ExpressionAttributeNames;
  }
  if (Object.keys(ExpressionAttributeValues).length > 0) {
    options["ExpressionAttributeValues"] = ExpressionAttributeValues;
  }

  // console.log(options);

  const command = "query" in params ? new QueryCommand(options) : new ScanCommand(options);
  const results = await DynamoDB.Instance.dynamodb.send(command);
  return results;
};

export const get_first = async (params) => {
  params['Limit'] = 1
  const resp = await get_items(params);
  return resp.Items.length ? resp.Items[0] : null;
};

export const put_item = async (params) => {
  const command = new PutCommand({ TableName: params["TableName"], Item: params["Item"] });
  return await DynamoDB.Instance.dynamodb.send(command);
};

export const update_item = async (params) => {
  const exp = _UpdateExpression(params["query"]);
  const options = {
    TableName: params["TableName"],
    Key: params["query"],
    UpdateExpression: `set Color = :color`,
    ExpressionAttributeNames: exp["ExpressionAttributeNames"],
    ExpressionAttributeValues: exp["ExpressionAttributeValues"],
  };
  await DynamoDB.Instance.dynamodb.send(new UpdateCommand(options));
};

export const delete_item = async (params) => {
  const command = new DeleteCommand({ TableName: params["TableName"], Key: params["query"] });
  return await DynamoDB.Instance.dynamodb.send(command);
};

const _KeyConditionExpression = (dct) => {
  const Expression: string[] = [];
  const ExpressionAttributeNames = {};
  const ExpressionAttributeValues = {};

  for (const key in dct) {
    if (!_is_dict(dct[key])) {
      dct[key] = { "=": dct[key] };
    }
    for (const op in dct[key]) {
      const vv = dct[key][op];
      // # expressions
      if (op == "begins_with") Expression.push(`begins_with(#${key}, :${key})`);
      else if (op == "between") Expression.push(`#${key} between :${key}1 and :${key}2`);
      else Expression.push(`#${key} ${op} :${key}`);
      // # keys
      ExpressionAttributeNames["#" + key] = key;
      // # values
      if (op === "between") {
        vv.sort();
        ExpressionAttributeValues[":" + key + "1"] = vv[0];
        ExpressionAttributeValues[":" + key + "2"] = vv[1];
      } else {
        ExpressionAttributeValues[":" + key] = vv;
      }
    }
  }

  return {
    Expression: Expression,
    ExpressionAttributeNames: ExpressionAttributeNames,
    ExpressionAttributeValues: ExpressionAttributeValues,
  };
};

const _UpdateExpression = (dct) => {
  const Expression: string[] = [];
  const ExpressionAttributeNames = {};
  const ExpressionAttributeValues = {};

  for (const k in dct) {
    Expression.push(`#${k} = :${k}`);
    ExpressionAttributeNames["#" + k] = k;
    ExpressionAttributeValues[":" + k] = dct[k];
  }
  return {
    Expression: Expression,
    ExpressionAttributeNames: ExpressionAttributeNames,
    ExpressionAttributeValues: ExpressionAttributeValues,
  };
};

const _ProjectionAttributeNames = (p) => {
  const lst = p.split(',')
  const exp = {}
  const pro: string[] = []
  for (const x of lst) {
    exp['#' + x.trim()] = x.trim()
    pro.push('#' + x.trim())
  }
  return [exp, pro.join(',')]
}

const _is_dict = (v) => typeof v === "object" && v !== null && !(v instanceof Array) && !(v instanceof Date);
