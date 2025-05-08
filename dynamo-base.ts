import { BatchGetItemCommand, DescribeTableCommand, DynamoDBClient, type DynamoDBClientConfig } from '@aws-sdk/client-dynamodb';
import { unmarshall } from "@aws-sdk/util-dynamodb";
import {
  DynamoDBDocumentClient,
  GetCommand,
  PutCommand,
  QueryCommand,
  ScanCommand,
  DeleteCommand,
  UpdateCommand,
  type QueryCommandInput,
  type ScanCommandInput,
  type GetCommandInput,
} from '@aws-sdk/lib-dynamodb';

// 定义 put_item 的参数类型
interface PutItemParams {
  TableName: string;
  Item     : Record<string, any>;
}

// 定义 get_item 的参数类型
interface GetItemParams {
  TableName            : string;
  IndexName           ?: string;
  query               ?: Record<string, any>;
  filter              ?: Record<string, any>;
  ProjectionExpression?: string;
  ScanIndexForward    ?: boolean;
  Limit               ?: number;
}

// 定义 get_item 的参数类型
interface BatchItemsParams {
  TableName: string;
  keys     : Record<string, any>[];
}

// 定义 delete_item 的参数类型
interface DeleteItemParams {
  TableName: string;
  query    : Record<string, any>;
}

// 定义 update_item 的参数类型
interface UpdateItemParams {
  TableName: string;
  query    : Record<string, any>;  // 主键对象，对应 Key
  update   : Record<string, any>;  // 要更新的字段
}

// 定义 _KeyConditionExpression 的返回类型
interface KeyConditionExpressionResult {
  Expression               : string;
  ExpressionAttributeNames : Record<string, string>;
  ExpressionAttributeValues: Record<string, any>;
}

// 定义 _UpdateExpression 的返回类型
interface UpdateExpressionResult {
  Expression               : string[];
  ExpressionAttributeNames : Record<string, string>;
  ExpressionAttributeValues: Record<string, any>;
}

// 定义 _ProjectionAttributeNames 的返回类型
interface ProjectionAttributeNamesResult {
  ProjectionExpression    : string;
  ExpressionAttributeNames: Record<string, string>;
}

// 缓存数据库表结构 修改数据库结构后需重启应用
const tableKeyCache = new Map<string, string[]>();

class DynamoDB {
  private dynamodb: DynamoDBDocumentClient;

  constructor(options: DynamoDBClientConfig) {
    const client = new DynamoDBClient(options);
    this.dynamodb = DynamoDBDocumentClient.from(client);
  }

  async get_primary_keys(TableName: string) {
    if (!tableKeyCache.has(TableName)) {
      const describeCommand = new DescribeTableCommand({ TableName });
      const response = await this.dynamodb.send(describeCommand);
      const keySchema = response.Table?.KeySchema;
      if (!keySchema) throw new Error('Cannot find key schema');
      const keys = keySchema.map(k => k.AttributeName) as string[];
      tableKeyCache.set(TableName, keys);
    }
    return tableKeyCache.get(TableName)!;
  }

  async put_item(params: PutItemParams) {
    const keys = await this.get_primary_keys(params.TableName);
    const condition = keys.reduce((acc, x) => ({ ...acc, [x]: { attribute_not_exists: true } }), {});
    const exp = this._KeyConditionExpression(condition);
    const command = new PutCommand({
      TableName: params.TableName,
      Item: params.Item,
      ConditionExpression: exp.Expression,
      ExpressionAttributeNames: exp.ExpressionAttributeNames,
    });
    return await this.dynamodb.send(command);
  }

  async delete_item(params: DeleteItemParams): Promise<void> {
    const command = new DeleteCommand({ TableName: params.TableName, Key: params.query });
    await this.dynamodb.send(command);
  }

  async update_item(params: UpdateItemParams): Promise<void> {
    const exp = this._UpdateExpression(params.update);
    const options = {
      TableName: params.TableName,
      Key: params.query,
      UpdateExpression: `set ${exp.Expression.join(', ')}`,
      ExpressionAttributeNames: exp.ExpressionAttributeNames,
      ExpressionAttributeValues: exp.ExpressionAttributeValues,
    };
    await this.dynamodb.send(new UpdateCommand(options));
  }

  async get_item(params: GetItemParams) {
    const options = this._Options(params);
    const command = new GetCommand(options as GetCommandInput);
    const result = await this.dynamodb.send(command);
    return result?.Item ?? null;
  }

  async batch_items(params: BatchItemsParams) {
    const option = {
      RequestItems: {
        [params.TableName]: {
          Keys: params.keys,
        },
      },
    };
    const command = new BatchGetItemCommand(option);
    const result = await this.dynamodb.send(command);
    const items = (result?.Responses?.[params.TableName]??[]).map(item => unmarshall(item));
    return items;
  }

  async get_first(params: GetItemParams): Promise<Record<string, any> | null> {
    const modifiedParams = { ...params, Limit: 1 };
    const resp = await this.get_items(modifiedParams);
    return resp.Items.length > 0 ? resp.Items[0] : null;
  }

  async get_items(params: GetItemParams): Promise<{ Items: Record<string, any>[] }> {
    const options = this._Options(params);
    const command = 'query' in params && params.query ? new QueryCommand(options as QueryCommandInput) : new ScanCommand(options as ScanCommandInput);
    const results = await this.dynamodb.send(command);
    return results as { Items: Record<string, any>[] };
  }

  private _Options = (params: GetItemParams) => {
    const options: Record<string, any> = { TableName: params.TableName };
    let ExpressionAttributeNames: Record<string, string> = {};
    let ExpressionAttributeValues: Record<string, any> = {};

    if ('IndexName' in params) {
      options.IndexName = params.IndexName;
      if ('query' in params && params.query) {
        // 索引查询 get_items
        const exp = this._KeyConditionExpression(params.query);
        options.KeyConditionExpression = exp.Expression;
        ExpressionAttributeNames = { ...ExpressionAttributeNames, ...exp.ExpressionAttributeNames };
        ExpressionAttributeValues = { ...ExpressionAttributeValues, ...exp.ExpressionAttributeValues };
      } else {
        // 索引扫描 get_items
      }
    } else {
      if ('query' in params && params.query) {
        // 主键查询 get_item
        options.Key = params.query;
      } else {
        // 全表扫描 get_items
      }
    }

    if ('filter' in params && params.filter) {
      const exp = this._KeyConditionExpression(params.filter);
      options.FilterExpression = exp.Expression;
      ExpressionAttributeNames = { ...ExpressionAttributeNames, ...exp.ExpressionAttributeNames };
      ExpressionAttributeValues = { ...ExpressionAttributeValues, ...exp.ExpressionAttributeValues };
    }

    if ('ProjectionExpression' in params && params.ProjectionExpression) {
      const exp = this._ProjectionAttributeNames(params.ProjectionExpression);
      options.ProjectionExpression = exp.ProjectionExpression;
      ExpressionAttributeNames = { ...ExpressionAttributeNames, ...exp.ExpressionAttributeNames };
    }

    if ('ScanIndexForward' in params) {
      options.ScanIndexForward = params.ScanIndexForward;
    }

    if ('Limit' in params) {
      options.Limit = params.Limit;
    }

    if (Object.keys(ExpressionAttributeNames).length > 0) {
      options.ExpressionAttributeNames = ExpressionAttributeNames;
    }
    if (Object.keys(ExpressionAttributeValues).length > 0) {
      options.ExpressionAttributeValues = ExpressionAttributeValues;
    }

    return options;
  };

  // {
  //   a: 3,
  //   b: { attribute_not_exists: true },
  //   c: { begins_with: 'acb' },
  //   d: { between: [1, 10] }
  // }
  private _KeyConditionExpression(dct: Record<string, any>): KeyConditionExpressionResult {
    const Expression: string[] = [];
    const ExpressionAttributeNames: Record<string, string> = {};
    const ExpressionAttributeValues: Record<string, any> = {};
    for (const [key, _value] of Object.entries(dct)) {
      const conditions = this._is_dict(_value) ? _value : { '=': _value };
      for (const [op, value] of Object.entries(conditions)) {
        switch (op) {
          case 'attribute_exists':
            Expression.push(`attribute_exists(#${key})`);
            break;
          case 'attribute_not_exists':
            Expression.push(`attribute_not_exists(#${key})`);
            break;
          case 'begins_with':
            Expression.push(`begins_with(#${key}, :${key})`);
            ExpressionAttributeValues[`:${key}`] = value;
            break;
          case 'between':
            Expression.push(`#${key} between :${key}1 and :${key}2`);
            const [v1, v2] = value[0] < value[1] ? [value[0], value[1]] : [value[1], value[0]];
            ExpressionAttributeValues[`:${key}1`] = v1;
            ExpressionAttributeValues[`:${key}2`] = v2;
            break;
          default:
            Expression.push(`#${key} ${op} :${key}`);
            ExpressionAttributeValues[`:${key}`] = value;
            break;
        }
        ExpressionAttributeNames[`#${key}`] = key;
      }
    }
    return { Expression: Expression.join(' AND '), ExpressionAttributeNames, ExpressionAttributeValues };
  }

  private _UpdateExpression(dct: Record<string, any>): UpdateExpressionResult {
    const Expression: string[] = [];
    const ExpressionAttributeNames: Record<string, string> = {};
    const ExpressionAttributeValues: Record<string, any> = {};

    for (const k in dct) {
      Expression.push(`#${k} = :${k}`);
      ExpressionAttributeNames[`#${k}`] = k;
      ExpressionAttributeValues[`:${k}`] = dct[k];
    }
    return {
      Expression,
      ExpressionAttributeNames,
      ExpressionAttributeValues,
    };
  }

  private _ProjectionAttributeNames(p: string): ProjectionAttributeNamesResult {
    const lst = p
      .split(',')
      .map(x => x.trim())
      .filter(Boolean);
    const ExpressionAttributeNames: Record<string, string> = {};
    const pro: string[] = [];
    for (const x of lst) {
      ExpressionAttributeNames[`#${x}`] = x;
      pro.push(`#${x}`);
    }
    const ProjectionExpression = pro.join(',');
    return {
      ExpressionAttributeNames,
      ProjectionExpression,
    };
  }

  private _is_dict(v: any): v is Record<string, any> {
    return typeof v === 'object' && v !== null && !Array.isArray(v) && !(v instanceof Date);
  }
}

export const db = new DynamoDB({});
