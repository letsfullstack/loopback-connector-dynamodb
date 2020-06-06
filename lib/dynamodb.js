/* eslint-disable import/order */
/* eslint-disable prefer-arrow-callback */
/* eslint-disable func-names */
/* eslint-disable no-extra-bind */

const g = require('strong-globalize')();
const { Connector } = require('loopback-connector');

// It doesn't include sharding or auto partitioning of items above 400kb

/**
 * Module dependencies
 */
const AWS = require('aws-sdk');

const DocClient = AWS.DynamoDB.DocumentClient;
const colors = require('colors');
const helper = require('./helper.js');
const async = require('async');
const { EventEmitter } = require('events');
const winston = require('winston');
const util = require('util');

// Winston logger configuration
const logger = new (winston.Logger)({
  transports: [
    new (winston.transports.Console)({
      colorize: true,
    }),
  ],
});

function countProperties(obj) {
  return Object.keys(obj).length;
}

/**
 * The constructor for MongoDB connector
 * @param {Object} settings The settings object
 * @param {DataSource} dataSource The data source instance
 * @constructor
 */
function DynamoDB(s, dataSource) {
  if (!AWS) {
    throw new Error('AWS SDK not installed. Please run npm install aws-sdk');
  }

  this.name = 'dynamodb';
  this._models = {};
  this._tables = {};
  this._attributeSpecs = [];
  // Connect to dynamodb server
  let dynamodb;
  // Try to read accessKeyId and secretAccessKey from environment constiables
  if ((process.env.AWS_ACCESS_KEY_ID !== undefined)
  && (process.env.AWS_SECRET_ACCESS_KEY !== undefined)) {
    logger.log('debug', 'Credentials selected from environment constiables');
    AWS.config.update({
      region: s.region,
      maxRetries: s.maxRetries,
    });
    dynamodb = new AWS.DynamoDB();
  } else {
    logger.log('warn', 'Credentials not found in environment constiables');
    try {
      AWS.config.loadFromPath('credentials.json');
      logger.log('info', 'Loading credentials from file');
      dynamodb = new AWS.DynamoDB();
    } catch (e) {
      logger.log('warn', 'Cannot find credentials file');
      logger.log('info', 'Using settings from schema');
      AWS.config.update({
        accessKeyId: s.accessKeyId,
        secretAccessKey: s.secretAccessKey,
        region: s.region,
        maxRetries: s.maxRetries,
      });
      dynamodb = new AWS.DynamoDB({
        endpoint: new AWS.Endpoint(`http://${s.host}:${s.port}`),
      });
    }
  }

  this.client = dynamodb; // Used by instance methods
  this.docClient = new DocClient({
    service: dynamodb,
  });

  this.emitter = new EventEmitter();
}

util.inherits(DynamoDB, Connector);


/**
 * Initialize the Cloudant connector for the given data source
 *
 * @param {DataSource} ds The data source instance
 * @param {Function} [cb] The cb function
 */
exports.initialize = function initializeSchema(ds, cb) {
  // s stores the ds settings
  const s = ds.settings;
  if (ds.settings) {
    s.host = ds.settings.host || 'localhost';
    s.port = ds.settings.port || 8000;
    s.region = ds.settings.region || 'ap-southeast-1';
    s.accessKeyId = ds.settings.accessKeyId || 'fake';
    s.secretAccessKey = ds.settings.secretAccessKey || 'fake';
    s.maxRetries = ds.settings.maxRetries || 0;
  } else {
    s.region = 'ap-southeast-1';
  }
  logger.transports.console.level = ds.settings.logLevel || 'debug';
  logger.info('Initializing dynamodb adapter');
  ds.connector = new DynamoDB(s, ds);
  ds.connector.dataSource = ds;

  if (cb) {
    cb();
  }
};


/*
  Assign Attribute Definitions
  and KeySchema based on the keys
*/
function AssignKeys(name, type, settings) {
  const attr = {};
  attr.keyType = name.keyType;

  const tempString = (name.type).toString();
  let aType = tempString.match(/\w+(?=\(\))/)[0];

  aType = aType.toLowerCase();
  attr.attributeType = helper.TypeLookup(aType);
  return attr;
}

/**
  Record current time in milliseconds
*/
function startTimer() {
  return new Date().getTime();
}
/**
  Given start time, return a string containing time difference in ms
*/
function stopTimer(timeStart) {
  return `[${String(new Date().getTime() - timeStart)} ms]`;
}
/**
 * Create a table based on hashkey, rangekey specifications
 * @param  {object} dynamodb        : adapter
 * @param  {object} tableParams     : KeySchema & other attrs
 * @param {Boolean} tableStatusWait : If true, wait for table to become active
 * @param {Number} timeInterval     : Check table status after `timeInterval` milliseconds
 * @param {function} callback       : Callback function
 */
function createTable(dynamodb, tableParams, tableStatusWait, timeInterval, callback) {
  let tableExists = false;
  let tableStatusFlag = false;
  dynamodb.listTables((err, data) => {
    if (err || !data) {
      logger.log('error', '-------Error while fetching tables from server. Please check your connection settings & AWS config--------');
      callback(err, null);
    } else {
      // Boolean constiable to check if table already exists.
      const existingTableNames = data.TableNames;
      existingTableNames.forEach((existingTableName) => {
        if (tableParams.TableName === existingTableName) {
          tableExists = true;
          logger.log('info', 'TABLE %s FOUND IN DATABASE', existingTableName);
        }
      });
      // If table exists do not create new table
      if (tableExists === false) {
        // DynamoDB will throw error saying table does not exist
        logger.log('info', 'CREATING TABLE: %s IN DYNAMODB', tableParams.TableName);
        dynamodb.createTable(tableParams, function (err1, data1) {
          if (err1 || !data1) {
            callback(err1, null);
          } else {
            logger.log('info', 'TABLE CREATED');
            if (tableStatusWait) {
              async.whilst(function () {
                return !tableStatusFlag;
              }, function (innerCallback) {
                logger.log('info', 'Checking Table Status');
                dynamodb.describeTable({
                  TableName: tableParams.TableName,
                }, (err2, tableData) => {
                  if (err2) {
                    innerCallback(err2);
                  } else if (tableData.Table.TableStatus === 'ACTIVE') {
                    logger.log('info', 'Table Status is `ACTIVE`');
                    tableStatusFlag = true;
                    innerCallback(null);
                  } else {
                    setTimeout(innerCallback, timeInterval);
                  }
                });
              }, function (err3) {
                if (err3) {
                  callback(err3, null);
                } else {
                  callback(null, 'active');
                }
              }.bind(this));
            }
          } // successful response
        }.bind(this));
      } else {
        callback(null, 'done');
      }
    }
  });
}


// Check if object is empty
function isEmpty(obj) {
  const { hasOwnProperty } = Object.prototype;
  // null and undefined are 'empty'
  if (obj === null) return true;

  // Assume if it has a length property with a non-zero value
  // that that property is correct.
  if (obj.length > 0) return false;
  if (obj.length === 0) return true;

  // Otherwise, does it have any properties of its own?
  // Note that this doesn't handle
  // toString and valueOf enumeration bugs in IE < 9
  Object.keys(obj).forEach((key) => {
    if (hasOwnProperty.call(obj, key)) return false;
    return true;
  });

  return true;
}


/**
 * Define schema and create table with hash and range keys
 * @param  {object} descr : description specified in the schema
 */
DynamoDB.prototype.define = function (descr) {
  const timeStart = startTimer();
  if (!descr.settings) descr.settings = {};
  const { modelName } = descr.model;
  const { emitter } = this;
  this._models[modelName] = descr;
  // Set Read & Write Capacity Units
  this._models[modelName].ReadCapacityUnits = descr.settings.ReadCapacityUnits || 5;
  this._models[modelName].WriteCapacityUnits = descr.settings.WriteCapacityUnits || 10;

  this._models[modelName].localIndexes = {};
  this._models[modelName].globalIndexes = {};

  let timeInterval;
  let tableStatusWait;
  // Wait for table to become active?
  if (descr.settings.tableStatus) {
    tableStatusWait = descr.settings.tableStatus.waitTillActive;
    if (tableStatusWait === undefined) {
      tableStatusWait = true;
    }
    timeInterval = descr.settings.tableStatus.timeInterval || 5000;
  } else {
    tableStatusWait = true;
    timeInterval = 5000;
  }

  // Create table now with the hash and range index.
  const { properties } = descr;
  // Iterate through properties and find index
  const tableParams = {};
  tableParams.AttributeDefinitions = [];
  tableParams.KeySchema = [];
  const LocalSecondaryIndexes = [];
  const GlobalSecondaryIndexes = [];
  this._attributeSpecs[modelName] = {};
  // Temporary object to store read and write capacity units for breakable attrs
  const rcus = {};
  const wcus = {};

  /*
    Build KeySchema for the table based on schema definitions.
    */
  Object.keys(properties).forEach((key) => {
    // Assign breakers, limits or whatever other properties
    // are specified first
    // Store the type of attributes in _attributeSpecs. This is
    // quite helpful to do Date & Boolean conversions later
    // on.
    const tempString = (properties[key].type).toString();
    let aType = tempString.match(/\w+(?=\(\))/)[0];
    aType = aType.toLowerCase();
    this._attributeSpecs[modelName][key] = aType;

    // Check if UUID is set to be true for HASH KEY attribute
    if (properties[key].keyType === 'hash') {
      if (properties[key].uuid === true) {
        if (key !== 'id') {
          throw new Error('UUID generation is only allowed for attribute name id');
        } else {
          this._models[modelName].hashKeyUUID = true;
          logger.log('debug', 'Hash key UUID generation: TRUE');
        }
      } else {
        this._models[modelName].hashKeyUUID = false;
      }
    }
    // Following code is applicable only for keys
    if (properties[key].keyType !== undefined) {
      const attrs = AssignKeys(properties[key]);
      // The keys have come! Add to tableParams
      // Add Attribute Definitions
      // HASH primary key?
      if (attrs.keyType === 'hash') {
        this._models[modelName].hashKey = key;
        logger.log('debug', 'HASH KEY:', key);
        tableParams.KeySchema.push({
          AttributeName: key,
          KeyType: 'HASH',
        });
        tableParams.AttributeDefinitions.push({
          AttributeName: key,
          AttributeType: attrs.attributeType,
        });
      }
      // Range primary key?
      if (attrs.keyType === 'range') {
        this._models[modelName].rangeKey = key;
        logger.log('debug', 'RANGE KEY:', key);
        tableParams.KeySchema.push({
          AttributeName: key,
          KeyType: 'RANGE',
        });
        tableParams.AttributeDefinitions.push({
          AttributeName: key,
          AttributeType: attrs.attributeType,
        });
      }
      // Composite virtual primary key?
      if (attrs.keyType === 'pk') {
        this._models[modelName].pKey = key;
        this._models[modelName].pkSeparator = properties[key].separator || '--x--';
      }
    }

    if (properties[key].index !== undefined) {
      if (properties[key].index.local !== undefined) {
        const attrs = AssignKeys(properties[key]);
        const index = properties[key].index.local;
        const keyName = `${key}LocalIndex`;
        const localIndex = {
          IndexName: keyName,
          KeySchema: [{
            AttributeName: this._models[modelName].hashKey,
            KeyType: 'HASH',
          }, {
            AttributeName: key,
            KeyType: 'RANGE',
          }],
          Projection: {},
        };

        if (index.project) {
          if (util.isArray(index.project)) {
            localIndex.Projection = {
              ProjectionType: 'INCLUDE',
              NonKeyAttributes: index.project,
            };
          } else {
            localIndex.Projection = {
              ProjectionType: 'ALL',
            };
          }
        } else {
          localIndex.Projection = {
            ProjectionType: 'KEYS_ONLY',
          };
        }

        LocalSecondaryIndexes.push(localIndex);
        tableParams.AttributeDefinitions.push({
          AttributeName: key,
          AttributeType: attrs.attributeType,
        });
        this._models[modelName].localIndexes[key] = {
          hash: this._models[modelName].hashKey,
          range: key,
          IndexName: keyName,
        };
      }
      if (properties[key].index.global !== undefined) {
        const attrs = AssignKeys(properties[key]);
        const index = properties[key].index.global;
        const keyName = `${key}GlobalIndex`;
        const globalIndex = {
          IndexName: keyName,
          KeySchema: [
            {
              AttributeName: key,
              KeyType: 'HASH',
            },
            {
              AttributeName: index.rangeKey,
              KeyType: 'RANGE',
            },
          ],
          ProvisionedThroughput: {
            ReadCapacityUnits: index.throughput.read || 5,
            WriteCapacityUnits: index.throughput.write || 10,
          },
        };

        if (index.project) {
          if (util.isArray(index.project)) {
            globalIndex.Projection = {
              ProjectionType: 'INCLUDE',
              NonKeyAttributes: index.project,
            };
          } else {
            globalIndex.Projection = {
              ProjectionType: 'ALL',
            };
          }
        } else {
          globalIndex.Projection = {
            ProjectionType: 'KEYS_ONLY',
          };
        }
        GlobalSecondaryIndexes.push(globalIndex);
        tableParams.AttributeDefinitions.push({
          AttributeName: key,
          AttributeType: attrs.attributeType,
        });
        this._models[modelName].globalIndexes[key] = {
          hash: key,
          range: index.rangeKey,
          IndexName: keyName,
        };
      }
    }
  });

  if (LocalSecondaryIndexes.length) {
    tableParams.LocalSecondaryIndexes = LocalSecondaryIndexes;
  }
  if (GlobalSecondaryIndexes.length) {
    tableParams.GlobalSecondaryIndexes = GlobalSecondaryIndexes;
  }

  tableParams.ProvisionedThroughput = {
    ReadCapacityUnits: this._models[modelName].ReadCapacityUnits,
    WriteCapacityUnits: this._models[modelName].WriteCapacityUnits,
  };
  logger.log('debug', 'Read Capacity Units:', tableParams.ProvisionedThroughput.ReadCapacityUnits);
  logger.log('debug', 'Write Capacity Units:', tableParams.ProvisionedThroughput.WriteCapacityUnits);

  if ((this._models[modelName].rangeKey !== undefined)
    && (this._models[modelName].pKey !== undefined)) {
    if (this._models[modelName].pKey !== 'id') {
      throw new Error('Primary Key must be named `id`');
    }
  }
  if ((this._models[modelName].rangeKey !== undefined)
  && (this._models[modelName].pKey === undefined)) {
    throw new Error('Range key is present, but primary key not specified in schema');
  }

  /*
    JugglingDB expects an id attribute in return even if a hash key is not specified. Hence
    if hash key is not defined in the schema, create an attribute called id, set it as hashkey.
    */
  if ((this._models[modelName].hashKey === undefined) && (properties.id === undefined)) {
    this._models[modelName].hashKey = 'id';
    this._models[modelName].hashKeyUUID = true;
    this._attributeSpecs[modelName][this._models[modelName].hashKey] = 'string';
    tableParams.KeySchema.push({
      AttributeName: 'id',
      KeyType: 'HASH',
    });
    tableParams.AttributeDefinitions.push({
      AttributeName: 'id',
      AttributeType: 'S',
    });
  }

  // If there are breakable attrs with sharding set to true, create the
  // extra tables now
  const _dynamodb = this.client;
  const attributeSpecs = this._attributeSpecs[modelName];
  const { ReadCapacityUnits } = this._models[modelName];
  const { WriteCapacityUnits } = this._models[modelName];
  const { hashKey } = this._models[modelName];
  const { pKey } = this._models[modelName];

  // Assign table name
  tableParams.TableName = descr.settings.table || modelName;
  logger.log('debug', 'Table Name:', tableParams.TableName);
  // Add this to _tables so that instance methods can use it.
  this._tables[modelName] = tableParams.TableName;
  // Create main table function
  createTable(_dynamodb, tableParams, tableStatusWait, timeInterval, (err, data) => {
    if (err || !data) {
      const tempString = `while creating table: ${tableParams.TableName} => ${err.message.toString()}`;
      throw new Error(tempString);
    }
  });
  logger.log('info', 'Defining model: ', modelName, stopTimer(timeStart).bold.cyan);
};

/**
 * Creates a DynamoDB compatible representation
 * of arrays, objects and primitives.
 * @param {object} data: Object to be converted
 * @return {object} DynamoDB compatible JSON
 */
function DynamoFromJSON(data) {
  let obj;
  if (data instanceof Array) {
    /*
    If data is an array, loop through each member
    of the array, and call objToDB on the element
    e.g ['someword',20] --> [ {'S': 'someword'} , {'N' : '20'}]
    */
    obj = [];
    data.forEach((dataElement) => {
      // If string is empty, assign it as
      // 'null'.
      if (dataElement === '') {
        dataElement = 'empty';
      }
      if (dataElement instanceof Date) {
        dataElement = Number(dataElement);
      }
      obj.push(helper.objToDB(dataElement));
    });
  } else if ((data instanceof Object) && (data instanceof Date !== true)) {
    /*
    If data is an object, loop through each member
    of the object, and call objToDB on the element
    e.g { age: 20 } --> { age: {'N' : '20'} }
    */
    obj = {};
    Object.keys(data).forEach((key) => {
      if (data.hasOwnProperty(key)) {
        // If string is empty, assign it as
        // 'null'.
        if (data[key] === undefined) {
          data[key] = 'undefined';
        }
        if (data[key] === null) {
          data[key] = 'null';
        }
        if (data[key] === '') {
          data[key] = 'empty';
        }
        // If Date convert to number
        if (data[key] instanceof Date) {
          data[key] = Number(data[key]);
        }
        obj[key] = helper.objToDB(data[key]);
      }
    });
  /*
  If data is a number, or string call objToDB on the element
  e.g 20 --> {'N' : '20'}
  */
  } else {
    // If string is empty, assign it as
    // 'empty'.
    if (data === null) {
      data = 'null';
    }
    if (data === undefined) {
      data = 'undefined';
    }
    if (data === '') {
      data = 'empty';
    }
    // If Date convert to number
    if (data instanceof Date) {
      data = Number(data);
    }
    obj = helper.objToDB(data);
  }
  return obj;
}

function KeyOperatorLookup(operator) {
  let value;
  switch (operator) {
    case '=':
      value = '=';
      break;
    case 'lt':
      value = '<';
      break;
    case 'lte':
      value = '<=';
      break;
    case 'gt':
      value = '>';
      break;
    case 'gte':
      value = '>=';
      break;
    case 'between':
      value = 'BETWEEN';
      break;
    default:
      value = '=';
      break;
  }
  return value;
}

/**
 * Converts jugglingdb operators like 'gt' to DynamoDB form 'GT'
 * @param {string} DynamoDB comparison operator
 */
function OperatorLookup(operator) {
  if (operator === 'inq') {
    operator = 'in';
  }
  return operator.toUpperCase();
}

DynamoDB.prototype.defineProperty = (model, prop, params) => {
  this._models[model].properties[prop] = params;
};

DynamoDB.prototype.tables = (name) => {
  if (!this._tables[name]) {
    this._tables[name] = name;
  }
  return this._tables[name];
};
/**
 * Create a new item or replace/update it if it exists
 * @param  {object}   model
 * @param  {object}   data   : key,value pairs of new model object
 * @param  {Function} callback
 */
DynamoDB.prototype.create = (model, data, callback) => {
  const timerStart = startTimer();
  const { hashKey } = this._models[model];
  const { rangeKey } = this._models[model];
  const { pkSeparator } = this._models[model];
  const { pKey } = this._models[model];
  let err;
  // If jugglingdb defined id is undefined, and it is not a
  // hashKey or a primary key , then delete it.
  if ((data.id === undefined) && (hashKey !== 'id')) {
    delete data.id;
  }
  // If some key is a hashKey, check if uuid is set to true. If yes, call the
  // UUID() function and generate a unique id.
  if (this._models[model].hashKeyUUID === true) {
    data[hashKey] = helper.UUID();
  }
  const originalData = {};
  // Copy all attrs from data to originalData
  Object.keys(data).forEach((key) => {
    originalData[key] = data[key];
  });

  if (data[hashKey] === undefined) {
    err = new Error(`Hash Key '${hashKey}' is undefined.`);
    callback(err, null);
    return;
  }
  if (data[hashKey] === null) {
    err = new Error(`Hash Key '${hashKey}' cannot be NULL.`);
    callback(err, null);
    return;
  }
  // If pKey is defined, range key is also present.
  if (pKey !== undefined) {
    if ((data[rangeKey] === null) || (data[rangeKey] === undefined)) {
      err = new Error(`Range Key '${rangeKey}' cannot be null or undefined.`);
      callback(err, null);
      return;
    }
    data[pKey] = String(data[hashKey]) + pkSeparator + String(data[rangeKey]);
    originalData[pKey] = data[pKey];
  }

  const queryString = `CREATE ITEM IN TABLE: ${this.tables(model)}`;
  const tableParams = {};
  tableParams.TableName = this.tables(model);
  tableParams.ReturnConsumedCapacity = 'TOTAL';

  const attributeSpecs = this._attributeSpecs[model];
  const outerCounter = 0;
  const chunkedData = {};

  const tempString = `INSERT ITEM INTO TABLE: ${tableParams.TableName}`;
  logger.log('debug', tempString);
  // if (pKey !== undefined) {
  //   delete data[pKey];
  // }
  tableParams.Item = data;
  this.docClient.put(tableParams, function (err1, res) {
    if (err1 || !res) {
      callback(err, null);
    } else {
      logger.log('info', queryString.blue, stopTimer(timerStart).bold.cyan);
      if (pKey !== undefined) {
        originalData.id = originalData[pKey];
        callback(null, originalData.id);
      } else {
        originalData.id = originalData[hashKey];
        callback(null, originalData.id);
      }
    }
  }.bind(this));
};
/**
 * Function that performs query operation on dynamodb
 * @param  {object} model
 * @param  {object} filter             : Query filter
 * @param  {Number/String} hashKey     : Hash Key
 * @param  {object} rangeKey           : Range Key
 * @param  {String} queryString        : The query string (used for console logs)
 * @param  {Number} timeStart          : Start time of query operation in milliseconds
 * @return {object}                    : Final query object to be sent to dynamodb
 */
function query(modelName, filter, model, queryString, timeStart) {
  // Table parameters to do the query/scan
  const {
    hashKey, rangeKey, localKeys, globalKeys,
  } = model;

  const tableParams = {};
  // Define the filter if it does not exist
  if (!filter) {
    filter = {};
  }
  // Initialize query as an empty object
  const queryObj = {};
  // Construct query for amazon DynamoDB
  // Set queryfileter to empty object
  tableParams.ExpressionAttributeNames = {};
  const ExpressionAttributeNames = {};
  tableParams.ExpressionAttributeValues = {};
  tableParams.KeyConditionExpression = '';

  const KeyConditionExpression = [];
  const FilterExpression = [];
  const ExpressionAttributeValues = {};

  // If a where clause exists in the query, extract
  // the conditions from it.
  if (filter.where) {
    queryString += ' WHERE ';
    Object.keys(filter.where).forEach((key) => {
      let condition = filter.where[key];

      let keyName = `#${key.slice(0, 1).toUpperCase()}`;
      if (tableParams.ExpressionAttributeNames[keyName] === undefined) {
        tableParams.ExpressionAttributeNames[keyName] = key;
      } else {
        let i = 1;
        while (tableParams.ExpressionAttributeNames[keyName] !== undefined) {
          keyName = `#${key.slice(0, i)}`;
          i += 1;
        }
        keyName = `#${key.slice(0, i).toUpperCase()}`;
        tableParams.ExpressionAttributeNames[keyName] = key;
      }

      ExpressionAttributeNames[key] = keyName;

      const ValueExpression = `:${key}`;
      let insideKey = null;

      if (key === hashKey || (globalKeys[key] && globalKeys[key].hash === key)
        || (localKeys[key] && localKeys[key].hash === key)) {
        if (condition && condition.constructor.name === 'Object') {
          // do nothing
        } else if (condition && condition.constructor.name === 'Array') {
          // do nothing
        } else {
          KeyConditionExpression[0] = `${keyName} = ${ValueExpression}`;
          tableParams.ExpressionAttributeValues[ValueExpression] = condition;
          if (globalKeys[key] && globalKeys[key].hash === key) {
            tableParams.IndexName = globalKeys[key].IndexName;
          } else if (localKeys[key] && localKeys[key].hash === key) {
            tableParams.IndexName = localKeys[key].IndexName;
          }
        }
      } else if (key === rangeKey || (globalKeys[key] && globalKeys[key].range === key)
        || (localKeys[key] && localKeys[key].range === key)) {
        if (condition && condition.constructor.name === 'Object') {
          [insideKey] = Object.keys(condition);
          condition = condition[insideKey];
          const operator = KeyOperatorLookup(insideKey);
          if (operator === 'BETWEEN') {
            [
              tableParams.ExpressionAttributeValues[`${ValueExpression}_start`],
              tableParams.ExpressionAttributeValues[`${ValueExpression}_end`],
            ] = condition;
            KeyConditionExpression[1] = `${keyName} ${operator} ${ValueExpression}_start AND ${ValueExpression}_end`;
          } else {
            tableParams.ExpressionAttributeValues[ValueExpression] = condition;
            KeyConditionExpression[1] = `${keyName} ${operator} ${ValueExpression}`;
          }
        } else if (condition && condition.constructor.name === 'Array') {
          [
            tableParams.ExpressionAttributeValues[`${ValueExpression}_start`],
            tableParams.ExpressionAttributeValues[`${ValueExpression}_end`],
          ] = condition;
          KeyConditionExpression[1] = `${keyName} BETWEEN ${ValueExpression}_start AND ${ValueExpression}_end`;
        } else {
          tableParams.ExpressionAttributeValues[ValueExpression] = condition;
          KeyConditionExpression[1] = `${keyName} = ${ValueExpression}`;
        }

        if (globalKeys[key] && globalKeys[key].range === key) {
          tableParams.IndexName = globalKeys[key].IndexName;
        } else if (localKeys[key] && localKeys[key].range === key) {
          tableParams.IndexName = localKeys[key].IndexName;
        }
      } else if (condition && condition.constructor.name === 'Object') {
        [insideKey] = Object.keys(condition);
        condition = condition[insideKey];
        const operator = KeyOperatorLookup(insideKey);
        if (operator === 'BETWEEN') {
          [
            tableParams.ExpressionAttributeValues[`${ValueExpression}_start`],
            tableParams.ExpressionAttributeValues[`${ValueExpression}_end`],
          ] = condition;
          FilterExpression.push(`${keyName} ${operator} ${ValueExpression}_start AND ${ValueExpression}_end`);
        } else if (operator === 'IN') {
          tableParams.ExpressionAttributeValues[ValueExpression] = `(${condition.join(',')})`;
          FilterExpression.push(`${keyName} ${operator} ${ValueExpression}`);
        } else {
          tableParams.ExpressionAttributeValues[ValueExpression] = condition;
          FilterExpression.push(`${keyName} ${operator} ${ValueExpression}`);
        }
      } else if (condition && condition.constructor.name === 'Array') {
        tableParams.ExpressionAttributeValues[ValueExpression] = `(${condition.join(',')})`;
        FilterExpression.push(`${keyName} IN ${ValueExpression}`);
      } else {
        tableParams.ExpressionAttributeValues[ValueExpression] = condition;
        FilterExpression.push(`${keyName} = ${ValueExpression}`);
      }

      // If condition is of type object, obtain key
      // and the actual condition on the key
      // In jugglingdb, `where` can have the following
      // forms.
      // 1) where : { key: value }
      // 2) where : { startTime : { gt : Date.now() } }
      // 3) where : { someKey : ['something','nothing'] }
      // condition now holds value in case 1),
      //  { gt: Date.now() } in case 2)
      // ['something, 'nothing'] in case 3)
      /*
        If key is of hash or hash & range type,
        we can use the query function of dynamodb
        to access the table. This saves a lot of time
        since it does not have to look at all records
      */
      // const insideKey = null;
      // if (condition && condition.constructor.name === 'Object') {
      //   insideKey = Object.keys(condition)[0];
      //   condition = condition[insideKey];

      //   logger.log('debug','Condition Type => Object', 'Operator',
      //              insideKey, 'Condition Value:', condition);
      //   // insideKey now holds gt and condition now holds Date.now()
      //   queryObj[key] = {
      //     operator: OperatorLookup(insideKey),
      //     attrs: condition
      //   };
      // } else if (condition && condition.constructor.name === 'Array') {
      //   logger.log('debug', 'Condition Type => Array', 'Opearator',
      //              'IN', 'Condition Value:', condition);
      //   queryObj[key] = {
      //     operator: 'IN',
      //     attrs: condition
      //   };
      // } else {
      //   logger.log('debug', 'Condition Type => Equality', 'Condition Value:', condition);
      //   queryObj[key] = {
      //     operator: 'EQ',
      //     attrs: condition
      //   };
      // }

      // if (key === hashKey) {
      //   // Add hashkey eq condition to keyconditions
      //   tableParams.KeyConditions[key] = {};
      //   tableParams.KeyConditions[key].ComparisonOperator = queryObj[key].operator;
      //   // For hashKey only 'EQ' operator is allowed. Issue yellow error. DB will
      //   // throw a red error.
      //   if (queryObj[key].operator !== 'EQ') {
      //     const errString = 'Warning: Only equality condition is allowed on HASHKEY';
      //     logger.log('warn', errString.yellow);
      //   }
      //   tableParams.KeyConditions[key].AttributeValueList = [];
      //   tableParams.KeyConditions[key].AttributeValueList.push(queryObj[key].attrs);
      //   //tableParams.KeyConditions[key].AttributeValueList.push(
      //                                DynamoFromJSON(queryObj[key].attrs));
      //   queryString = queryString + ' HASHKEY: `' + String(key) + '` '
      //       + String(queryObj[key].operator) + ' `' + String(queryObj[key].attrs) + '`';
      // } else if (key === rangeKey) {
      //   // Add hashkey eq condition to keyconditions
      //   tableParams.KeyConditions[key] = {};
      //   tableParams.KeyConditions[key].ComparisonOperator = queryObj[key].operator;
      //   tableParams.KeyConditions[key].AttributeValueList = [];

      //   const attrResult = queryObj[key].attrs;
      //   //const attrResult = DynamoFromJSON(queryObj[key].attrs);
      //   if (attrResult instanceof Array) {
      //     logger.log('debug', 'Attribute Value list is an array');
      //     tableParams.KeyConditions[key].AttributeValueList = queryObj[key].attrs;
      //     // tableParams.KeyConditions[key].AttributeValueList =
      //       DynamoFromJSON(queryObj[key].attrs);
      //   } else {
      //     tableParams.KeyConditions[key].AttributeValueList.push(queryObj[key].attrs);
      //     //tableParams.KeyConditions[key].AttributeValueList.push(
      //        DynamoFromJSON(queryObj[key].attrs));
      //   }

      //   queryString = queryString + '& RANGEKEY: `' + String(key) + '` ' +
      //      String(queryObj[key].operator) + ' `' + String(queryObj[key].attrs) + '`';
      // } else {
      //   tableParams.QueryFilter[key] = {};
      //   tableParams.QueryFilter[key].ComparisonOperator = queryObj[key].operator;
      //   tableParams.QueryFilter[key].AttributeValueList = [];


      //   const attrResult = queryObj[key].attrs;
      //   //const attrResult = DynamoFromJSON(queryObj[key].attrs);
      //   if (attrResult instanceof Array) {
      //     tableParams.QueryFilter[key].AttributeValueList = queryObj[key].attrs;
      //     //tableParams.QueryFilter[key].AttributeValueList =
      //         DynamoFromJSON(queryObj[key].attrs);
      //   } else {
      //     tableParams.QueryFilter[key].AttributeValueList.push(queryObj[key].attrs);
      //     //tableParams.QueryFilter[key].AttributeValueList.push(
      //           DynamoFromJSON(queryObj[key].attrs));
      //   }
      //   queryString = queryString + '& `' + String(key) + '` '
      //        + String(queryObj[key].operator) + ' `' + String(queryObj[key].attrs) + '`';
      // }
    });

    tableParams.KeyConditionExpression = KeyConditionExpression.join(' AND ');
    if (countProperties(tableParams.ExpressionAttributeNames)
    > countProperties(KeyConditionExpression)) {
      // tableParams.FilterExpression = '';
      tableParams.FilterExpression = `${FilterExpression.join(' AND ')}`;
    }
  }
  queryString += ' WITH QUERY OPERATION ';
  logger.log('info', queryString.blue, stopTimer(timeStart).bold.cyan);
  return tableParams;
}

/**
 * Builds table parameters for scan operation
 * @param  {[type]} model       Model object
 * @param  {[type]} filter      Filter
 * @param  {[type]} queryString String that holds query operation actions
 * @param  {[type]} timeStart   start time of operation
 */
function scan(model, filter, queryString, timeStart) {
  // Table parameters to do the query/scan
  const tableParams = {};
  // Define the filter if it does not exist
  if (!filter) {
    filter = {};
  }
  // Initialize query as an empty object
  const queryScan = {};
  // Set scanfilter to empty object
  tableParams.ScanFilter = {};
  // If a where clause exists in the query, extract
  // the conditions from it.
  if (filter.where) {
    queryString += ' WHERE ';
    Object.keys(filter.where).forEach((key) => {
      let condition = filter.where[key];
      // If condition is of type object, obtain key
      // and the actual condition on the key
      // In jugglingdb, `where` can have the following
      // forms.
      // 1) where : { key: value }
      // 2) where : { startTime : { gt : Date.now() } }
      // 3) where : { someKey : ['something','nothing'] }
      // condition now holds value in case 1),
      //  { gt: Date.now() } in case 2)
      // ['something, 'nothing'] in case 3)
      let insideKey = null;
      if (condition && condition.constructor.name === 'Object') {
        logger.log('debug', 'Condition Type => Object', 'Operator', insideKey, 'Condition Value:', condition);
        [insideKey] = Object.keys(condition);
        condition = condition[insideKey];
        // insideKey now holds gt and condition now holds Date.now()
        queryScan[key] = {
          operator: OperatorLookup(insideKey),
          attrs: condition,
        };
      } else if (condition && condition.constructor.name === 'Array') {
        logger.log('debug', 'Condition Type => Array', 'Operator', insideKey, 'Condition Value:', condition);
        queryScan[key] = {
          operator: 'IN',
          attrs: condition,
        };
      } else {
        logger.log('debug', 'Condition Type => Equality', 'Condition Value:', condition);
        queryScan[key] = {
          operator: 'EQ',
          attrs: condition,
        };
      }
      tableParams.ScanFilter[key] = {};
      tableParams.ScanFilter[key].ComparisonOperator = queryScan[key].operator;
      tableParams.ScanFilter[key].AttributeValueList = [];

      const attrResult = queryScan[key].attrs;
      // const attrResult = DynamoFromJSON(queryScan[key].attrs);

      if (attrResult instanceof Array) {
        logger.log('debug', 'Attribute Value list is an array');
        tableParams.ScanFilter[key].AttributeValueList = queryScan[key].attrs;
        // tableParams.ScanFilter[key].AttributeValueList = DynamoFromJSON(queryScan[key].attrs);
      } else {
        tableParams.ScanFilter[key].AttributeValueList.push(queryScan[key].attrs);
        // tableParams.ScanFilter[key].AttributeValueList.push(
        //  DynamoFromJSON(queryScan[key].attrs));
      }

      queryString += `'${String(key)}' ${String(queryScan[key].operator)} '${String(queryScan[key].attrs)}'`;
    });
  }
  queryString += ' WITH SCAN OPERATION ';
  logger.log('info', queryString.blue, stopTimer(timeStart).bold.cyan);

  return tableParams;
}
/**
 *  Uses Amazon DynamoDB query/scan function to fetch all
 *  matching entries in the table.
 *
 */
DynamoDB.prototype.all = function all(model, filter, callback) {
  const timeStart = startTimer();
  let queryString = 'GET ALL ITEMS FROM TABLE ';

  // If limit is specified, use it to limit results
  let limitObjects;
  if (filter && filter.limit) {
    if (typeof (filter.limit) !== 'number') {
      callback(new Error('Limit must be a number in Model.all function'), null);
      return;
    }
    limitObjects = filter.limit;
  }


  // Order, default by hash key or id
  let orderByField;
  const args = {};
  if (this._models[model].rangeKey === undefined) {
    orderByField = this._models[model].hashKey;
    args[orderByField] = 1;
  } else {
    orderByField = 'id';
    args.id = 1;
  }
  // Custom ordering
  if (filter && filter.order) {
    let keys = filter.order;
    if (typeof keys === 'string') {
      keys = keys.split(',');
    }

    Object.keys(keys).forEach((index) => {
      const m = keys[index].match(/\s+(A|DE)SC$/);
      let keyA = keys[index];
      keyA = keyA.replace(/\s+(A|DE)SC$/, '').trim();
      orderByField = keyA;
      if (m && m[1] === 'DE') {
        args[keyA] = -1;
      } else {
        args[keyA] = 1;
      }
    });
  }

  // Skip , Offset
  let offset;
  if (filter && filter.offset) {
    if (typeof (filter.offset) !== 'number') {
      callback(new Error('Offset must be a number in Model.all function'), null);
      return;
    }
    offset = filter.offset;
  } else if (filter && filter.skip) {
    if (typeof (filter.skip) !== 'number') {
      callback(new Error('Skip must be a number in Model.all function'), null);
      return;
    }
    offset = filter.skip;
  }

  queryString += String(this.tables(model));
  // If hashKey is present in where filter, use query
  let hashKeyFound = false;
  if (filter && filter.where) {
    Object.keys(filter.where).forEach((key) => {
      // console.log(key, this._models[model].hashKey);
      if (key === this._models[model].hashKey) {
        hashKeyFound = true;
        logger.log('debug', 'Hash Key Found, QUERY operation will be used');
      }
    });
  }

  // Check if an array of hash key values are provided. If yes, use scan.
  // Otherwise use query. This is because query does not support array of
  // hash key values
  if (hashKeyFound === true) {
    let condition = filter.where[this._models[model].hashKey];
    let insideKey = null;
    if ((condition && condition.constructor.name === 'Object') || (condition && condition.constructor.name === 'Array')) {
      [insideKey] = Object.keys(condition);
      condition = condition[insideKey];
      if (condition instanceof Array) {
        hashKeyFound = false;
        logger.log('debug', 'Hash key value is an array. Using SCAN operation instead');
      }
    }
  }

  // If true use query function
  if (hashKeyFound === true) {
    const tableParams = query(model, filter, this._models[model], queryString, timeStart);
    // console.log('tableParams', tableParams);
    // Set table name based on model
    tableParams.TableName = this.tables(model);
    tableParams.ReturnConsumedCapacity = 'TOTAL';

    const attributeSpecs = this._attributeSpecs[model];
    let LastEvaluatedKey = 'junk';
    let queryResults = [];
    const finalResult = [];
    const {
      hashKey, pKey, pkSeparator, rangeKey,
    } = this._models[model];
    const { docClient } = this;
    tableParams.ExclusiveStartKey = undefined;
    const modelObj = this._models[model];
    // If KeyConditions exist, then call DynamoDB query function
    if (tableParams.KeyConditionExpression) {
      async.doWhilst(function (queryCallback) {
        logger.log('debug', 'Query issued');
        docClient.query(tableParams, function (err, res) {
          if (err || !res) {
            queryCallback(err);
          } else {
            // Returns an array of objects. Pass each one to
            // JSONFromDynamo and push to empty array
            LastEvaluatedKey = res.LastEvaluatedKey;
            if (LastEvaluatedKey !== undefined) {
              logger.log('debug', 'LastEvaluatedKey found. Refetching..');
              tableParams.ExclusiveStartKey = LastEvaluatedKey;
            }

            queryResults = queryResults.concat(res.Items);
            queryCallback();
          }
        }.bind(this));
      }, function () {
        return LastEvaluatedKey !== undefined;
      }, function (err) {
        if (err) {
          callback(err, null);
        } else {
          if (offset !== undefined) {
            logger.log('debug', 'Offset by', offset);
            queryResults = queryResults.slice(offset, limitObjects + offset);
          }
          if (limitObjects !== undefined) {
            logger.log('debug', 'Limit by', limitObjects);
            queryResults = queryResults.slice(0, limitObjects);
          }
          logger.log('debug', 'Sort by', orderByField, 'Order:', args[orderByField] > 0 ? 'ASC' : 'DESC');
          queryResults = helper.SortByKey(queryResults, orderByField, args[orderByField]);
          if (filter && filter.include) {
            logger.log('debug', 'Model includes', filter.include);
            modelObj.model.include(queryResults, filter.include, callback);
          } else {
            logger.log('debug', 'Query results complete');
            callback(null, queryResults);
          }
        }
      }.bind(this));
    }
  } else {
    // Call scan function
    const tableParams = scan(model, filter, queryString, timeStart);
    tableParams.TableName = this.tables(model);
    tableParams.ReturnConsumedCapacity = 'TOTAL';
    const attributeSpecs = this._attributeSpecs[model];
    const finalResult = [];
    const {
      hashKey, pKey, pkSeparator, rangeKey,
    } = this._models[model];
    let LastEvaluatedKey = 'junk';
    let queryResults = [];
    const { docClient } = this;
    tableParams.ExclusiveStartKey = undefined;
    const modelObj = this._models[model];
    // Scan DynamoDB table
    async.doWhilst(function (queryCallback) {
      docClient.scan(tableParams, function (err, res) {
        if (err || !res) {
          queryCallback(err);
        } else {
          LastEvaluatedKey = res.LastEvaluatedKey;
          if (LastEvaluatedKey !== undefined) {
            tableParams.ExclusiveStartKey = LastEvaluatedKey;
          }
          queryResults = queryResults.concat(res.Items);
          queryCallback();
        }
      }.bind(this));
    }, function () {
      return LastEvaluatedKey !== undefined;
    }, function (err) {
      if (err) {
        callback(err, null);
      } else {
        if (offset !== undefined) {
          logger.log('debug', 'Offset by', offset);
          queryResults = queryResults.slice(offset, limitObjects + offset);
        }
        if (limitObjects !== undefined) {
          logger.log('debug', 'Limit by', limitObjects);
          queryResults = queryResults.slice(0, limitObjects);
        }
        logger.log('debug', 'Sort by', orderByField, 'Order:', args[orderByField] > 0 ? 'ASC' : 'DESC');
        queryResults = helper.SortByKey(queryResults, orderByField, args[orderByField]);
        if (filter && filter.include) {
          logger.log('debug', 'Model includes', filter.include);
          modelObj.model.include(queryResults, filter.include, callback);
        } else {
          callback(null, queryResults);
          logger.log('debug', 'Query complete');
        }
      }
    }.bind(this));
  }
};
/**
 * Find an item based on hashKey alone
 * @param  {object}   model    [description]
 * @param  {object/primitive}   pKey   : If range key is undefined,
 *                                       this is the same as hash key. If range key is defined,
 *                                       then pKey is hashKey + (Separator) + rangeKey
 * @param  {Function} callback
 */
DynamoDB.prototype.find = function find(model, pk, callback) {
  const timeStart = startTimer();
  const queryString = 'GET AN ITEM FROM TABLE ';
  const {
    hashKey, rangeKey, pKey, pkSeparator,
  } = this._models[model];
  let hk;
  let rk;
  if (pKey !== undefined) {
    const temp = pk.split(pkSeparator);
    [hk, rk] = temp;
    if (this._attributeSpecs[model][rangeKey] === 'number') {
      rk = parseInt(rk, 10);
    } else if (this._attributeSpecs[model][rangeKey] === 'date') {
      rk = Number(rk);
    }
  } else {
    hk = pk;
  }

  // If hashKey is of type Number use parseInt
  if (this._attributeSpecs[model][hashKey] === 'number') {
    hk = parseInt(hk, 10);
  } else if (this._attributeSpecs[model][hashKey] === 'date') {
    hk = Number(hk);
  }

  const tableParams = {};
  tableParams.Key = {};
  tableParams.Key[hashKey] = hk;
  if (pKey !== undefined) {
    tableParams.Key[rangeKey] = rk;
  }

  tableParams.TableName = this.tables(model);

  tableParams.ReturnConsumedCapacity = 'TOTAL';

  if (tableParams.Key) {
    this.docClient.get(tableParams, function (err, res) {
      if (err || !res) {
        callback(err, null);
      } else if (isEmpty(res)) {
        callback(null, null);
      } else {
        // Single object - > Array
        callback(null, res.Item);
        logger.log('info', queryString.blue, stopTimer(timeStart).bold.cyan);
      }
    }.bind(this));
  }
};

/**
 * Save an object to the database
 * @param  {[type]}   model    [description]
 * @param  {[type]}   data     [description]
 * @param  {Function} callback [description]
 * @return {[type]}            [description]
 */
DynamoDB.prototype.save = function save(model, data, callback) {
  const timeStart = startTimer();
  const originalData = {};
  const {
    hashKey, rangeKey, pkSeparator, pKey,
  } = this._models[model].hashKey;

  /* Data is the original object coming in the body. In the body
      if the data has a key which is breakable, it must be chunked
      into N different attrs. N is specified by the breakValue[key]
  */
  const attributeSpecs = this._attributeSpecs[model];
  const outerCounter = 0;

  /*
    Checks for hash and range keys
    */
  if ((data[hashKey] === null) || (data[hashKey] === undefined)) {
    const err = new Error(`Hash Key '${hashKey}' cannot be null or undefined.`);
    callback(err, null);
    return;
  }
  // If pKey is defined, range key is also present.
  if (pKey !== undefined) {
    if ((data[rangeKey] === null) || (data[rangeKey] === undefined)) {
      const err = new Error(`Range Key '${rangeKey}' cannot be null or undefined.`);
      callback(err, null);
      return;
    }
    data[pKey] = String(data[hashKey]) + pkSeparator + String(data[rangeKey]);
    originalData[pKey] = data[pKey];
  }

  // Copy all attrs from data to originalData
  Object.keys(data).forEach((key) => {
    originalData[key] = data[key];
  });

  const queryString = 'PUT ITEM IN TABLE ';
  const tableParams = {};
  tableParams.TableName = this.tables(model);
  tableParams.ReturnConsumedCapacity = 'TOTAL';

  if (pKey !== undefined) {
    delete data[pKey];
  }
  tableParams.Item = data;
  this.docClient.put(tableParams, function (err, res) {
    if (err) {
      callback(err, null);
    } else {
      callback(null, originalData);
    }
  }.bind(this));

  logger.log('info', queryString.blue, stopTimer(timeStart).bold.cyan);
};

// function not working
DynamoDB.prototype.updateAttributes = function (model, pk, data, callback) {
  const timeStart = startTimer();
  const originalData = {};
  const {
    hashKey, rangeKey, pkSeparator, pKey,
  } = this._models[model];
  let hk;
  let rk;
  let err;
  let key;
  const tableParams = {};
  const attributeSpecs = this._attributeSpecs[model];
  const outerCounter = 0;
  // Copy all attrs from data to originalData
  Object.keys(data).forEach((key1) => {
    originalData[key1] = data[key1];
  });

  // If pKey is defined, range key is also present.
  if (pKey !== undefined) {
    if ((data[rangeKey] === null) || (data[rangeKey] === undefined)) {
      err = new Error(`Range Key '${rangeKey}' cannot be null or undefined.`);
      callback(err, null);
      return;
    }
    data[pKey] = String(data[hashKey]) + pkSeparator + String(data[rangeKey]);
    originalData[pKey] = data[pKey];
  }

  // Log queryString
  const queryString = 'UPDATE ITEM IN TABLE ';

  // Use updateItem function of DynamoDB

  // Set table name as usual
  tableParams.TableName = this.tables(model);
  tableParams.Key = {};
  tableParams.AttributeUpdates = {};
  tableParams.ReturnConsumedCapacity = 'TOTAL';

  // Add hashKey / rangeKey to tableParams
  if (pKey !== undefined) {
    const temp = pk.split(pkSeparator);
    [hk, rk] = temp;
    tableParams.Key[this._models[model].hashKey] = hk;
    tableParams.Key[this._models[model].rangeKey] = rk;
  } else {
    tableParams.Key[this._models[model].hashKey] = pk;
    hk = pk;
  }

  if (pKey !== undefined) {
    delete data[pKey];
  }
  // Add attrs to update

  Object.keys(data).forEach((key2) => {
    if (data.hasOwnProperty(key2) && data[key2] !== null
    && (key2 !== hashKey) && (key2 !== rangeKey)) {
      tableParams.AttributeUpdates[key2] = {};
      tableParams.AttributeUpdates[key2].Action = 'PUT';
      tableParams.AttributeUpdates[key2].Value = data[key2];
    }
  });

  tableParams.ReturnValues = 'ALL_NEW';
  this.docClient.update(tableParams, function (err2, res) {
    if (err2) {
      callback(err2, null);
    } else if (!res) {
      callback(null, null);
    } else {
      callback(null, res.data);
    }
  }.bind(this));
  logger.log('info', queryString.blue, stopTimer(timeStart).bold.cyan);
};

DynamoDB.prototype.destroy = function (model, pk, callback) {
  const timeStart = startTimer();
  const {
    hashKey, rangeKey, pKey, pkSeparator,
  } = this._models[model].hashKey;
  let hk;
  let rk;

  if (pKey !== undefined) {
    const temp = pk.split(pkSeparator);
    [hk, rk] = temp;
    if (this._attributeSpecs[model][rangeKey] === 'number') {
      rk = parseInt(rk, 10);
    } else if (this._attributeSpecs[model][rangeKey] === 'date') {
      rk = Number(rk);
    }
  } else {
    hk = pk;
  }

  // If hashKey is of type Number use parseInt
  if (this._attributeSpecs[model][hashKey] === 'number') {
    hk = parseInt(hk, 10);
  } else if (this._attributeSpecs[model][hashKey] === 'date') {
    hk = Number(hk);
  }

  // Use updateItem function of DynamoDB
  const tableParams = {};
  // Set table name as usual
  tableParams.TableName = this.tables(model);
  tableParams.Key = {};
  // Add hashKey to tableParams
  tableParams.Key[this._models[model].hashKey] = hk;

  if (pKey !== undefined) {
    tableParams.Key[this._models[model].rangeKey] = rk;
  }

  tableParams.ReturnValues = 'ALL_OLD';
  const attributeSpecs = this._attributeSpecs[model];
  const outerCounter = 0;
  const chunkedData = {};

  this.docClient.delete(tableParams, function (err, res) {
    if (err) {
      callback(err, null);
    } else if (!res) {
      callback(null, null);
    } else {
      // Attributes is an object
      const tempString = `DELETE ITEM FROM TABLE ${tableParams.TableName} WHERE ${hashKey} 'EQ' ${String(hk)}`;
      logger.log('info', tempString.blue, stopTimer(timeStart).bold.cyan);
      callback(null, res.Attributes);
    }
  }.bind(this));
};

DynamoDB.prototype.defineForeignKey = function (model, key, cb) {
  const { hashKey } = this._models[model];
  const attributeSpec = this._attributeSpecs[model].id || this._attributeSpecs[model][hashKey];
  if (attributeSpec === 'string') {
    cb(null, String);
  } else if (attributeSpec === 'number') {
    cb(null, Number);
  } else if (attributeSpec === 'date') {
    cb(null, Date);
  }
};

/**
 * Destroy all deletes all records from table.
 * @param  {[type]}   model    [description]
 * @param  {Function} callback [description]
 */
DynamoDB.prototype.destroyAll = function (model, callback) {
  /*
    Note:
    Deleting individual items is extremely expensive. According to
    AWS, a better solution is to destroy the table, and create it back again.
    */
  const timeStart = startTimer();
  const t = `DELETE EVERYTHING IN TABLE: ${this.tables(model)}`;
  const {
    hashKey, rangeKey, pkSeparator,
  } = this._models[model].hashKey;
  const attributeSpecs = this._attributeSpecs[model];
  let hk;
  let rk;
  let pk;
  const { docClient } = this;

  const self = this;
  const tableParams = {};
  tableParams.TableName = this.tables(model);
  docClient.scan(tableParams, function (err, res) {
    if (err) {
      callback(err);
    } else if (res === null) {
      callback(null);
    } else {
      async.mapSeries(res.Items, function (item, insideCallback) {
        if (rangeKey === undefined) {
          hk = item[hashKey];
          pk = hk;
        } else {
          hk = item[hashKey];
          rk = item[rangeKey];
          pk = String(hk) + pkSeparator + String(rk);
        }
        self.destroy(model, pk, insideCallback);
      }, function (err1, items) {
        if (err) {
          callback(err);
        } else {
          callback();
        }
      }.bind(this));
    }
  });
  logger.log('warn', t.bold.red, stopTimer(timeStart).bold.cyan);
};

/**
 * Get number of records matching a filter
 * @param  {Object}   model
 * @param  {Function} callback
 * @param  {Object}   where    : Filter
 * @return {Number}            : Number of matching records
 */
DynamoDB.prototype.count = function count(model, callback, where) {
  const filter = {};
  filter.where = where;
  this.all(model, filter, function (err, results) {
    if (err || !results) {
      callback(err, null);
    } else {
      callback(null, results.length);
    }
  });
};

/**
 * Check if a given record exists
 * @param  {[type]}   model    [description]
 * @param  {[type]}   id       [description]
 * @param  {Function} callback [description]
 * @return {[type]}            [description]
 */
DynamoDB.prototype.exists = function exists(model, id, callback) {
  this.find(model, id, function (err, record) {
    if (err) {
      callback(err, null);
    } else if (isEmpty(record)) {
      callback(null, false);
    } else {
      callback(null, true);
    }
  });
};
