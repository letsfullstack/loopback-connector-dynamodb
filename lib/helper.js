/* eslint-disable prefer-arrow-callback */
/* eslint-disable func-names */
/* eslint-disable no-extra-bind */
/* eslint-disable no-mixed-operators */

const async = require('async');
// let colors = require('colors');

module.exports = {
  TypeLookup: function TypeLookup(typestring) {
    switch (typestring) {
      case 'string':
        return 'S';
      case 'number':
        return 'N';
      case 'boolean':
        return 'S';
      case 'date':
        return 'N';
      default:
        break;
    }
  },

  ReverseTypeLookup: function ReverseTypeLookup(typestring) {
    switch (typestring) {
      case 'date':
        return 'N';
      default:
        break;
    }

    if (typestring === 'S') {
      return 'string';
    }
    if (typestring === 'N') {
      return 'number';
    }

    return 'string';
  },
  /**
   * Helper function to convert a regular model
   * object to DynamoDB JSON notation.
   *
   * e.g 20 will be returned as { 'N': '20' }
   * & `foobar` will be returned as { 'S' : 'foobar' }
   *
   * Usage
   * - objToDB(20);
   * - objToDB('foobar');
   * ----------------------------------------------
   *
   * @param  {object} data to be converted
   * @return {object} DynamoDB compatible JSON object
   */
  objToDB: function objToDB(data) {
    const tempObj = {};
    const elementType = this.TypeLookup(typeof (data));
    tempObj[elementType] = data.toString();
    return tempObj;
  },
  /**
   * Helper function to convert a DynamoDB type
   * object into regular model object.
   *
   * e.g { 'N': '20' } will be returned as 20
   * & { 'S' : 'foobar' }  will be returned as `foobar`
   *
   * @param  {object} data
   * @return {object}
   */
  objFromDB: function objFromDB(data) {
    let tempObj;
    Object.keys(data).forEach((key) => {
      if (data.hasOwnProperty(key)) {
        const elementType = this.ReverseTypeLookup(key);
        if (elementType === 'string') {
          tempObj = data[key];
        } else if (elementType === 'number') {
          tempObj = Number(data[key]);
        } else {
          tempObj = data[key];
        }
      }
    });

    return tempObj;
  },
  /**
   * Slice a string into N different strings
   * @param  {String} str : The string to be chunked
   * @param  {Number} N   : Number of pieces into which the string must be broken
   * @return {Array}  Array of N strings
   */
  splitSlice: function splitSlice(str, N) {
    const ret = [];
    const strLen = str.length;
    if (strLen === 0) {
      return ret;
    }

    const len = Math.floor(strLen / N) + 1;
    const residue = strLen % len;
    let offset = 0;
    for (let index = 1; index < N; index += 1) {
      const subString = str.slice(offset, len + offset);
      ret.push(subString);
      offset += len;
    }
    ret.push(str.slice(offset, residue + offset));
    return ret;
  },
  /**
   * Chunks data and assigns it to the data object
   * @param {Object} data : Complete data object
   * @param {String} key  : Attribute to be chunked
   * @param {Number} N    : Number of chunks
   */
  ChunkMe: function ChunkMe(data, key, N) {
    let counter;
    const newData = [];
    // Call splitSlice to chunk the data
    const chunkedData = this.splitSlice(data[key], N);
    // Assign each element in the chunked data
    // to data.
    for (counter = 1; counter <= N; counter += 1) {
      const tempObj = {};
      const chunkKeyName = key;
      // DynamoDB does not allow empty strings.
      // So filter out empty strings
      if (chunkedData[counter - 1] !== '') {
        tempObj[chunkKeyName] = chunkedData[counter - 1];
        newData.push(tempObj);
      }
    }
    delete data[key];
    // Finally delete data[key]
    return newData;
  },
  /**
   * Builds back a chunked object stored in the
   * database to its normal form
   * @param {Object} data : Object to be rebuilt
   * @param {String} key  : Name of the field in the object
   */
  BuildMeBack: function BuildMeBack(data, breakKeys) {
    let counter;
    let currentName;
    let finalObject;
    breakKeys.forEach((breakKey) => {
      counter = 1;
      finalObject = '';
      Object.keys(data).forEach(() => {
        currentName = `${breakKey}-${String(counter)}`;
        if (data[currentName]) {
          finalObject += data[currentName];
          delete data[currentName];
          counter += 1;
        }
      });
      data[breakKey] = finalObject;
    });
    return data;
  },
  /*
  See http://stackoverflow.com/questions/105034/how-to-create-a-guid-uuid-in-javascript
   */
  UUID: function UUID() {
    const uuid = 'xxxxxxxx-xxxx-4xxx-yxxx-xxxxxxxxxxxx'.replace(/[xy]/g, (c) => {
      const r = Math.random() * 16 | 0;
      const v = (c === 'x' ? r : (r & 0x3 | 0x8));
      return v.toString(16);
    });
    return uuid;
  },

  GetMyChildrenBack: function GetMyChildrenBack(data, model, pKey, breakb, dynamodb, outerCb) {
    // Iterate over breakb. Query using data's hashKey
    const hashKeyAttribute = `${model.toLowerCase()}#${pKey}`;
    /*
    Use async series to fetch each breakable attribute in series.
     */
    async.mapSeries(breakb, (breakable, callback) => {
      const params = {};
      params.KeyConditions = {};
      params.KeyConditions[hashKeyAttribute] = {};
      params.KeyConditions[hashKeyAttribute].ComparisonOperator = 'EQ';
      params.KeyConditions[hashKeyAttribute].AttributeValueList = [];
      params.KeyConditions[hashKeyAttribute].AttributeValueList.push(
        { S: String(data[pKey]) },
      );
      params.TableName = `${model}_${breakable}`;

      dynamodb.query(params, function (err, res) {
        if (err) {
          return callback(err, null);
        }

        let callbackData = '';
        res.Items.forEach(function (item) {
          callbackData += item[breakable].S;
        });
        return callback(null, callbackData);
      }.bind(this));
    }, function (err, results) {
      if (err) {
        outerCb(err, null);
      } else {
        // results array will contain an array of built back attribute values.
        for (let i = 0; i < results.length; i += 1) {
          data[breakb[i]] = results[i];
        }
        outerCb(null, data);
      }
    }.bind(this));
  },
  SortByKey: function SortByKey(array, key, order) {
    return array.sort((a, b) => {
      let x = a[key];
      let y = b[key];

      if (typeof x === 'string') {
        x = x.toLowerCase();
        y = y.toLowerCase();
      }

      if (order === 1) {
        return ((x < y) ? -1 : ((x > y) ? 1 : 0));
      }

      return ((x < y) ? 1 : ((x > y) ? -1 : 0));
    });
  },
};
