// Copyright IBM Corp. 2012,2019. All Rights Reserved.
// Node module: loopback-connector-mysql
// This file is licensed under the MIT License.
// License text available at https://opensource.org/licenses/MIT

'use strict';
const g = require('strong-globalize')();
const util = require('util');

/*!
 * Module dependencies
 */
const mysql = require('mysql');

const SqlConnector = require('loopback-connector').SqlConnector;
const ParameterizedSQL = SqlConnector.ParameterizedSQL;
const EnumFactory = require('./enumFactory').EnumFactory;

const debug = require('debug')('loopback:connector:mysql');
const debugSort = require('debug')('loopback:connector:mysql:order');
const setHttpCode = require('./set-http-code');

/**
 * @module loopback-connector-mysql
 *
 * Initialize the MySQL connector against the given data source
 *
 * @param {DataSource} dataSource The loopback-datasource-juggler dataSource
 * @param {Function} [callback] The callback function
 */
exports.initialize = function initializeDataSource(dataSource, callback) {
  dataSource.driver = mysql; // Provide access to the native driver
  dataSource.connector = new MySQL(dataSource.settings);
  dataSource.connector.dataSource = dataSource;

  defineMySQLTypes(dataSource);

  dataSource.EnumFactory = EnumFactory; // factory for Enums. Note that currently Enums can not be registered.

  if (callback) {
    if (dataSource.settings.lazyConnect) {
      process.nextTick(function() {
        callback();
      });
    } else {
      dataSource.connector.connect(callback);
    }
  }
};

exports.MySQL = MySQL;

function defineMySQLTypes(dataSource) {
  const modelBuilder = dataSource.modelBuilder;
  const defineType = modelBuilder.defineValueType ?
    // loopback-datasource-juggler 2.x
    modelBuilder.defineValueType.bind(modelBuilder) :
    // loopback-datasource-juggler 1.x
    modelBuilder.constructor.registerType.bind(modelBuilder.constructor);

  // The Point type is inherited from jugglingdb mysql adapter.
  // LoopBack uses GeoPoint instead.
  // The Point type can be removed at some point in the future.
  defineType(function Point() {
  });
}

/**
 * @constructor
 * Constructor for MySQL connector
 * @param {Object} client The node-mysql client object
 */
function MySQL(settings) {
  SqlConnector.call(this, 'mysql', settings);
}

require('util').inherits(MySQL, SqlConnector);

MySQL.prototype.connect = function(callback) {
  const self = this;
  const options = generateOptions(this.settings);
  const s = self.settings || {};

  if (this.client) {
    if (callback) {
      process.nextTick(function() {
        callback(null, self.client);
      });
    }
  } else {
    this.client = mysql.createPool(options);
    this.client.getConnection(function(err, connection) {
      const conn = connection;
      if (!err) {
        if (self.debug) {
          debug('MySQL connection is established: ' + self.settings || {});
        }
        connection.release();
      } else {
        if (self.debug || !callback) {
          console.error('MySQL connection is failed: ' + self.settings || {}, err);
        }
      }
      callback && callback(err, conn);
    });
  }
};

function generateOptions(settings) {
  const s = settings || {};
  if (s.collation) {
    // Charset should be first 'chunk' of collation.
    s.charset = s.collation.substr(0, s.collation.indexOf('_'));
  } else {
    s.collation = 'utf8_general_ci';
    s.charset = 'utf8';
  }

  s.supportBigNumbers = (s.supportBigNumbers || false);
  s.timezone = (s.timezone || 'local');

  if (isNaN(s.connectionLimit)) {
    s.connectionLimit = 10;
  }

  let options;
  if (s.url) {
    // use url to override other settings if url provided
    options = s.url;
  } else {
    options = {
      host: s.host || s.hostname || 'localhost',
      port: s.port || 3306,
      user: s.username || s.user,
      password: s.password,
      timezone: s.timezone,
      socketPath: s.socketPath,
      charset: s.collation.toUpperCase(), // Correct by docs despite seeming odd.
      supportBigNumbers: s.supportBigNumbers,
      connectionLimit: s.connectionLimit,
    };

    // Don't configure the DB if the pool can be used for multiple DBs
    if (!s.createDatabase) {
      options.database = s.database;
    }

    // Take other options for mysql driver
    // See https://github.com/strongloop/loopback-connector-mysql/issues/46
    for (const p in s) {
      if (p === 'database' && s.createDatabase) {
        continue;
      }
      if (options[p] === undefined) {
        options[p] = s[p];
      }
    }
    // Legacy UTC Date Processing fallback - SHOULD BE TRANSITIONAL
    if (s.legacyUtcDateProcessing === undefined) {
      s.legacyUtcDateProcessing = true;
    }
    if (s.legacyUtcDateProcessing) {
      options.timezone = 'Z';
    }
  }
  return options;
}

/**
 * Execute the sql statement
 *
 * @param {String} sql The SQL statement
 * @param {Function} [callback] The callback after the SQL statement is executed
 */
MySQL.prototype.executeSQL = function(sql, params, options, callback) {
  const self = this;
  const client = this.client;
  const debugEnabled = debug.enabled;
  const db = this.settings.database;
  if (typeof callback !== 'function') {
    throw new Error(g.f('{{callback}} should be a function'));
  }
  if (debugEnabled) {
    debug('SQL: %s, params: %j', sql, params);
  }

  const transaction = options.transaction;

  function handleResponse(connection, err, result) {
    if (!transaction) {
      connection.release();
    }
    if (err) {
      err = setHttpCode(err);
    }
    callback && callback(err, result);
  }

  function runQuery(connection, release) {
    connection.query(sql, params, function(err, data) {
      if (debugEnabled) {
        if (err) {
          debug('Error: %j', err);
        }
        debug('Data: ', data);
      }
      handleResponse(connection, err, data);
    });
  }

  function executeWithConnection(err, connection) {
    if (err) {
      return callback && callback(err);
    }
    if (self.settings.createDatabase) {
      // Call USE db ...
      connection.query('USE ??', [db], function(err) {
        if (err) {
          if (err && err.message.match(/(^|: )unknown database/i)) {
            const charset = self.settings.charset;
            const collation = self.settings.collation;
            const q = 'CREATE DATABASE ?? CHARACTER SET ?? COLLATE ??';
            connection.query(q, [db, charset, collation], function(err) {
              if (!err) {
                connection.query('USE ??', [db], function(err) {
                  runQuery(connection);
                });
              } else {
                handleResponse(connection, err);
              }
            });
            return;
          } else {
            handleResponse(connection, err);
            return;
          }
        }
        runQuery(connection);
      });
    } else {
      // Bypass USE db
      runQuery(connection);
    }
  }

  if (transaction && transaction.connection &&
    transaction.connector === this) {
    if (debugEnabled) {
      debug('Execute SQL within a transaction');
    }
    executeWithConnection(null, transaction.connection);
  } else {
    client.getConnection(executeWithConnection);
  }
};

MySQL.prototype._modifyOrCreate = function(model, data, options, fields, cb) {
  const sql = new ParameterizedSQL('INSERT INTO ' + this.tableEscaped(model));
  const columnValues = fields.columnValues;
  const fieldNames = fields.names;
  if (fieldNames.length) {
    sql.merge('(' + fieldNames.join(',') + ')', '');
    const values = ParameterizedSQL.join(columnValues, ',');
    values.sql = 'VALUES(' + values.sql + ')';
    sql.merge(values);
  } else {
    sql.merge(this.buildInsertDefaultValues(model, data, options));
  }

  sql.merge('ON DUPLICATE KEY UPDATE');
  const setValues = [];
  for (let i = 0, n = fields.names.length; i < n; i++) {
    if (!fields.properties[i].id) {
      setValues.push(new ParameterizedSQL(fields.names[i] + '=' +
          columnValues[i].sql, columnValues[i].params));
    }
  }

  sql.merge(ParameterizedSQL.join(setValues, ','));

  this.execute(sql.sql, sql.params, options, function(err, info) {
    if (!err && info && info.insertId) {
      data.id = info.insertId;
    }
    const meta = {};
    // When using the INSERT ... ON DUPLICATE KEY UPDATE statement,
    // the returned value is as follows:
    // 1 for each successful INSERT.
    // 2 for each successful UPDATE.
    // 1 also for UPDATE with same values, so we cannot accurately
    // report if we have a new instance.
    meta.isNewInstance = undefined;
    cb(err, data, meta);
  });
};

/**
 * Replace if the model instance exists with the same id or create a new instance
 *
 * @param {String} model The model name
 * @param {Object} data The model instance data
 * @param {Object} options The options
 * @param {Function} [cb] The callback function
 */
MySQL.prototype.replaceOrCreate = function(model, data, options, cb) {
  const fields = this.buildReplaceFields(model, data);
  this._modifyOrCreate(model, data, options, fields, cb);
};

/**
 * Update if the model instance exists with the same id or create a new instance
 *
 * @param {String} model The model name
 * @param {Object} data The model instance data
 * @param {Object} options The options
 * @param {Function} [cb] The callback function
 */
MySQL.prototype.save =
MySQL.prototype.updateOrCreate = function(model, data, options, cb) {
  const fields = this.buildFields(model, data);
  this._modifyOrCreate(model, data, options, fields, cb);
};

MySQL.prototype.getInsertedId = function(model, info) {
  const insertedId = info && typeof info.insertId === 'number' ?
    info.insertId : undefined;
  return insertedId;
};

/*!
 * Convert property name/value to an escaped DB column value
 * @param {Object} prop Property descriptor
 * @param {*} val Property value
 * @returns {*} The escaped value of DB column
 */
MySQL.prototype.toColumnValue = function(prop, val) {
  if (val === undefined && this.isNullable(prop)) {
    return null;
  }
  if (val === null) {
    if (this.isNullable(prop)) {
      return val;
    } else {
      try {
        const castNull = prop.type(val);
        if (prop.type === Object) {
          return JSON.stringify(castNull);
        }
        return castNull;
      } catch (err) {
        // if we can't coerce null to a certain type,
        // we just return it
        return 'null';
      }
    }
  }
  if (!prop) {
    return val;
  }
  if (prop.type === String) {
    return String(val);
  }
  if (prop.type === Number) {
    if (isNaN(val)) {
      // FIXME: [rfeng] Should fail fast?
      return val;
    }
    return val;
  }
  if (prop.type === Date) {
    if (!val.toUTCString) {
      val = new Date(val);
    }
    return val;
  }
  if (prop.type.name === 'DateString') {
    return val.when;
  }
  if (prop.type === Boolean) {
    return !!val;
  }
  if (prop.type.name === 'GeoPoint') {
    return new ParameterizedSQL({
      sql: 'Point(?,?)',
      params: [val.lng, val.lat],
    });
  }
  if (prop.type === Buffer) {
    return val;
  }
  if (prop.type === Object) {
    return this._serializeObject(val);
  }
  if (typeof prop.type === 'function') {
    return this._serializeObject(val);
  }
  return this._serializeObject(val);
};

MySQL.prototype._serializeObject = function(obj) {
  let val;
  if (obj && typeof obj.toJSON === 'function') {
    obj = obj.toJSON();
  }
  if (typeof obj !== 'string') {
    val = JSON.stringify(obj);
  } else {
    val = obj;
  }
  return val;
};

/*!
 * Convert the data from database column to model property
 * @param {object} Model property descriptor
 * @param {*) val Column value
 * @returns {*} Model property value
 */
MySQL.prototype.fromColumnValue = function(prop, val) {
  if (val == null) {
    return val;
  }
  if (prop) {
    switch (prop.type.name) {
      case 'Number':
        val = Number(val);
        break;
      case 'String':
        val = String(val);
        break;
      case 'Date':
      case 'DateString':
        // MySQL allows, unless NO_ZERO_DATE is set, dummy date/time entries
        // new Date() will return Invalid Date for those, so we need to handle
        // those separate.
        if (!val || /^0{4}(-00){2}( (00:){2}0{2}(\.0{1,6}){0,1}){0,1}$/.test(val)) {
          val = null;
        }
        break;
      case 'Boolean':
        // BIT(1) case: <Buffer 01> for true and <Buffer 00> for false
        // CHAR(1) case: '1' for true and '0' for false
        // TINYINT(1) case: 1 for true and 0 for false
        val = Buffer.isBuffer(val) && val.length === 1 ? Boolean(val[0]) : Boolean(parseInt(val));
        break;
      case 'GeoPoint':
      case 'Point':
        val = {
          lng: val.x,
          lat: val.y,
        };
        break;
      case 'ObjectID':
        val = new prop.type(val);
        break;
      case 'Buffer':
        val = prop.type(val);
        break;
      case 'List':
      case 'Array':
      case 'Object':
      case 'JSON':
        if (typeof val === 'string') {
          val = JSON.parse(val);
        }
        break;
      default:
        break;
    }
  }
  return val;
};

/**
 * Escape an identifier such as the column name
 * @param {string} name A database identifier
 * @returns {string} The escaped database identifier
 */
MySQL.prototype.escapeName = function(name) {
  return this.client.escapeId(name);
};

/**
 * Build the LIMIT clause
 * @param {string} model Model name
 * @param {number} limit The limit
 * @param {number} offset The offset
 * @returns {string} The LIMIT clause
 */
MySQL.prototype._buildLimit = function(model, limit, offset) {
  if (isNaN(limit)) {
    limit = 0;
  }
  if (isNaN(offset)) {
    offset = 0;
  }
  if (!limit && !offset) {
    return '';
  }
  return 'LIMIT ' + (offset ? (offset + ',' + limit) : limit);
};

MySQL.prototype.applyPagination = function(model, stmt, filter) {
  const limitClause = this._buildLimit(model, filter.limit,
    filter.offset || filter.skip);
  return stmt.merge(limitClause);
};

/**
 * Get the place holder in SQL for identifiers, such as ??
 * @param {String} key Optional key, such as 1 or id
 * @returns {String} The place holder
 */
MySQL.prototype.getPlaceholderForIdentifier = function(key) {
  return '??';
};

/**
 * Get the place holder in SQL for values, such as :1 or ?
 * @param {String} key Optional key, such as 1 or id
 * @returns {String} The place holder
 */
MySQL.prototype.getPlaceholderForValue = function(key) {
  return '?';
};

MySQL.prototype.getCountForAffectedRows = function(model, info) {
  const affectedRows = info && typeof info.affectedRows === 'number' ?
    info.affectedRows : undefined;
  return affectedRows;
};

/**
 * Disconnect from MySQL
 */
MySQL.prototype.disconnect = function(cb) {
  if (this.debug) {
    debug('disconnect');
  }
  if (this.client) {
    this.client.end((err) => {
      this.client = null;
      cb(err);
    });
  } else {
    process.nextTick(cb);
  }
};

MySQL.prototype.ping = function(cb) {
  this.execute('SELECT 1 AS result', cb);
};

MySQL.prototype.buildExpression = function(columnName, operator, operatorValue,
  propertyDefinition, columnJSON = '') {
  if (operator === 'regexp') {
    let clause = columnName + ' REGEXP ?';
    // By default, MySQL regexp is not case sensitive. (https://dev.mysql.com/doc/refman/5.7/en/regexp.html)
    // To allow case sensitive regexp query, it has to be binded to a `BINARY` type.
    // If ignore case is not specified, search it as case sensitive.
    if (!operatorValue.ignoreCase) {
      clause = columnName + ' REGEXP BINARY ?';
    }

    if (operatorValue.ignoreCase)
      g.warn('{{MySQL}} {{regex}} syntax does not respect the {{`i`}} flag');
    if (operatorValue.global)
      g.warn('{{MySQL}} {{regex}} syntax does not respect the {{`g`}} flag');

    if (operatorValue.multiline)
      g.warn('{{MySQL}} {{regex}} syntax does not respect the {{`m`}} flag');

    return new ParameterizedSQL(clause,
      [operatorValue.source]);
  }

  // invoke the base implementation of `buildExpression`
  return this.invokeSuper('buildExpression', columnName, operator,
    operatorValue, propertyDefinition);
};

/**
 * Build the INNER JOIN AS clauses for the filter.where object
 * Prepare WHERE clause with the creation innerWhere object
 * Prepare ORDER BY clause with the creation of innerOrder object
 * @param {string} model Model name
 * @param {object} filter An object for the filter
 * @returns {object} An object with the INNER JOIN AS SQL statement innerWhere and innerOrder for next builders
 */
MySQL.prototype.buildJoin = function(model, filter) {
  const {
    order,
    where,
  } = filter;
  let candidateProperty, candidateRelations;
  const innerJoins = [];
  const innerOrder = {};
  const innerWhere = {};
  if (!where && !order) {
    return new ParameterizedSQL('');
  }
  if ((typeof where !== 'object' || Array.isArray(where)) && !order) {
    debug('Invalid value for where: %j', where);
    return new ParameterizedSQL('');
  }
  const props = this.getModelDefinition(model).properties;
  let basePrefix = 0;

  const _buildJoin = (key, type) => {
    let orderValue = '';
    if (type === 'order') {
      const t = key.split(/[\s,]+/);
      orderValue = t.length === 1 ? '' : t[1];
      key = t.length === 1 ? key : t[0];
    }
    let p = props[key];
    if (p) {
      return;
    }
    if (p == null && _isNested(key)) {
      p = props[key.split('.')[0]];
    }
    if (p) {
      return;
    }
    // It may be an innerWhere
    const splitedKey = key.split('.');
    if (splitedKey.length === 1) {
      debug('Unknown property %s is skipped for model %s', key, model);
      return;
    }
    // Pop the property
    candidateProperty = splitedKey.pop();
    // Keep the candidate relations
    candidateRelations = splitedKey;
    let parentModel = model;
    let parentPrefix = '';
    for (let i = 0; i < candidateRelations.length; i++) {
      // Build a prefix for alias to prevent conflict
      const prefix = `_${basePrefix}_${i}_`;
      const candidateRelation = candidateRelations[i];
      const modelDefinition = this.getModelDefinition(parentModel);

      if (!modelDefinition) {
        debug('No definition for model %s', parentModel);
        break;
      }
      // The next line need a monkey patch of @loopback/repository to add relations to modelDefinition */
      if (!(candidateRelation in modelDefinition.settings.__relations__)) {
        debug('No relation for model %s', parentModel);
        break;
      }

      if (Array.isArray(modelDefinition.settings.__hiddenRelations__) &&
       modelDefinition.settings.__hiddenRelations__.includes(candidateRelation)) {
        debug('Hidden relation for model %s skipping', parentModel);
        break;
      }
      const relation =
       modelDefinition.settings.__relations__[candidateRelation];
      // Only supports belongsTo, hasOne and hasMany
      if (relation.type !== 'belongsTo' && relation.type !== 'hasOne' && relation.type !== 'hasMany') {
        debug('Invalid relation type for model %s for inner join', parentModel);
        break;
      }

      const target = relation.target();
      const targetDefinition = this.getModelDefinition(target.modelName);
      let hasProp = true;
      let whereJSON = null;
      // process for data type is json
      if (candidateProperty.indexOf('->') !== -1) {
        const jsonProperty = candidateProperty.split('->');
        candidateProperty = jsonProperty.shift();
        whereJSON = `JSON_UNQUOTE(JSON_EXTRACT(?, "${'$.' + jsonProperty.join('.')}"))`;
      }

      if (!(candidateProperty in targetDefinition.properties)) {
        if (targetDefinition.settings.__relations__[candidateRelations[i + 1]]) {
          hasProp = false;
        } else {
          debug(
           'Unknown property %s is skipped for model %s',
           candidateProperty,
           target.modelName,
          );
          break;
        }
      }

      // Check if join already done
      let alreadyJoined = false;
      for (const innerJoin of innerJoins) {
        if (
         innerJoin.model === target.modelName &&
         innerJoin.parentModel === parentModel
        ) {
          alreadyJoined = true;
          // Keep what needed to build the WHERE or ORDER statement properly
          if (type === 'where' && hasProp) {
            innerWhere[key] = {
              prefix: innerJoin.prefix,
              model: innerJoin.model,
              property: {
                ...targetDefinition.properties[candidateProperty],
                key: candidateProperty,
              },
              whereJSON: whereJSON,
              value: where[key],
            };
          } else if (type === 'order' && hasProp) {
            innerOrder[key] = {
              prefix: innerJoin.prefix,
              model: innerJoin.model,
              property: {
                ...targetDefinition.properties[candidateProperty],
                key: candidateProperty,
              },
              value: orderValue,
            };
          }
        }
      }
      // Keep what needed to build INNER JOIN AS statement
      if (!alreadyJoined) {
        innerJoins.push({
          prefix,
          parentPrefix,
          parentModel,
          relation: {
            ...relation,
            name: candidateRelation,
          },
          model: target.modelName,
        });
        // Keep what needed to build the WHERE or ORDER statement properly
        if (type === 'where' && hasProp) {
          innerWhere[key] = {
            prefix,
            model: target.modelName,
            property: {
              ...targetDefinition.properties[candidateProperty],
              key: candidateProperty,
            },
            value: where[key],
          };
        } else if (type === 'order' && hasProp) {
          innerOrder[key] = {
            prefix,
            model: target.modelName,
            property: {
              ...targetDefinition.properties[candidateProperty],
              key: candidateProperty,
            },
            value: orderValue,
          };
        }
      }
      // Keep the parentModel for recursive INNER JOIN and parentPrefix for the alias
      parentModel = target.modelName;
      parentPrefix = prefix;
    }
    basePrefix++;
  };

  const _extractKeys = (where, _keys = new Set()) => {
    for (const key in where) {
      if (key !== 'or' && key !== 'and') {
        _keys.add(key);
        continue;
      }

      const clauses = where[key];
      if (Array.isArray(clauses)) {
        for (const clause of clauses) {
          _extractKeys(clause, _keys);
        }
      }
    }

    return _keys;
  };

  const flatWhere = [..._extractKeys(where)];

  for (const key of flatWhere) {
    _buildJoin(key, 'where');
  }
  let orderArray = order;
  if (typeof order === 'string') {
    orderArray = [order];
  }
  if (Array.isArray(orderArray)) {
    for (const key of orderArray) {
      _buildJoin(key, 'order');
    }
  }

  const joinStmt = {
    sql: '',
  };

  for (const innerJoin of innerJoins) {
    joinStmt.sql += `INNER JOIN ${this.tableEscaped(
     innerJoin.model,
    )} AS ${this.escapeName(innerJoin.prefix + this.table(innerJoin.model))} `;
    /** @todo Fix ::uuid to be dynamic */
    joinStmt.sql += `ON ${this.columnEscaped(
     innerJoin.parentModel,
     innerJoin.relation.keyFrom,
     true,
     innerJoin.parentPrefix,
    )} = ${this.columnEscaped(
     innerJoin.model,
     innerJoin.relation.keyTo,
     true,
     innerJoin.prefix,
    )} `;
  }

  return {
    joinStmt,
    innerWhere,
    innerOrder,
  };
};

/**
 * Build a list of escaped column names for the given model and fields filter
 * @param {string} model Model name
 * @param {object} filter The filter object
 * @param {boolean} withTable If true prepend the table name (default false)
 * @returns {string} Comma separated string of escaped column names
 */
MySQL.prototype.buildColumnNames = function(model, filter, withTable = false) {
  const fieldsFilter = filter && filter.fields;
  const cols = this.getModelDefinition(model).properties;
  if (!cols) {
    return '*';
  }
  let keys = Object.keys(cols);
  if (Array.isArray(fieldsFilter) && fieldsFilter.length > 0) {
    // Not empty array, including all the fields that are valid properties
    keys = fieldsFilter.filter(function(f) {
      return cols[f];
    });
  } else if ('object' === typeof fieldsFilter &&
   Object.keys(fieldsFilter).length > 0) {
    // { field1: boolean, field2: boolean ... }
    const included = [];
    const excluded = [];
    keys.forEach(function(k) {
      if (fieldsFilter[k]) {
        included.push(k);
      } else if ((k in fieldsFilter) && !fieldsFilter[k]) {
        excluded.push(k);
      }
    });
    if (included.length > 0) {
      keys = included;
    } else if (excluded.length > 0) {
      excluded.forEach(function(e) {
        const index = keys.indexOf(e);
        keys.splice(index, 1);
      });
    }
  }
  const names = keys.map(function(c) {
    return this.columnEscaped(model, c, withTable);
  }, this);
  return names.join(',');
};

/*
 * Overwrite the loopback-connector column escape
 * to allow querying nested json keys
 * @param {String} model The model name
 * @param {String} property The property name
 * @param {boolean} if true prepend the table name (default= false)
 * @param {String} add a prefix to column (for alias)
 * @returns {String} The escaped column name, or column with nested keys for deep json columns
 */
MySQL.prototype.columnEscaped = function(model, property, withTable = false, prefix = '', whereJSON = null) {
  if (_isNested(property)) {
    // Convert column to PostgreSQL json style query: "model"->>'val'
    return property
     .split('.')
     .map(function(val, idx) {
       return idx === 0 ? this.columnEscaped(model, val) : _escapeLiteral(val);
     }, this)
     .reduce(function(prev, next, idx, arr) {
       return idx === 0 ?
        next :
        idx < arr.length - 1 ?
         prev + '->' + next :
         prev + '->>' + next;
     });
  } else {
    let columnName;
    if (withTable) {
      columnName = (
       this.escapeName(prefix + this.table(model)) +
       '.' +
       this.escapeName(this.column(model, property))
      );
    } else {
      columnName = this.escapeName(this.column(model, property));
    }

    if (whereJSON) {
      columnName = whereJSON.replace('?', columnName);
    }

    return columnName;
  }
};

/*!
 * Convert to the Database name
 * @param {String} name The name
 * @returns {String} The converted name
 */
MySQL.prototype.dbName = function(name) {
  if (!name) {
    return name;
  }
  // Set default to lowercase names
  return name.toLowerCase();
};

/**
 * Build a SQL SELECT statement
 * @param {String} model Model name
 * @param {Object} filter Filter object
 * @param {Object} options Options object
 * @returns {ParameterizedSQL} Statement object {sql: ..., params: ...}
 */
MySQL.prototype.buildSelect = function(model, filter, options = {}, count = false) {
  let sortById;

  const sortPolicy = this.getDefaultIdSortPolicy(model);

  switch (sortPolicy) {
    case 'numericIdOnly':
      sortById = this.hasOnlyNumericIds(model);
      break;
    case false:
      sortById = false;
      break;
    default:
      sortById = true;
      break;
  }

  debugSort(model, 'sort policy:', sortPolicy, sortById);

  if (sortById && !filter.order && !count) {
    const idNames = this.idNames(model);
    if (idNames && idNames.length) {
      filter.order = idNames;
    }
  }

  const selectSql = count ?
   'count(' + this.tableEscaped(model) + '.' + this.idNames(model) + ') as "cnt"' :
   this.buildColumnNames(model, filter, true);

  let selectStmt = new ParameterizedSQL(
   'SELECT ' +
   selectSql +
   ' FROM ' +
   this.tableEscaped(model),
  );

  if (filter) {
    if (filter.where || filter.order) {
      const {
        joinStmt,
        innerWhere,
        innerOrder,
      } = this.buildJoin(
       model,
       filter,
      );
      filter.innerWhere = innerWhere || {};
      filter.innerOrder = innerOrder || {};
      selectStmt.merge(joinStmt);
    }
    if (filter.where) {
      const whereStmt = this.buildWhere(model, filter);
      selectStmt.merge(whereStmt);
    }

    if (filter.order) {
      selectStmt.merge(this.buildOrderBy(model, filter));
    }

    if (filter.limit || filter.skip || filter.offset) {
      selectStmt = this.applyPagination(model, selectStmt, filter);
    }
  }
  return this.parameterize(selectStmt);
};

/**
 * Get default find sort policy
 * @param model
 */
MySQL.prototype.getDefaultIdSortPolicy = function(model) {
  const modelClass = this._models[model];

  if (modelClass.settings.hasOwnProperty('defaultIdSort')) {
    return modelClass.settings.defaultIdSort;
  }

  if (this.settings.hasOwnProperty('defaultIdSort')) {
    return this.settings.defaultIdSort;
  }

  return null;
};

/**
 * Check if id types have a numeric type
 * @param {String} model name
 * @returns {Boolean}
 */
MySQL.prototype.hasOnlyNumericIds = function(model) {
  const cols = this.getModelDefinition(model).properties;
  const idNames = this.idNames(model);
  const numericIds = idNames.filter(function(idName) {
    return cols[idName].type === Number;
  });

  return numericIds.length === idNames.length;
};


/**
 * Build the ORDER BY clause
 * @param {string} model Model name
 * @param {string[]} order An array of sorting criteria
 * @returns {string} The ORDER BY clause
 */
MySQL.prototype.buildOrderBy = function(model, filter) {
  const {
    order,
    innerOrder,
  } = filter;
  if (!order) {
    return '';
  }
  let orderArray = order;
  if (typeof order === 'string') {
    orderArray = [order];
  }
  const clauses = [];
  for (let i = 0, n = orderArray.length; i < n; i++) {
    const t = orderArray[i].split(/[\s,]+/);
    let value = false;
    if (t.length === 2) {
      value = true;
    }
    if (innerOrder && innerOrder[t[0]]) {
      const column = this.columnEscaped(
       innerOrder[t[0]].model,
       innerOrder[t[0]].property.key,
       true,
       innerOrder[t[0]].prefix,
      );
      if (value) {
        clauses.push(column + ' ' + t[1]);
      } else {
        clauses.push(column);
      }
    } else {
      const column = this.columnEscaped(model, t[0], true);
      if (value) {
        clauses.push(column + ' ' + t[1]);
      } else {
        clauses.push(column);
      }
    }
  }
  return 'ORDER BY ' + clauses.join(',');
};

/**
 * @private
 * @param model
 * @param filter
 * @returns {ParameterizedSQL}
 */
MySQL.prototype._buildWhere = function(model, filter) {
  const {
    innerWhere,
  } = filter;
  let {
    where,
  } = filter;
  let columnValue, sqlExp;
  let withTable = true;
  if (!where && !innerWhere) {
    withTable = false;
    where = filter;
  }
  if (!filter) {
    return new ParameterizedSQL('');
  }
  if (typeof where !== 'object' || Array.isArray(where)) {
    debug('Invalid value for where: %j', where);
    return new ParameterizedSQL('');
  }
  const self = this;
  const props = self.getModelDefinition(model).properties;
  const modelDefinition = this.getModelDefinition(model);

  const whereStmts = [];
  for (const key in where) {
    let innerJoin = false;
    const stmt = new ParameterizedSQL('', []);
    // Handle and/or operators
    if (key === 'and' || key === 'or') {
      const branches = [];
      let branchParams = [];
      const clauses = where[key];
      if (Array.isArray(clauses)) {
        for (let i = 0, n = clauses.length; i < n; i++) {
          const stmtForClause = self._buildWhere(model, {
            innerWhere,
            where: clauses[i],
          });
          if (stmtForClause.sql) {
            stmtForClause.sql = '(' + stmtForClause.sql + ')';
            branchParams = branchParams.concat(stmtForClause.params);
            branches.push(stmtForClause.sql);
          }
        }
        stmt.merge({
          sql: branches.join(' ' + key.toUpperCase() + ' '),
          params: branchParams,
        });
        whereStmts.push(stmt);
        continue;
      }
      // The value is not an array, fall back to regular fields
    }
    let p = props[key];

    if (Array.isArray(modelDefinition.settings.__hiddenProps__) &&
     modelDefinition.settings.__hiddenProps__.includes(p)) {
      debug('Hidden prop for model %s skipping', model);
      continue;
    }

    if (p == null && _isNested(key)) {
      // See if we are querying nested json
      p = props[key.split('.')[0]];
    }
    // It may be an innerWhere
    if (p == null) {
      if (innerWhere && innerWhere[key]) {
        p = innerWhere[key].property;
      }
      if (p) {
        innerJoin = true;
      }
    }

    if (p == null) {
      // Unknown property, ignore it
      debug('Unknown property %s is skipped for model %s', key, model);
      continue;
    }
    // eslint-disable one-var
    let expression = where[key];
    // Use alias from INNER JOIN AS builder
    const columnName = innerJoin ?
     self.columnEscaped(
      innerWhere[key].model,
      innerWhere[key].property.key,
      true,
      innerWhere[key].prefix,
      innerWhere[key].whereJSON
     ) :
     self.columnEscaped(model, key, withTable);
    // eslint-enable one-var
    if (expression === null || expression === undefined) {
      stmt.merge(columnName + ' IS NULL');
    } else if (expression && expression.constructor === Object) {
      const operator = Object.keys(expression)[0];
      // Get the expression without the operator
      expression = expression[operator];
      if (operator === 'inq' || operator === 'nin' || operator === 'between') {
        columnValue = [];
        if (Array.isArray(expression)) {
          // Column value is a list
          for (let j = 0, m = expression.length; j < m; j++) {
            columnValue.push(this.toColumnValue(p, expression[j], true));
          }
        } else {
          columnValue.push(this.toColumnValue(p, expression, true));
        }
        if (operator === 'between') {
          // BETWEEN v1 AND v2
          const v1 = columnValue[0] === undefined ? null : columnValue[0];
          const v2 = columnValue[1] === undefined ? null : columnValue[1];
          columnValue = [v1, v2];
        } else {
          // IN (v1,v2,v3) or NOT IN (v1,v2,v3)
          if (columnValue.length === 0) {
            if (operator === 'inq') {
              columnValue = [null];
            } else {
              // nin () is true
              continue;
            }
          }
        }
      } else if (operator === 'regexp' && expression instanceof RegExp) {
        // do not coerce RegExp based on property definitions
        columnValue = expression;
      } else {
        columnValue = this.toColumnValue(p, expression, true);
      }
      sqlExp = self.buildExpression(columnName, operator, columnValue, p);
      stmt.merge(sqlExp);
    } else {
      // The expression is the field value, not a condition
      columnValue = self.toColumnValue(p, expression);
      if (columnValue === null) {
        stmt.merge(columnName + ' IS NULL');
      } else {
        if (columnValue instanceof ParameterizedSQL) {
          if (p.type.name === 'GeoPoint')
            stmt.merge(columnName + '~=').merge(columnValue);
          else stmt.merge(columnName + '=').merge(columnValue);
        } else {
          stmt.merge({
            sql: columnName + '=?',
            params: [columnValue],
          });
        }
      }
    }
    whereStmts.push(stmt);
  }
  let params = [];
  const sqls = [];
  for (let k = 0, s = whereStmts.length; k < s; k++) {
    if (whereStmts[k].sql) {
      sqls.push(whereStmts[k].sql);
      params = params.concat(whereStmts[k].params);
    }
  }
  const whereStmt = new ParameterizedSQL({
    sql: sqls.join(' AND '),
    params: params,
  });
  return whereStmt;
};

MySQL.prototype.count = function(model, where, options, cb) {
  if (typeof where === 'function') {
    // Backward compatibility for 1.x style signature:
    // count(model, cb, where)
    const tmp = options;
    cb = where;
    where = tmp;
  }

  const stmt = this.buildSelect(model, {
    where,
  }, options, true);
  this.execute(stmt.sql, stmt.params, options,
   function(err, res) {
     if (err) {
       return cb(err);
     }
     const c = (res && res[0] && res[0].cnt) || 0;
     // Some drivers return count as a string to contain bigint
     // See https://github.com/brianc/node-postgres/pull/427
     cb(err, Number(c));
   });
};

/**
 * Build the SQL WHERE clause for the filter object
 * @param {string} model Model name
 * @param {object} filter An object for the filter conditions
 * @returns {ParameterizedSQL} The SQL WHERE clause
 */
MySQL.prototype.buildWhere = function(model, filter) {
  const whereClause = this._buildWhere(model, filter);
  if (whereClause.sql) {
    whereClause.sql = 'WHERE ' + whereClause.sql;
  }
  return whereClause;
};

/*
 * Check if a value is attempting to use nested json keys
 * @param {String} property The property being queried from where clause
 * @returns {Boolean} True of the property contains dots for nested json
 */
function _isNested(property) {
  return property.split('.').length > 1;
}

function _escapeLiteral(str) {
  let hasBackslash = false;
  let escaped = "'";
  for (let i = 0; i < str.length; i++) {
    const c = str[i];
    if (c === "'") {
      escaped += c + c;
    } else if (c === '\\') {
      escaped += c + c;
      hasBackslash = true;
    } else {
      escaped += c;
    }
  }
  escaped += "'";
  if (hasBackslash === true) {
    escaped = ' E' + escaped;
  }
  return escaped;
}

require('./migration')(MySQL, mysql);
require('./discovery')(MySQL, mysql);
require('./transaction')(MySQL, mysql);
