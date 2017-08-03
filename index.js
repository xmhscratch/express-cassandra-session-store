/*!
 * express-cassandra-session-store
 * MIT Licensed
 */

'use strict'

/**
 * Module dependencies.
 * @private
 */

const fs = require('fs')
const util = require('util')

const debug = util.debuglog('express-cassandra-session-store')
const { Store } = require('express-session')
const async = require('async')
const cassandra = require('cassandra-driver')

/**
 * Shim setImmediate for node.js < 0.10
 * @private
 */

/* istanbul ignore next */
var defer = typeof setImmediate === 'function' ?
    setImmediate :
    function(fn) { process.nextTick(fn.bind.apply(fn, arguments)) }

/**
 * Module exports.
 */

module.exports = CassandraStore

/**
 * A session store in cassandra.
 * @public
 */

function CassandraStore(options) {
    Store.call(this)

    options = (options || {})

    const clientOptions = Object.assign({
        contactPoints: ['127.0.0.1'],
        queryOptions: { prepare: true }
    }, options.clientOptions || {})

    options = Object.assign({
        keyspace: 'session',
        tableName: 'clients',
        client: null,
        clientOptions: clientOptions
    }, options)

    this.sessions = {}

    const cqlsh = new cassandra.Client(clientOptions)

    const tableName = options.tableName || 'clients'
    const defaultKeyspace = options.keyspace || 'session'

    cqlsh.connect((error) => {
        if (error) {
            debug(error)
        }

        async.series([
            (callback) => cqlsh.execute(
                `CREATE KEYSPACE IF NOT EXISTS ${defaultKeyspace}
                    WITH replication = {
                        'class': 'SimpleStrategy',
                        'replication_factor': '1'
                    }
                    AND durable_writes = true
                ;`, [], { prepare: true }
            ).then((results) => {
                return callback(null, results)
            }).catch(callback),

            (callback) => cqlsh.execute(
                `CREATE TABLE IF NOT EXISTS ${defaultKeyspace}.${tableName} (
                        sid text,
                        session text,
                        PRIMARY KEY(sid)
                    ) WITH default_time_to_live = 3600
                ;`, [], { prepare: true }
            ).then((results) => {
                return callback(null, results)
            }).catch(callback),

            (callback) => cqlsh.execute(
                `USE ${defaultKeyspace};`, [], { prepare: true }
            ).then((results) => {
                return callback(null, results)
            }).catch(callback),
        ], (error, results) => {
            if (error) {
                debug(error)
            }
        })
    })

    cqlsh.on('log', (level, className, message, furtherInfo) => {
        debug(`${className} [${level}]: ${message} (${furtherInfo})`)
    })

    this.cqlsh = cqlsh
    this.tableName = tableName
    this.defaultKeyspace = defaultKeyspace
}

/**
 * Inherit from Store.
 */

util.inherits(CassandraStore, Store)

/**
 * Clear all sessions.
 *
 * @param {function} callback
 * @public
 */

CassandraStore.prototype.clear = function clear(callback) {
    const {
        cqlsh,
        defaultKeyspace,
        tableName
    } = this

    cqlsh.execute(`
        TRUNCATE ${defaultKeyspace}.${tableName};`,
        []
    ).then((results) => {
        callback && defer(callback)
    })
}

/**
 * Destroy the session associated with the given session ID.
 *
 * @param {string} sessionId
 * @public
 */

CassandraStore.prototype.destroy = function destroy(sessionId, callback) {
    destroySession.call(this, sessionId, callback)
}

/**
 * Fetch session by the given session ID.
 *
 * @param {string} sessionId
 * @param {function} callback
 * @public
 */

CassandraStore.prototype.get = function get(sessionId, callback) {
    const {
        cqlsh,
        defaultKeyspace,
        tableName
    } = this

    getSession.call(this, sessionId, (error, session) => {
        callback && defer(callback, error, session)
    })
}

/**
 * Commit the given session associated with the given sessionId to the store.
 *
 * @param {string} sessionId
 * @param {object} session
 * @param {function} callback
 * @public
 */

CassandraStore.prototype.set = function set(sessionId, session, callback) {
    upsertSession.call(this, sessionId, session, callback)
}

/**
 * Get number of active sessions.
 *
 * @param {function} callback
 * @public
 */

CassandraStore.prototype.length = function length(callback) {
    const {
        cqlsh,
        defaultKeyspace,
        tableName
    } = this

    cqlsh.execute(`
        SELECT
            COUNT(sid) AS count
        FROM
            ${defaultKeyspace}.${tableName};`,
        []
    ).then((results) => {
        const { count } = results.rows[0]

        callback && defer(() => callback(null, count))
    }).catch((error) => {
        callback && defer(callback, error)
    })
}

/**
 * Touch the given session object associated with the given session ID.
 *
 * @param {string} sessionId
 * @param {object} session
 * @param {function} callback
 * @public
 */

CassandraStore.prototype.touch = function touch(sessionId, session, callback) {
    getSession.call(this, sessionId, (error, currentSession) => {
        if (error) {
            callback && defer(callback, error)
            return
        }

        if (currentSession) {
            // update expiration
            currentSession.cookie = session.cookie
            upsertSession.call(this, sessionId, currentSession, callback)
            return
        }

        callback && defer(callback)
    })
}

/**
 * Get session from the store.
 * @private
 */

function destroySession(sessionId, callback) {
    const {
        cqlsh,
        defaultKeyspace,
        tableName
    } = this

    cqlsh.execute(`
        DELETE FROM ${defaultKeyspace}.${tableName} WHERE sid = ?;`,
        [sessionId]
    ).then((results) => {
        callback && defer(callback)
    }).catch((error) => {
        callback && defer(callback, error)
    })
}

/**
 * Get session from the store.
 * @private
 */

function upsertSession(sessionId, session, callback) {
    const {
        cqlsh,
        defaultKeyspace,
        tableName
    } = this

    cqlsh.execute(`
        INSERT INTO
            ${defaultKeyspace}.${tableName} (
                sid, session
            ) VALUES (?, ?);
        ;`, [sessionId, JSON.stringify(session)]
    ).then((results) => {
        callback && defer(callback)
    }).catch((error) => {
        callback && defer(callback, error)
    })
}

/**
 * Get session from the store.
 * @private
 */

function getSession(sessionId, callback) {
    const {
        cqlsh,
        defaultKeyspace,
        tableName
    } = this

    cqlsh.execute(`
        SELECT * FROM ${defaultKeyspace}.${tableName} WHERE sid = ?;`,
        [sessionId]
    ).then((results) => {
        let record = results.rows[0]

        if (!record) {
            callback && defer(callback)
            return
        }

        let { session } = record

        if (!session) {
            callback && defer(callback)
            return
        }

        // parse
        session = JSON.parse(session)

        let expires = typeof session.cookie.expires === 'string'
            ? new Date(session.cookie.expires)
            : session.cookie.expires

        // destroy expired session
        if (expires && expires <= Date.now()) {
            destroySession.call(this, sessionId, callback)
            return
        }

        callback && defer(callback, null, session)
    }).catch((error) => {
        callback && defer(callback, error)
    })
}
