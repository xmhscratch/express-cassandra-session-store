/*!
 * express-cassandra-session-store
 * MIT Licensed
 */

'use strict'

/**
 * Module dependencies.
 * @private
 */

const util = require('util')
const debug = util.debuglog('cassandra-store')
const { Store } = require('express-session')

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
 * A session store in memory.
 * @public
 */

function CassandraStore(options) {
    Store.call(this)

    options = Object.assign({
        contactPoints: ['127.0.0.1'],
        keyspace: 'sessions',
        queryOptions: { prepare: true }
    }, options || {})

    this.options = options
    this.cqlsh = new cassandra.Client({
        contactPoints: options.contactPoints,
        keyspace: keyspace,
        ...options
    })
    this.cqlsh.connect((error) => {
        if (error) {
            throw error
        }
    })
    this.cqlsh.on('log', (level, className, message, furtherInfo) => {
        debug(`${className} [${level}]: ${message} (${furtherInfo})`)
    })
}

/**
 * Inherit from Store.
 */

util.inherits(CassandraStore, Store)

/**
 * Get all active sessions.
 *
 * @param {function} callback
 * @public
 */

CassandraStore.prototype.all = function all(callback) {
    var sessionIds = Object.keys(this.sessions)
    var sessions = Object.create(null)

    for (var i = 0; i < sessionIds.length; i++) {
        var sessionId = sessionIds[i]
        var session = getSession.call(this, sessionId)

        if (session) {
            sessions[sessionId] = session
        }
    }

    callback && defer(callback, null, sessions)
}

/**
 * Clear all sessions.
 *
 * @param {function} callback
 * @public
 */

CassandraStore.prototype.clear = function clear(callback) {
    this.sessions = Object.create(null)
    callback && defer(callback)
}

/**
 * Destroy the session associated with the given session ID.
 *
 * @param {string} sessionId
 * @public
 */

CassandraStore.prototype.destroy = function destroy(sessionId, callback) {
    delete this.sessions[sessionId]
    callback && defer(callback)
}

/**
 * Fetch session by the given session ID.
 *
 * @param {string} sessionId
 * @param {function} callback
 * @public
 */

CassandraStore.prototype.get = function get(sessionId, callback) {
    defer(callback, null, getSession.call(this, sessionId))
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
    this.sessions[sessionId] = JSON.stringify(session)
    callback && defer(callback)
}

/**
 * Get number of active sessions.
 *
 * @param {function} callback
 * @public
 */

CassandraStore.prototype.length = function length(callback) {
    this.all(function(err, sessions) {
        if (err) return callback(err)
        callback(null, Object.keys(sessions).length)
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
    var currentSession = getSession.call(this, sessionId)

    if (currentSession) {
        // update expiration
        currentSession.cookie = session.cookie
        this.sessions[sessionId] = JSON.stringify(currentSession)
    }

    callback && defer(callback)
}

/**
 * Get session from the store.
 * @private
 */

function getSession(sessionId) {
    var sess = this.sessions[sessionId]

    if (!sess) {
        return
    }

    // parse
    sess = JSON.parse(sess)

    var expires = typeof sess.cookie.expires === 'string' ?
        new Date(sess.cookie.expires) :
        sess.cookie.expires

    // destroy expired session
    if (expires && expires <= Date.now()) {
        delete this.sessions[sessionId]
        return
    }

    return sess
}
