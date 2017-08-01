const express = require('express')
const session = require('express-session')
const CassandraStore = require('./index')

const app = express()

app.use(session({
    store: new CassandraStore({}),
    secret: 'keyboard',
    cookie: {},
    resave: true,
    saveUninitialized: true,
}))

app.get('/', (req, res, next) => {
    return res.json({})
})

app.listen(3000, function() {
    console.log('Example app listening on port 3000!')
})

module.exports = app
