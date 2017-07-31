DROP KEYSPACE IF EXISTS session;

CREATE KEYSPACE session WITH replication = {
    'class': 'SimpleStrategy',
    'replication_factor': '1'
};

CREATE TABLE session.clients (
    sid text,
    session text,
    expires timestamp,
    PRIMARY KEY(sid)
) WITH default_time_to_live = 3600;
