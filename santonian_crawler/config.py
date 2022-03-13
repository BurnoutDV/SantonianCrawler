#!/usr/bin/env python
# coding: utf-8

api_calls = {
    "endpoint": "https://santonianindustries.com/backend",
    "readfile": "readFile",
    "hdd_details": "hdd_details",
    "hdd": "hdd",
    "file": "file"
}

req_retries = 5
req_wait = 2.5
_PREFIX = ""

# database definition, don't change if you don't know what you are doing

SHM = {}
SHM['folders'] = f"""
                    CREATE TABLE IF NOT EXISTS {_PREFIX}folders (
                        uid INTEGER PRIMARY KEY AUTOINCREMENT,
                        name TEXT UNIQUE NOT NULL,
                        file_id TEXT UNIQUE NOT NULL,
                        temporary INT NOT NULL CHECK (temporary in (0, 1)) DEFAULT 0,
                        last_check TIMESTAMP,
                        first_entry TIMESTAMP
                    );"""
SHM['log'] = f"""
                CREATE TABLE IF NOT EXISTS {_PREFIX}log (
                    uid INTEGER PRIMARY KEY AUTOINCREMENT,
                    name TEXT NOT NULL,
                    folder INT REFERENCES {_PREFIX}folders(uid),
                    content TEXT,
                    audio INT NOT NULL CHECK (audio in (0, 1)) DEFAULT 0,
                    aud_fl BLOB,
                    hash TEXT NOT NULL,
                    revision INT NOT NULL,
                    last_check TIMESTAMP NOT NULL,
                    first_entry TIMESTAMP NOT NULL
                );"""
SHM['stats'] = f"""
                CREATE TABLE IF NOT EXISTS {_PREFIX}stats (
                    uid INTEGER PRIMARY KEY AUTOINCREMENT,
                    property TEXT UNIQUE,
                    value TEXT
                );"""
SHM['tags'] = f"""
                CREATE TABLE IF NOT EXISTS {_PREFIX}tag (
                    uid INTEGER PRIMARY KEY AUTOINCREMENT,
                    name TEXT UNIQUE,
                    type TEXT
                    );"""
SHM['tag_link'] = f"""
                CREATE TABLE IF NOT EXISTS {_PREFIX}tag_link (
                    uid INTEGER PRIMARY KEY AUTOINCREMENT,
                    log TEXT REFERENCES {_PREFIX}log(name),
                    tag INTEGER REFERENCES {_PREFIX}tag(uid),
                    changed TIMESTAMP NOT NULL,
                );"""
SHM['insert1'] = f"""
                INSERT INTO {_PREFIX}stats
                (property, value)
                VALUES ('schema_version', '1.0.0')
                """

