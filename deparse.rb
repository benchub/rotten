#!/usr/bin/env ruby
# encoding: utf-8

require 'pg_query'
require 'json'

lines = STDIN.read

q = PgQuery.new("", JSON.parse(lines), [])

print q.deparse
