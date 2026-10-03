CREATE EXTENSION IF NOT EXISTS pg_stat_statements;
CREATE ROLE rotten_observer LOGIN PASSWORD 'rotten_observer';
GRANT pg_read_all_stats TO rotten_observer;
