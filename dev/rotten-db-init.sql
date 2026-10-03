CREATE EXTENSION IF NOT EXISTS pg_partman SCHEMA public;

CREATE ROLE rotten_owner LOGIN PASSWORD 'rotten_owner';
CREATE ROLE rotten_ingest LOGIN PASSWORD 'rotten_ingest';
CREATE ROLE rotten_ui LOGIN PASSWORD 'rotten_ui';
CREATE ROLE rotten_readonly LOGIN PASSWORD 'rotten_readonly';

ALTER DATABASE rotten OWNER TO rotten_owner;
GRANT ALL ON ALL TABLES IN SCHEMA public TO rotten_owner;
GRANT ALL ON ALL SEQUENCES IN SCHEMA public TO rotten_owner;
GRANT EXECUTE ON ALL FUNCTIONS IN SCHEMA public TO rotten_owner;
GRANT EXECUTE ON ALL PROCEDURES IN SCHEMA public TO rotten_owner;
