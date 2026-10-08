-- +goose Up
-- Workers from task 20261007-120000-5 on ship each context's own time
-- (pssc execution time scaled to events.time), which the server stores as attributed_time. Only the column comment
-- changes; no data or grants do.
set local search_path to rotten, public;

comment on column event_context.attributed_time is 'This context''s time in ms, as the worker shipped it (QueryContext.time): pssc execution time scaled so the event''s contexts sum to events.time. For rows from older workers, the proportional estimate events.time * c / the sum of c over the event''s contexts. Rows with all three IDs NULL are the untagged context.';

-- +goose Down
set local search_path to rotten, public;

comment on column event_context.attributed_time is 'This context''s share of the event''s time: events.time * c / the sum of c over the event''s contexts.';
