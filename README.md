rotten
======
Requests Over Time, Tracked Easily Now is a project written to help optimize your app
by letting you know which queries it is murdering your DB with. A reasonable person
might ask, "Isn't this why we have pgBadger?" to which I would say that while 
pgBadger provides excellent analytics in a usable format, 

1. it requires full logging to not be misleading
2. full logging is problematic when you're logging to a remote server for compliance 
   reasons, and also approaching the packets/sec limits of your hardware _before_ 
   enabling full logging.
3. pgBadger reports are generated once for a static set of queries. If you want to 
   drill in on a subset of data, that doesn't work.

rotten attemps to address these things. We do so by frequently looking at the helpful
data gathered by pg_stat_statements, and then resetting those statements to get a fresh
sample for the next interation. This has shortcomings, but it gets us 95% of the 
usefulness of logging all queries in order to see which are problematic, without any of
the firehose problems that might bring.

Instead of recording our analysis to text files, like pgBadger does, we just store the 
data in an additional database and punt on any form of UI. Yes, that's a bit of a cop
out, but it also lets us employ the power of SQL to filter our data pull whatever report
we might want, for whatever time period we might want. 


Assumptions
===========
As a young project rotten makes a lot of assumptions. Among them:

1. You are using pg_stat_statements and have no qualms frequently resetting them.
2. You are ok with not getting everything from pg_stat_statements, but rather the "most"
   interesting queries. Getting *everything* is orders of magnitude too expensive to be
   useful on busy systems, and this project is only trying to find the worst offenders, 
   anyway.
3. You are ok with a SQL prompt as a UI for now.
4. You will have a role on your monitored dbs called "rotten-observer" and it will call
   some security definer functions (to query and reset pg_stat_statments in a "dba" schema.
5. Your monitored databases have distinct identifiers of some kind (fqdn, IP, etc) as well
   as some logical identification ("the master production server").

How to use it
=============
1. Get and install Go. http://www.golang.org
2. After setting up your $GOPATH, get the code:
  go get github.com/tj/go-pg-escape
  go get github.com/benchub/rotten
3. go build github.com/benchub/rotten
4. Import schema/tables.sql into the rotten db. Also create a "rotten-client" role that 
   will use these tables.
5. Install the "rotten-observer" role in your monitored databases and the sql/function.sql
   in the "dba" schema on those databases. 
6. Unless you like to be webscale with tmux, script up some systemd services to run rotten.
7. Modify the conf to fit your environment.
  1. RottenDBConn and ObservedDBConn are hopefully self-explanitory, BUT note that if you
     are using pgbouncer in transaction pooling between rotten and your rotten db, you are
     going to need to use the nifty undocumented binary_parameters=yes parameter in order
     to keep go's pg library from using named queries for a single one-liner. 
  2. StatusInterval is how often to report status (in seconds) to stdout.
  3. ObservationInterval is how long (in seconds) to let pg_stat_statements gather info 
     for. This is the most granular you can make your reports, and the lower you set this,
     the more data you will need to store in your rotten db.
  4. FQDN is some unique string (typically the FQDN of the observed db) to help find a 
     physical log if more information is desired other than the fingerprint.
  5. Project, Environment, Cluster, and Role are logical identifiers for where the samples
     of data are coming from. 
8. Install the awful deparse.rb
  1. Put it into /usr/local/bin (or change the code if you want it elsewhere)
  2. Install the pg_query gem to let it work

Known Issues
============
- The code is ugly.
- pg_query, which we use to normalize queries, has a bug that keeps us from being able to parse
  some things: https://github.com/lfittl/pg_query/issues/86
- pg_query's go library doesn't let us reconstitute a parse tree into a query like the ruby one
  does, so we have a horibly non-performant work around.

TODO
====
Um yeah quite a bit.

- pull out all the stats stuff to a module
- make a UI
- allow for arbitrary logical source descriptions, not just Project/Environment/Cluster/Role
- allow for arbitrary locations of the functions in the observed db
- allow for an arbitrary observer role name other than "rotten-observer"
- allow the observation window to adjust size as needed for processing
- allow for a worker pool of reparse executions to speed things up in wall time
- extend pg_query's go library to include deparse natively, so we don't have to use the ugly depase.rb script.
- configurable context, instead of the hardcoded controller/action/job_tag, with their hard-coded regexes
- keep a log of the queries we can't parse
- While we make an effort to normalize cursors and temp tables, those regexs should probably not be hardcoded.