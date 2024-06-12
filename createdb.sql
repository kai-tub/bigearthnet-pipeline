-- Call with psql --variable=dbname=DNAME --variable=db_opts="ENCODING=LATIN1"
-- Compared to `createdb` this allows for custom messages/logic
-- if database already exists

\set ON_ERROR_STOP on

\if :{?dbname} \else
    \warn 'You have to set dbname!'
    -- use invalid command to trigger non-zero exit code
    dbname not set;
\endif

SELECT 
    -- Store the output of the query inside of `:db_exists`
    -- Note the use of :'' to ensure that the dbname is escaped as literal
    EXISTS(SELECT 1 FROM pg_database WHERE datname = :'dbname') as db_exists
\gset
\if :db_exists
    \echo 'Database' :'dbname' 'already exists. Doing nothing'
\else
    -- note that db_opts is not escaped for maximum flexibility!
    -- and that :"dbname" escapes the name as identifier!
    CREATE DATABASE :"dbname" \if :{?db_opts}
        :db_opts
    \endif
\endif

