library(redditsuite)
library(dplyr)
library(dbplyr)

# Connect to the database
pg_connection <- DBI::dbConnect(
  drv = RPostgres::Postgres(),
  host = 'localhost',
  port = 5432,
  user = Sys.getenv("POSTGRES_USER"),
  password = Sys.getenv('POSTGRES_PASSWORD'),
  dbname = Sys.getenv('POSTGRES_DB')
)

# Create an empty table
DBI::dbCreateTable(conn = pg_connection,
                   name = 'mtcars',
                   fields = mtcars)
# the type was inferred from the data that was passed

DBI::dbListTables(pg_connection)

# Add data into the table
DBI::dbAppendTable(conn = pg_connection,
                   name = 'mtcars',
                   value = mtcars)

# Remove table
# DBI::dbRemoveTable(conn = pg_connection,
#                    name = 'mtcars')

# Way of interacting with dplyr to the database backend
# this writes sql code behind the scenes
mtcars_connection <- tbl(pg_connection, in_schema('public', 'mtcars'))

mtcars_modified <- mtcars_connection %>%
  mutate(new_column = gear + carb) %>%
  collect()

# show_query()

DBI::dbRemoveTable(conn = pg_connection,
                   name = 'mtcars')

DBI::dbCreateTable(conn = pg_connection,
                   name = 'mtcars',
                   fields = mtcars_modified)

DBI::dbAppendTable(conn = pg_connection,
                   name = 'mtcars',
                   value = mtcars_modified)

DBI::dbRemoveTable(conn = pg_connection,
                   name = 'mtcars')

DBI::dbListTables(conn = pg_connection)

