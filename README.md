# LINZ Bulk Data Extract Uploader

linz_bde_loader is a programme for loading LINZ BDE files into a PostgreSQL
database. linz_bde_uploader has the ability to load full and incremental table
Landonline BDE loads, as well as manage versioning information.

Copyright 2011 Crown copyright (c) Land Information New Zealand and the New
Zealand Government.

## Requirements

* Perl 5.88 or greater, plus
    - DBD::Pg
    - Date::Calc
    - File::Which
    - LINZ::Config
    - LINZ::Bde
    - Log::Log4perl
    - Log::Dispatch
    - Log::Dispatch::FileRotate
    - Log::Dispatch::Email::MailSender
    - Try::Tiny
    
* PostgreSQL 9.0 or greater, plus
    - Postgis 2.X
* linz_bde_copy, plus
    - zlib library for building

## Compatibility

Tested on Ubuntu 10.04 and 14.04

## Install

Use the Debian packaging within the debian/ director to install this software on Ubuntu.

### Manual install

The main components to install are the perl packages, PostgreSQL database
setup and config script and the actual linz_bde_uploader programme and
configuration. The build install process handles this, however setup of the
PostgreSQL user account, database, logging directory and configuration setup need
to be done as manual task afterward.

### Simple install

    perl Build.PL
    ./Build install
    
### Advanced install options

The build system is using perl Module::Build. A full list of the building
options are available run:

    ./Build help
    
A more complex example involving specific install directories could something
like:

    perl Build.PL --prefix=/usr/local
    ./Build install
or:

    perl Build.PL \
        --prefix=/usr \
        --install_path conf=/my/conf/dir \
        --install_path sql=/my/sql/dir

### Setup of PostgreSQL

A UTF8 database called bde_db needs to be created for the management of the BDE
data. This can be done using the following command:

    createdb -O postgres -E UTF8 -T template0 -l C bde_db
    
Also the perl and perlu procedural languages are required:
    
    createlang plperl bde_db
    createlang plperlu bde_db
    
Next PostGIS is required to be installed into the database. 

    psql -d bde_db -c "CREATE EXTENSION postgis"

Next the database needs to be configured for uploading BDE data. This is setup
with the following series of scripts:

    psql -d bde_db -f sql/bde_roles.sql
    psql -d bde_db -f sql/bde_schema.sql
    psql -d bde_db -f sql/bde_schema_index.sql
    psql -d bde_db -f sql/bde_functions.sql
    psql -d bde_db -f sql/table_version_tables.sql
    psql -d bde_db -f sql/table_version_functions.sql
    psql -d bde_db -f sql/bde_control_tables.sql
    psql -d bde_db -f sql/bde_control_functions.sql

Also if you want to enable versioning on the BDE tables the following scripts
need to be run:

    psql -d bde_db -f sql/lds_layer_tables.sql
    psql -d bde_db -f sql/lds_layer_functions.sql

Lastly the PostgreSQL user account for linz_bde_uploader needs to be created. On
LINUX it is best to first create a system user account as well so ident
authenication can be used.

    adduser --system --gecos "LDS BDE Maintainer" lds_bde
    
The lds_bde PostgreSQL user account needs to have bde_dba rights, but does not
need to have superuser rights by default. An example SQL create user script
could look like:
    
    DO $$
    BEGIN
    
    IF NOT EXISTS (SELECT * FROM pg_roles where rolname = 'lds_bde') THEN
        CREATE ROLE lds_bde LOGIN
              NOSUPERUSER INHERIT NOCREATEDB NOCREATEROLE;
        ALTER ROLE lds_bde SET search_path=bde, bde_control, lds, public;
        GRANT bde_dba TO lds_bde;
    END IF;
    
    END
    $$;

## Configuration

All parameters to setup and running of linz_bde_uploader can be found within the
template conf file in conf/linz_bde_uploader.conf. Another important file is
tables.conf which lists the BDE tables and their associated loading parameters.
On Ubuntu using the debian packaging these files are installed into /etc/linz-bde-uploader/

**Note:** You can create **.conf.test* configuration file (e.g linz_bde_uploader.conf.test)
which can override any of the parameters in the main config file. This can be used as a
convenient way to set parameters without having to change the installed default config

Key parameters to change in the linz_bde_uploader.conf config are:

* **bde_repository**: Set the path to the directory of BDE unload files.
This directory should have a level_0 and level_5 subdirectory with child folders
in each with the naming convention of YYYYMMDDhhmmss for each dataset

* **tmp_base_dir**: This temp processing directory for uncompressing and
pre-processing BDE datafile. This directory should have at 15GB of free
space for large tables such as crs_adj_obs_change

* **include_tables**: A list of table to load for the run. These table must exist
in the in the file as defined by bde_tables_config (tables.conf by defualt)

* **smtpserver** and **smtpsender**: The mail host and sender email address to send
completion or error reports to.

* **log_email_address**: The email address ro addressed that the mail server will
send the completion or error reports to. multiple emails can be listing with
a "," separator.
    
## Running linz_bde_uploader

A simple example to upload all available bde files into the database is:
    
    linz_bde_uploader -verbose -listing /var/log/linz-bde-uploader/linz_bde_loader.log

For more help about options for running linz_bde_uploader can be made available
through the following command:
    
    linz_bde_uploader -help

## License

This project is under 3-clause BSD License, except where otherwise specified. See the LICENSE file for more details.
