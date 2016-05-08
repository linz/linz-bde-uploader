# LINZ Bulk Data Extract Uploader

linz_bde_uploader is a programme for loading LINZ BDE files into a PostgreSQL
database. linz_bde_uploader has the ability to load full and incremental table
Landonline BDE loads, as well as manage versioning information.

## Requirements

* PostgreSQL 9.3 or greater
* [postgresql-tableversion](https://github.com/linz/postgresql-tableversion)
* [linz-bde-schema](https://github.com/linz/linz-bde-schema)
* Perl 5.88 or greater, plus
    - DBD::Pg
    - Date::Calc
    - File::Which
    - [LINZ::Config](https://github.com/linz/linz_utils_perl)
    - [LINZ::Bde](https://github.com/linz/linz_bde_perl)
    - IO::Zlib
    - Log::Log4perl
    - Log::Dispatch
    - Log::Dispatch::FileRotate
    - Log::Dispatch::Email::MailSender
    - Try::Tiny

## Compatibility

Tested on Ubuntu 14.04

## Install

Use the Debian packaging within the debian/ directory to install this software on Ubuntu.

### Manual install

The main components to install are the perl packages, PostgreSQL database
setup and config script and the actual linz_bde_uploader programme and
configuration. The build install process handles this, however setup of the
PostgreSQL user account, database, logging directory and configuration setup need
to be done as manual task afterward.

### Simple install

```shell
perl Build.PL
./Build install
```

### Advanced install options

The build system is using Perl Module::Build. A full list of the building
options are available run:

```shell
./Build help
```

A more complex example involving specific install directories could something
like:

```shell
perl Build.PL --prefix=/usr
./Build install
```

or:

```shell
perl Build.PL \
    --prefix=/usr \
    --install_path conf=/my/conf/dir \
    --install_path sql=/my/sql/dir
```

### Setup under *UNIX

A UTF8 database needs to be created for the management of the BDE data.
This can be done following the instructions in
[linz-bde-schema](https://github.com/linz/linz-bde-schema)

If you haven't already installed the PostgreSQL table_version extension then do
so:

```shell
psql $DB_NAME -c "CREATE EXTENSION IF NOT EXISTS table_version"
```

You can then execute the installed linz-bde-uploader SQL support files with
something like:

```shell
for file in /usr/share/linz-bde-uploader/sql/*.sql
    do psql $DB_NAME -f $file -v ON_ERROR_STOP=1
done
```

Lastly the linz_bde_uploader software should be run under a UNIX account that
also has PostgreSQL ``bde_dba`` database access. It is best to first create a
system user account as well so ident authentication can be used.

```shell
adduser --system --gecos "BDE Maintainer" bde
```

The bde PostgreSQL user account needs to have bde_dba rights, but does not
need to have superuser rights by default. An example SQL create user script
could look like:

```sql
DO $$
BEGIN
    IF NOT EXISTS (SELECT * FROM pg_roles where rolname = 'bde') THEN
        CREATE ROLE bde LOGIN
              NOSUPERUSER INHERIT NOCREATEDB NOCREATEROLE;
        ALTER ROLE bde SET search_path=bde, bde_control, lds, public;
        GRANT bde_dba TO bde;
    END IF;
END
$$;
```
Also creating a standard system log directory in /var/log would be a good idea:

```shell
BDE_LOG_DIR=/var/log/linz-bde-uploader
BDE_USER=bde
sudo mkdir -p $BDE_LOG_DIR
sudo chown -R $BDE_USER:adm $BDE_LOG_DIR;
sudo chmod 0755 $BDE_LOG_DIR;
```

## linz_bde_uploader Configuration

All parameters to setup and running of linz_bde_uploader can be found within the
template conf file in conf/linz_bde_uploader.conf. Another important file is
tables.conf which lists the BDE tables and their associated loading parameters.
On Ubuntu using the debian packaging these files are installed into /etc/linz-bde-uploader/

**Note:** You can create **.conf.test* configuration file (e.g linz_bde_uploader.conf.test)
which can override any of the parameters in the main config file. This can be used as a
convenient way to set parameters without having to change the installed default config

Key parameters to change in the linz_bde_uploader.conf config are:

* ``db_connection``: The PostgreSQL connection string for setting the database
connection string. e.g dbname=linz_db

* ``bde_repository``: Set the path to the directory of BDE unload files.
This directory should have a level_0 and level_5 subdirectory with child folders
in each with the naming convention of YYYYMMDDhhmmss for each dataset

* ``tmp_base_dir``: This temp processing directory for uncompressing and
pre-processing BDE datafile. This directory should have at 15GB of free
space for large production tables such as crs_adj_obs_change

* ``include_tables``: A list of table to load for the run. These table must exist
in the in the file as defined by bde_tables_config (tables.conf by default)

* ``log4perl.logger``: Setting the loggers to use. By default the ErrorEmail and
Email loggers are on. If you don't want to use a email reporting then remove these
loggers.

* ``smtpserver`` and ``smtpsender``: The mail host and sender email address to send
completion or error reports to if email logging is enabled.

* ``log_email_address``: The email address that the mail server will send the
completion or error reports to. multiple emails can be listing with a "," separator.
    
## Running linz_bde_uploader

A simple example to upload latest available full set of bde files into the
database:

```shell
linz_bde_uploader -full -verbose -config-path /etc/linz-bde-uploader/linz_bde_uploader.conf -listing /var/log/linz-bde-uploader/linz_bde_uploader.log
```

There is also the wrapper shell script ``run_bde_upload`` that runs
linz_bde_uploader with the config and log directory already defined:

```shell
run_bde_upload -full -verbose
```

### Normal run
The normal mode to run the software is to load any available level 0 or 5
dataset files available. This can be done via:

```shell
run_bde_upload -full-incremental -incremental
```

**Note:** In this mode the -full-incremental option is used rather than the -full
options which updates the current dataset instead of a full replace.

### Help
For more help about options for running linz_bde_uploader can be made available
through the following command:

```shell
linz_bde_uploader -help
```

## License

Copyright 2016 Crown copyright (c) Land Information New Zealand and the New
Zealand Government.

This project is under 3-clause BSD License, except where otherwise specified.
See the LICENSE file for more details.
