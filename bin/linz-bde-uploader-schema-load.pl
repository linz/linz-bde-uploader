#!/usr/bin/env perl
################################################################################
#
# linz-bde-uploader-schema-load.pl -  LINZ BDE uploader / schema loader
#
# Copyright 2017 Crown copyright (c)
# Land Information New Zealand and the New Zealand Government.
# All rights reserved
#
# This program is released under the terms of the new BSD license. See the
# LICENSE file for more information.
#
################################################################################
# Script to load support schema in PostgreSQL databases to use for
# loading BDE files into.
################################################################################
use strict;
use warnings;
use Getopt::Long;

our $PREFIX = '@@PREFIX@@';
our $SCRIPTSDIR="${PREFIX}/share/linz-bde-uploader/sql/";
our $PSQL="psql -tA --set ON_ERROR_STOP";

if ( defined( $ENV{'BDEUPLOADER_SQLDIR'} ) ) {
    $SCRIPTSDIR=$ENV{'BDEUPLOADER_SQLDIR'};
}

our $DB_NAME;
our $EXTENSION_MODE = 1;
our $SHOW_VERSION = 0;

sub help
{
    my ($exitcode) = @_;
    print STDERR "Usage: $0 [--noextension] <database>\n";
    print STDERR "       $0 --version\n";
    exit $exitcode;
}

GetOptions (
    "extension!" => \$EXTENSION_MODE,
    "version!" => \$SHOW_VERSION
) || help(0);

$DB_NAME=$ARGV[0];

if ( $SHOW_VERSION )
{
    print "@@VERSION@@ @@REVISION@@";
    exit 0;
}
help(1) if ( ! $DB_NAME );

if ( ! -f "${SCRIPTSDIR}/01-bde_control_tables.sql" ) {
    die "Cannot find 01-bde_control_tables.sql in ${SCRIPTSDIR}\n"
      . "Please set BDEUPLOADER_SQLDIR environment variable\n";
}

$ENV{'PGDATABASE'}=$DB_NAME;

print STDERR "Loading DBE uploader schema in database "
    . $ENV{'PGDATABASE'} . " (extension mode "
    . ( ${EXTENSION_MODE} ?  "on" : "off" )
    . ")\n";

if ( ${EXTENSION_MODE} )
{
    `$PSQL -c 'CREATE EXTENSION IF NOT EXISTS table_version;'` || die;
    `$PSQL -c 'CREATE SCHEMA IF NOT EXISTS _patches;'` || die;
    `$PSQL -c 'CREATE EXTENSION IF NOT EXISTS dbpatch SCHEMA _patches;'` || die;
}
else
{
    # As of table_version-1.4.0 and dbpatch-1.2.0
    # the loader binaries are in `pg_config --bindir`
    # but this may change in the future, so we add
    # that directory to the PATH and hope for the best
    my $pgbin = `pg_config --bindir 2>/dev/null`; chop($pgbin);
    if ( $pgbin ) { $ENV{'PATH'} .= ":$pgbin"; }
    else
    {
        # When `pg_config` is not installed we can try
        # a wild guess as for where the loaders are
        # installed.
        foreach my $dir (`'ls' -d /usr/lib/postgresql/*/bin/`)
        {
            chop($dir);
            $ENV{'PATH'} .= ":$dir";
        }
    }

    `which table_version-loader` ||
        die "Cannot find required table_version-loader.\n"
          . "Is table_version 1.4.0+ installed ?\n";
    `which dbpatch-loader` ||
        die "Cannot find required dbpatch-loader.\n"
          . "Is dbpatch 1.2.0+ installed ?\n";

    `table_version-loader --no-extension "$DB_NAME"` || die;
    `dbpatch-loader --no-extension "$DB_NAME" _patches` || die;
}

my @sqlfiles = <${SCRIPTSDIR}/*>;
foreach my $f (@sqlfiles) {
    print "Loading $f\n";
    `$PSQL -f $f` || die;
}
