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
our $EXTENSION_MODE;

sub help
{
    my ($exitcode) = @_;
    print STDERR "Usage: $0 [--noextension] <database>\n";
    exit $exitcode;
}

GetOptions (
    "extension!" => \$EXTENSION_MODE
) || help(0);

$DB_NAME=$ARGV[0];

help(1) if ( ! $DB_NAME );

if ( ! -f "${SCRIPTSDIR}/01-bde_control_tables.sql" ) {
    die "Cannot find 01-bde_control_tables.sql in ${SCRIPTSDIR}\n"
      . "Please set BDEUPLOADER_SQLDIR environment variable\n";
}

$ENV{'PGDATABASE'}=$DB_NAME;

print "Loading DBE uploader schema in database "
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
    my $pgbin = `pg_config --bindir`; chop($pgbin);
    `${pgbin}/table_version-loader --no-extension "$DB_NAME"` || die;
    `${pgbin}/dbpatch-loader --no-extension "$DB_NAME" _patches` || die;
}

my @sqlfiles = <${SCRIPTSDIR}/*>;
foreach my $f (@sqlfiles) {
    print "Loading $f\n";
    `$PSQL -f $f` || die;
}
