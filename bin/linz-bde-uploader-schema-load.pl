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

our $PREFIX = '@@PREFIX@@';
our $SCRIPTSDIR="${PREFIX}/share/linz-bde-uploader/sql/";
our $PSQL="psql -tA --set ON_ERROR_STOP";

if ( defined( $ENV{'BDEUPLOADER_SQLDIR'} ) ) {
    $SCRIPTSDIR=$ENV{'BDEUPLOADER_SQLDIR'};
}

our $DB_NAME=$ARGV[0] || die "Usage: $0 <database>\n";

if ( ! -f "${SCRIPTSDIR}/01-bde_control_tables.sql" ) {
    die "Cannot find 01-bde_control_tables.sql in ${SCRIPTSDIR}\n"
      . "Please set BDEUPLOADER_SQLDIR environment variable\n";
}

$ENV{'PGDATABASE'}=$DB_NAME;

`$PSQL -c 'CREATE EXTENSION IF NOT EXISTS table_version;'` || die;
`$PSQL -c 'CREATE SCHEMA IF NOT EXISTS _patches;'` || die;
`$PSQL -c 'CREATE EXTENSION IF NOT EXISTS dbpatch SCHEMA _patches;'` || die;

my @sqlfiles = <${SCRIPTSDIR}/*>;
foreach my $f (@sqlfiles) {
    print "Loading $f\n";
    `$PSQL -f $f` || die;
}
