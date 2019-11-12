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
our $PSQL="psql -o /dev/null -XtA --set ON_ERROR_STOP";

if ( defined( $ENV{'BDEUPLOADER_SQLDIR'} ) ) {
    $SCRIPTSDIR=$ENV{'BDEUPLOADER_SQLDIR'};
}

our $DB_NAME;
our $EXTENSION_MODE = 1;
our $SHOW_VERSION = 0;
our $READ_ONLY = 0;

sub help
{
    my ($exitcode) = @_;
    print STDERR "Usage: $0 [--noextension] [--readonly] { <database> | - }\n";
    print STDERR "       $0 --version\n";
    exit $exitcode;
}

GetOptions (
    "extension!" => \$EXTENSION_MODE,
    "version!" => \$SHOW_VERSION,
    "readonly!" => \$READ_ONLY
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

system("which table_version-loader > /dev/null") == 0 or
    die "Cannot find required table_version-loader.\n"
      . "Is table_version 1.4.0+ installed ?\n";
system("which dbpatch-loader > /dev/null") == 0 or
    die "Cannot find required dbpatch-loader.\n"
      . "Is dbpatch 1.2.0+ installed ?\n";

# Check if table_version-loader supports stdout
my $TABLEVERSION_SUPPORTS_STDOUT = (
     system("table_version-loader -  > /dev/null 2>&1") == 0
);

# Check if dbpatch-loader supports stdout
my $DBPATCH_SUPPORTS_STDOUT = (
     system("dbpatch-loader - _patches > /dev/null 2>&1") == 0
);

my $sql;
if ( $DB_NAME ne '-' ) {
    system("$PSQL -c 'select version()'") == 0
        or die "Could not connect to database ${DB_NAME}\n";
    open($sql, '|-', "$PSQL") or die "Cannot start psql\n";
} else {
    die "ERROR: table_version-loader does not support stdout mode, cannot proceed.\n"
        . "HINT: install table_version 1.6.0 or higher to fix this\n"
        unless $TABLEVERSION_SUPPORTS_STDOUT;
    die "ERROR: dbpatch-loader does not support stdout mode, cannot proceed\n"
        . "HINT: install dbpatch 1.4.0 or higher to fix this\n"
        unless $DBPATCH_SUPPORTS_STDOUT;
    $sql = \*STDOUT;
}

print STDERR "Loading DBE uploader schema in database "
    . $ENV{'PGDATABASE'} . " (extension mode "
    . ( ${EXTENSION_MODE} ?  "on" : "off" )
    . ")\n";

$SIG{'PIPE'} = sub {
    die "Got sigpipe \n";
};

my $EXTOPT = ${EXTENSION_MODE} ? '' : '--no-extension';

if ( ! $TABLEVERSION_SUPPORTS_STDOUT ) {
    print STDERR "WARNING: table_version-loader does not support stdout mode, working in non-transactional mode\n";
    print STDERR "HINT: install table_version 1.6.0 or higher to fix this\n";
    system("table_version-loader ${EXTOPT} '$DB_NAME'") == 0
        or die "Could not load table_version in ${DB_NAME} database\n";
}

if ( ! $DBPATCH_SUPPORTS_STDOUT ) {
    print STDERR "WARNING: dbpatch-loader does not support stdout mode, working in non-transactional mode\n";
    print STDERR "HINT: install dbpatch 1.4.0 or higher to fix this\n";
    system("dbpatch-loader ${EXTOPT} '$DB_NAME' _patches") == 0
        or die "Could not load dbpatch in ${DB_NAME} database\n";
}

print $sql "BEGIN;\n";

if ( $TABLEVERSION_SUPPORTS_STDOUT ) {
    open(my $loader, "table_version-loader ${EXTOPT} - |")
        or die "Could not run table_version -\n";
    while (<$loader>) {
        # NOTE: begin/commit will be filtered later
        print $sql $_;
    }
    close($loader);
}

if ( $DBPATCH_SUPPORTS_STDOUT ) {
    # TODO: open pipe from loader, print to stdout
    open(my $loader, "dbpatch-loader ${EXTOPT} - _patches |")
        or die "Could not run dbpatch_loader -\n";
    while (<$loader>) {
        # NOTE: begin/commit will be filtered later
        print $sql $_;
    }
    close($loader);
}

my @sqlfiles = <${SCRIPTSDIR}/*>;
foreach my $f (@sqlfiles) {
    print STDERR "Loading $f\n";
    open(F, "<$f") or die "Could not open $f for reading\n";
    while (<F>) {
        next if /^BEGIN;/;
        next if /^COMMIT;/;
        print $sql $_;
    }
    close(F);
}

if ( $READ_ONLY ) {
    print $sql <<EOF;
REVOKE UPDATE, INSERT, DELETE, TRUNCATE
    ON ALL TABLES IN SCHEMA bde_control
    FROM bde_dba, bde_admin, bde_user;
EOF
}

print $sql "COMMIT;\n";
close($sql);
