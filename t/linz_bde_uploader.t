#!/usr/bin/perl
################################################################################
#
# Copyright 2011 Crown copyright (c)
# Land Information New Zealand and the New Zealand Government.
# All rights reserved
#
# This program is released under the terms of the new BSD license. See the
# LICENSE file for more information.
#
################################################################################

use strict;
use warnings;

use Test::More;
use Test::Exception;
use Test::Cmd;
use File::Temp qw/ tempdir /;
use File::Copy qw/ copy /;
use DBI;

my $script = "./blib/script/linz_bde_uploader";
my $confdir = "conf";
my $sqldir = "sql";

my $tmpdir = tempdir( '/tmp/linz_bde_uploader.t-data-XXXX', CLEANUP => 1);
my $logfname = ${tmpdir}.'/log';
#print "XXX ${tmpdir}\n";

my $testdbname = "linz_bde_uploader_test_$$";

my $pgoptions_backup = $ENV{'PGOPTIONS'};
$ENV{'PGOPTIONS'} .= ' -c log_duration=0 -c log_statement=none';
END {
  $ENV{'PGOPTIONS'} = $ENV{'PGOPTIONS'};
}

# Create test database

my $dbh = DBI->connect("dbi:Pg:dbname=template1", "") or
    die "Cannot connect to template1, please set PG env variables";

$dbh->do("create database ${testdbname}") or
    die "Cannot create test database ${testdbname}";

$dbh = DBI->connect("dbi:Pg:dbname=${testdbname}", "") or
    die "Cannot connect to ${testdbname}";

END {
  my $dbh = DBI->connect("dbi:Pg:dbname=template1", "");
  $dbh->do("drop database if exists ${testdbname}") if $dbh;
}

my $test = Test::Cmd->new( prog => $script, workdir => '' );
$test->run();

like( $test->stderr, qr/at least .* -full, -incremental, -full-incremental, -purge, or -remove-zombie/,
  'complain on stderr when no args');
like( $test->stderr, qr/Syntax/,
  'prints syntax on stderr when no args');
like( $test->stderr, qr/linz_bde_uploader.pl \[options..\] \[tables..\]/,
  'prints synopsis on stderr when no args');
is( $test->stdout, '', 'empty stdout on no args' );
is( $? >> 8, 1, 'exit status, with no args' );

$test->run( args => '-full' );
like( $test->stderr, qr/Cannot open configuration file/, 'stderr, called with -full');
is( $test->stdout, '', 'stdout, called with -full');
is( $? >> 8, 1, 'exit status, with -full' );

# Provide an empty configuration
open(my $cfg_fh, ">", "${tmpdir}/cfg1")
  or die "Can't write ${tmpdir}/cfg1: $!";
close($cfg_fh);

# Empty configuration
$test->run( args => "-full -config-path ${tmpdir}/cfg1" );
is( $test->stderr, '', 'stderr, empty config' );
like( $test->stdout,
  qr/.*tables.conf.*No such file/ms,
  'stdout, empty config' );
is( $? >> 8, 1, 'exit status, empty config' );

# Add empty log_settings configuration
open($cfg_fh, ">>", "${tmpdir}/cfg1")
  or die "Can't append to ${tmpdir}/cfg1: $!";
print $cfg_fh <<"EOF";
log_settings <<END_OF_LOG_SETTINGS
END_OF_LOG_SETTINGS
EOF
close($cfg_fh);

# Empty log_settings still writes to stderr
# See https://github.com/linz/linz_bde_uploader/issues/103
$test->run( args => "-full -config-path ${tmpdir}/cfg1" );
is( $test->stderr, '', 'stderr, empty log_settings' );
like( $test->stdout,
  qr/.*tables.conf.*No such file/ms,
  'stdout, empty config' );
is( $? >> 8, 1, 'exit status, empty log_settings' );

# TODO: Add log_settings w/out a root
# See https://github.com/linz/linz_bde_uploader/issues/103

# Add sane log_settings configuration
open($cfg_fh, ">>", "${tmpdir}/cfg1")
  or die "Can't append to ${tmpdir}/cfg1: $!";
print $cfg_fh <<"EOF";
log_settings <<END_OF_LOG_SETTINGS
log4perl.logger = DEBUG, File
log4perl.appender.File = Log::Log4perl::Appender::File
log4perl.appender.File.filename = ${logfname}
log4perl.appender.File.layout = Log::Log4perl::Layout::SimpleLayout
END_OF_LOG_SETTINGS
EOF
close($cfg_fh);

# We're now missing tables.conf...
$test->run( args => "-full -config-path ${tmpdir}/cfg1" );
is( $test->stderr, '', 'stderr, no tables.conf' );
is( $test->stdout, '', 'stdout, no tables.conf' );
is( $? >> 8, 1, 'exit status, no tables.conf' );
open(my $log_fh, "<", "${logfname}") or die "Cannot open ${logfname}";
my @logged = <$log_fh>;
is( @logged, 2,
  'logged 2 lines, no tables.conf' ); # WARNING: might depend on verbosity
my $log = join '', @logged;
like( $log,
  qr/FATAL.*tables.conf.*No such file/ms,
  'logfile - no bde_tables_config');

# Let's write a test tables configuration next

open($cfg_fh, ">", "${tmpdir}/tables.conf")
  or die "Can't write ${tmpdir}/tables.conf: $!";
print $cfg_fh <<"EOF";
TABLE test_table key=id row_tol=0.20,0.80 files test_file
EOF
close($cfg_fh);

# db_connection is now required now..

$test->run( args => "-full -config-path ${tmpdir}/cfg1" );
is( $test->stderr, '', 'stderr, no db_connection' );
is( $test->stdout, '', 'stdout, no db_connection' );
is( $? >> 8, 1, 'exit status, no db_connection' );
@logged = <$log_fh>;
is( @logged, 3,
  'logged 3 lines, no db_connection' ); # WARNING: might depend on verbosity
$log = join '', @logged;
like( $log,
  qr/ERROR.*item "db_connection".*missing.*Duration of job/ms,
  'logfile - no db_connection');

# Add db_connection

open($cfg_fh, ">>", "${tmpdir}/cfg1")
  or die "Can't append to ${tmpdir}/cfg1: $!";
print $cfg_fh "db_connection dbname=nonexistent\n";
close($cfg_fh);

# Attempts to connect to non-existing database

$test->run( args => "-full -config-path ${tmpdir}/cfg1" );
is( $test->stderr, '', 'stderr, nonexistent db');
is( $test->stdout, '', 'stdout, nonexistent db');
is( $? >> 8, 1, 'exit status, with nonexistent db' );
@logged = <$log_fh>;
is( @logged, 3,
  'logged 3 lines, nonexistent db' ); # WARNING: might depend on verbosity
$log = join '', @logged;
like( $log,
  qr/ERROR.*FATAL.*database "nonexistent" does not exist.*Duration of job/ms,
  'logfile - nonexistent db');

# Dry run logs to stdout instead of logfile

$test->run( args => "-full -dry-run -config-path ${tmpdir}/cfg1" );
is( $test->stderr, '', 'stderr, nonexistent db, dry-run');
like( $test->stdout,
  qr/FATAL.*database "nonexistent" does not exist.*Duration of job/ms,
  'logfile - nonexistent db, dry-run');
is( $? >> 8, 1, 'exit status, nonexistent db, dry-run');
@logged = <$log_fh>;
is( @logged, 0,
  'logged 0 lines, nonexistent db, dry-run' ); # WARNING: might depend on verbosity

# Dry run can also be specified with -d

$test->run( args => "-full -d -config-path ${tmpdir}/cfg1" );
is( $test->stderr, '', 'stderr, nonexistent db, dry-run (-d)');
like( $test->stdout,
  qr/FATAL.*database "nonexistent" does not exist.*Duration of job/ms,
  'logfile - nonexistent db, dry-run (-d)');
is( $? >> 8, 1, 'exit status, nonexistent db, dry-run (-d)');
@logged = <$log_fh>;
is( @logged, 0,
  'logged 0 lines, nonexistent db, dry-run (-d)' ); # WARNING: might depend on verbosity

# Create override config for testing
open($cfg_fh, ">", "${tmpdir}/cfg1.ext")
  or die "Can't append to ${tmpdir}/cfg1.ext: $!";
print $cfg_fh <<"EOF";
db_connection dbname=nonexist_override
EOF
close($cfg_fh);

# -config-extension (or -x) adds an override configuration

$test->run( args => "-full -d -config-path ${tmpdir}/cfg1 -config-extension ext" );
is( $test->stderr, '', 'stderr, nonexist_override db, dry-run');
like( $test->stdout,
  qr/FATAL.*database "nonexist_override" does not exist.*Duration of job/ms,
  'logfile - nonexist_override db, dry-run');
is( $? >> 8, 1, 'exit status, nonexist_override db, dry-run');
@logged = <$log_fh>;
is( @logged, 0,
  'logged 0 lines, nonexist_override db, dry-run' ); # WARNING: might depend on verbosity

# -config-extension can also be passed as -x

$test->run( args => "-full -d -config-path ${tmpdir}/cfg1 -x ext" );
is( $test->stderr, '', 'stderr, nonexist_override db, dry-run (-x)');
like( $test->stdout,
  qr/FATAL.*database "nonexist_override" does not exist.*Duration of job/ms,
  'logfile - nonexist_override db, dry-run (-x)');
is( $? >> 8, 1, 'exit status, nonexist_override db, dry-run (-x)');
@logged = <$log_fh>;
is( @logged, 0,
  'logged 0 lines, nonexist_override db, dry-run (-x)' ); # WARNING: might depend on verbosity

# A configuration with .test suffix will be read by default to
# override the main configuration
# Set database connection to the test database
open($cfg_fh, ">", "${tmpdir}/cfg1.test")
  or die "Can't append to ${tmpdir}/cfg1.test: $!";
print $cfg_fh <<"EOF";
db_connection dbname=${testdbname}
EOF
close($cfg_fh);

# Run with ability to connect to database

$test->run( args => "-full -config-path ${tmpdir}/cfg1" );
is( $test->stderr, '', 'stderr, empty db');
is( $test->stdout, '', 'stdout, empty db');
is( $? >> 8, 1, 'exit status, empty db');
@logged = <$log_fh>;
$log = join '', @logged;
like( $log,
  qr/ERROR.*function bde_checkschema.*not exist.*Duration of job/ms,
  'logfile - empty db');

# Run with both .test config and config-extension (-x)
#
# The .test config is parsed last, so overrides any
# setting found in the config extension (db_connection, in this case)
#
# We change log_settings in the extension file to test that it is
# still parsed (as current .text config has no log_settings)
#

open($cfg_fh, ">>", "${tmpdir}/cfg1.ext")
  or die "Can't append to ${tmpdir}/cfg1.ext: $!";
print $cfg_fh <<"EOF";
log_settings <<END_OF_LOG_SETTINGS
log4perl.logger = DEBUG, Screen
log4perl.appender.Screen = Log::Log4perl::Appender::Screen
log4perl.appender.Screen.stderr = 1
log4perl.appender.Screen.layout = Log::Log4perl::Layout::SimpleLayout
END_OF_LOG_SETTINGS
EOF
close($cfg_fh);

$test->run( args => "-full -config-path ${tmpdir}/cfg1 -x ext" );
like( $test->stderr,
    qr/ERROR.*function bde_checkschema.*not exist.*Duration of job/ms,
    'stderr, empty db (-x)');
is( $test->stdout, '', 'stdout, empty db (-x)');

# Unlink config extension file, not needed anymore
unlink("${tmpdir}/cfg1.ext");

# Set db_error_level to terse

open($cfg_fh, ">>", "${tmpdir}/cfg1")
  or die "Can't append to ${tmpdir}/cfg1: $!";
print $cfg_fh "db_error_level 0\n";
close($cfg_fh);

# Run again, should have less lines logged now

$test->run( args => "-full -config-path ${tmpdir}/cfg1" );
is( $test->stderr, '', 'stderr, empty db (terse)');
is( $test->stdout, '', 'stdout, empty db (terse)');
is( $? >> 8, 1, 'exit status, empty db (terse)');
@logged = <$log_fh>;
$log = join '', @logged;
like( $log,
  qr/ERROR.*function bde_checkschema.*not exist.*Duration of job/ms,
  'logfile - empty db (terse)');

# Prepare the database now
# TODO: make this simpler, see
# https://github.com/linz/linz_bde_uploader/issues/82

my $PSQLOPTS = "--set ON_ERROR_STOP=1";

# Install table_version extension
$dbh->do("CREATE EXTENSION IF NOT EXISTS table_version") or die
  "Could not create extension table_version";

# Install dbpatch extension
$dbh->do("CREATE SCHEMA IF NOT EXISTS _patches") or die
  "Could not create schema _patches";
$dbh->do("CREATE EXTENSION IF NOT EXISTS dbpatch SCHEMA _patches") or die
  "Could not create extension dbpatch";

# Install postgis extension
$dbh->do("CREATE EXTENSION IF NOT EXISTS postgis") or die
  "Could not create extension postgis";

# Install linz-dbe-schema

my $bdeschema_sqldir;
if ( $ENV{'BDESCHEMA_SQLDIR'} )
{
  $bdeschema_sqldir = $ENV{'BDESCHEMA_SQLDIR'};
  die "Cannot ivalid BDESCHEMA_SQLDIR $bdeschema_sqldir: not such directory"
    unless -d $bdeschema_sqldir;
}
else
{
  my @trydirs = ( '/usr/share/linz-bde-schema/sql',
                  '/usr/local/share/linz-bde-schema/sql' );
  foreach my $d (@trydirs) {
    if ( -d $d ) {
      $bdeschema_sqldir = $d;
      last
    }
  }
  die "Cannot find linz-bde-schema sql dir, try setting BDESCHEMA_SQLDIR\n"
      . '(tried: ' .  join(', ', @trydirs) . ')'
      unless $bdeschema_sqldir;
}
my @sqlfiles = <$bdeschema_sqldir/*>;
foreach my $f (@sqlfiles) {
  my $out = `psql --set ON_ERROR_STOP=1 "${testdbname}" -f $f 2>&1`;
  unlike( $out, qr/ERROR/, "sourcing $f gives no error" );
  #print "XXX $f - $out\n";
}

# Install local support functions

$ENV{'BDEUPLOADER_SQLDIR'} = $sqldir;
my $schemaload = Test::Cmd->new(
    prog => 'blib/script/linz-bde-uploader-schema-load',
    workdir => '' );
$schemaload->run();
like( $schemaload->stderr, qr/Usage.*database/,
    'prints syntax on stderr when calling schema-load with no arg'
    );
is( $schemaload->stdout, '',
    'empty stdout on calling schema-load with no arg');
is( $? >> 8, 1, 'exit status, schema-load with no args' );
$schemaload->run( args => 'unexistent_db' );
like( $schemaload->stderr, qr/database "unexistent_db" does not exist/,
    'prints error on stderr when passing non-existent db to schema-load'
    );
is( $schemaload->stdout, '',
    'empty stdout on schema-load with non-existent db');
is( $? >> 8, 2, 'exit status, schema-load with non-existent db' );

$schemaload->run( args => ${testdbname} );
# NOTE: table_version already exists only if we load it in previous
#       steps
unlike( $schemaload->stderr, qr/ERROR/,
    'stderr correct call has no ERROR printed'
    );
like( $schemaload->stdout, qr/Loading/,
    'stdout on calling schema-load with correct arg' );
unlike( $schemaload->stdout, qr/ERROR/,
    'no ERROR in stdout on calling schema-load with correct arg' );
is( $? >> 8, 0, 'exit status, schema-load with correct arg' );

# Check bde_control.upload

my $res = $dbh->selectall_arrayref(
  'SELECT * FROM bde_control.upload ORDER BY id DESC LIMIT 1',
  { Slice => {} }
);
is( @{$res}, 0, 'bde_control.upload is empty' );

# Run with prepared database, it's missing bde_repository now

$test->run( args => "-full -config-path ${tmpdir}/cfg1" );
is( $test->stderr, '', 'stderr, missing bde_repository');
is( $test->stdout, '', 'stdout, missing bde_repository');
is( $? >> 8, 1, 'exit status, missing bde_repository');
@logged = <$log_fh>;
is( @logged, 3,
  'logged 3 lines, missing bde_repository.*Duration' ); # WARNING: might depend on verbosity
$log = join '', @logged;
like( $log,
  qr/ERROR - Configuration item "bde_repository" is missing/ms,
  'logfile - missing bde_repository');

# Add bde_repository

my $repodir = ${tmpdir};
open($cfg_fh, ">>", "${tmpdir}/cfg1")
  or die "Can't append to ${tmpdir}/cfg1: $!";
print $cfg_fh "bde_repository ${repodir}\n";
close($cfg_fh);

# Run with prepared database, it's missing level0 dir now

$test->run( args => "-full -config-path ${tmpdir}/cfg1" );
is( $test->stderr, '', 'stderr, missing level0 dir');
is( $test->stdout, '', 'stdout, missing level0 dir');
is( $? >> 8, 1, 'exit status, missing level0 dir');
@logged = <$log_fh>;
is( @logged, 3,
  'logged 3 lines, missing level0 dir' ); # WARNING: might depend on verbosity
$log = join '', @logged;
like( $log,
  qr/ERROR - Apply Updates Failed: Level 0 directory.*doesn't exist/ms,
  'logfile - missing level0 dir');

# Check bde_control.upload

$res = $dbh->selectall_arrayref(
  'SELECT * FROM bde_control.upload ORDER BY id DESC LIMIT 1',
  { Slice => {} }
);
is( @{$res}, 0, 'bde_control.upload is empty' );

# Craft a level_0 directory

my $level0dir = $repodir . '/level_0';
mkdir $level0dir or die "Cannot create $level0dir";

# Run with prepared database, it's missing available uploads now

$test->run( args => "-full -config-path ${tmpdir}/cfg1" );
is( $test->stderr, '', 'stderr, no uploads available');
is( $test->stdout, '', 'stdout, no uploads available');
is( $? >> 8, 0, 'exit status, no uploads available');
@logged = <$log_fh>;
is( @logged, 3,
  'logged 3 lines, no uploads available' ); # WARNING: might depend on verbosity
$log = join '', @logged;
like( $log,
  qr/INFO - No level 0 uploads available/ms,
  'logfile - no uploads available');

# Check that appname can be set in configuration

open($cfg_fh, ">>", "${tmpdir}/cfg1")
  or die "Can't append to ${tmpdir}/cfg1: $!";
print $cfg_fh "application_name TEST_APP_NAME\n";
close($cfg_fh);

$test->run( args => "-full -config-path ${tmpdir}/cfg1" );
is( $test->stderr, '', 'stderr, no uploads available, application_name');
is( $test->stdout, '', 'stdout, no uploads available, application_name');
is( $? >> 8, 0, 'exit status, no uploads available, application_name');
@logged = <$log_fh>;
is( @logged, 4,
  'logged 4 lines, no uploads available, application_name' ); # WARNING: might depend on verbosity
$log = join '', @logged;
like( $log,
  qr/SET application_name='TEST_APP_NAME'/ms,
  'logfile - no uploads available, application_name');

# Check bde_control.upload

$res = $dbh->selectall_arrayref(
  'SELECT * FROM bde_control.upload ORDER BY id DESC LIMIT 1',
  { Slice => {} }
);
is( @{$res}, 0, 'bde_control.upload is empty' );

# Craft an upload in dataset in level_0 directory

my $level0ds1 = $level0dir . '/20170622170629';
mkdir $level0ds1 or die "Cannot create $level0ds1";
my $datadir = "t/data";

# Missing table.conf requested test_file now

$test->run( args => "-full -config-path ${tmpdir}/cfg1" );
is( $test->stderr, '', 'stderr, missing test_file');
is( $test->stdout, '', 'stdout, missing test_file');
is( $? >> 8, 1, 'exit status, missing test_file');
@logged = <$log_fh>;
#is( @logged, 4, 'logged 4 lines, missing test_table input file' ); # WARNING: might depend on verbosity
$log = join '', @logged;
like( $log,
  qr/Level 0 dataset is not complete.*missing: test_file/,
  'logfile - missing test_file');

# Check bde_control.upload

$res = $dbh->selectall_arrayref(
  'SELECT * FROM bde_control.upload ORDER BY id DESC LIMIT 1',
  { Slice => {} }
);
is( @{$res}, 0, 'bde_control.upload is empty' );

# Make test_file.crs available

copy($datadir.'/pab1.crs', $level0ds1.'/test_file.crs') or die "Copy failed: $!";

# Missing test_table table in database

$test->run( args => "-full -config-path ${tmpdir}/cfg1" );
is( $test->stderr, '', 'stderr, missing test_table');
is( $test->stdout, '', 'stdout, missing test_table');
is( $? >> 8, 1, 'exit status, missing test_table');
@logged = <$log_fh>;
#is( @logged, 4, 'logged 4 lines, missing test_table input file' ); # WARNING: might depend on verbosity
$log = join '', @logged;
like( $log,
  qr/Table 'bde.test_table' does not exist/,
  'logfile - missing test_table');

# Check bde_control.upload

$res = $dbh->selectall_arrayref(
  'SELECT * FROM bde_control.upload ORDER BY id DESC LIMIT 1',
  { Slice => {} }
);
is( @{$res}, 1, 'bde_control.upload is empty' );
is( $res->[0]{'id'}, '1', 'upload[3].id' );
is( $res->[0]{'schema_name'}, 'bde', 'upload[3].schema-name' );
is( $res->[0]{'status'}, 'E', 'upload[3].status' );

# Change tables.conf to reference one of the existing BDE tables

open($cfg_fh, ">", "${tmpdir}/tables.conf")
  or die "Can't write ${tmpdir}/tables.conf: $!";
print $cfg_fh <<"EOF";
TABLE crs_parcel_bndry key=audit_id  row_tol=0.20,0.95 files test_file
EOF
close($cfg_fh);

# Strip out the WARNING about /dev/stdout being unusable
sub clean_stderr
{
    my $stderr = shift;
    if ( $stderr ) {
        if ( $stderr =~ m/(WARNING:.*dev.stdout.*)\n/ )
        {
            print STDERR "$1\n";
            $stderr =~ s/WARNING:.*dev.stdout.*\n//;
        }
    } else {
        $stderr = '';
    }
    return $stderr;
}

# This should supposedly be first successful upload

$test->run( args => "-full -config-path ${tmpdir}/cfg1" );
my $stderr = $test->stderr;
$stderr =~ s/WARNING:.*dev.stdout//;
is( clean_stderr($test->stderr), '', 'stderr, success upload test_file');
is( $test->stdout, '', 'stdout, success upload test_file');
is( $? >> 8, 0, 'exit status, success upload test_file');
@logged = <$log_fh>;
$log = join '', @logged;
like( $log,
  qr/INFO - Job.*finished successfully/,
  'logfile - success upload test_file');

# check actual table content

$res = $dbh->selectall_arrayref(
  'SELECT * FROM bde.crs_parcel_bndry ORDER BY pri_id',
  { Slice => {} }
);
is( @{$res}, 3, 'crs_parcel_bndry has 3 entries' );

is( $res->[0]{'pri_id'}, '4457326', 'crs_parcel_bndry[0].pri_id' );
is( $res->[0]{'sequence'}, '3', 'crs_parcel_bndry[0].sequence' );
is( $res->[0]{'lin_id'}, '11960041', 'crs_parcel_bndry[0].lin_id' );
is( $res->[0]{'reversed'}, 'Y', 'crs_parcel_bndry[0].reversed' );
is( $res->[0]{'audit_id'}, '80401150', 'crs_parcel_bndry[0].audit_id' );

is( $res->[1]{'pri_id'}, '4457327', 'crs_parcel_bndry[1].pri_id' );
is( $res->[1]{'sequence'}, '2', 'crs_parcel_bndry[1].sequence' );
is( $res->[1]{'lin_id'}, '29694578', 'crs_parcel_bndry[1].lin_id' );
is( $res->[1]{'reversed'}, 'N', 'crs_parcel_bndry[1].reversed' );
is( $res->[1]{'audit_id'}, '80401149', 'crs_parcel_bndry[1].audit_id' );

is( $res->[2]{'pri_id'}, '4457328', 'crs_parcel_bndry[2].pri_id' );
is( $res->[2]{'sequence'}, '1', 'crs_parcel_bndry[2].sequence' );
is( $res->[2]{'lin_id'}, '29694591', 'crs_parcel_bndry[2].lin_id' );
is( $res->[2]{'reversed'}, 'Y', 'crs_parcel_bndry[2].reversed' );
is( $res->[2]{'audit_id'}, '80401148', 'crs_parcel_bndry[2].audit_id' );

# Check bde_control.upload

$res = $dbh->selectall_arrayref(
  'SELECT * FROM bde_control.upload ORDER BY id DESC LIMIT 1',
  { Slice => {} }
);
is( $res->[0]{'id'}, '2', 'upload[2].id' );
is( $res->[0]{'schema_name'}, 'bde', 'upload[2].schema-name' );
is( $res->[0]{'status'}, 'C', 'upload[2].status' );

# Run full upload again - no updates to apply this time (by date)

$test->run( args => "-full -config-path ${tmpdir}/cfg1" );
is( $test->stderr, '', 'stderr, success upload test_file (2)');
is( $test->stdout, '', 'stdout, success upload test_file (2)');
is( $? >> 8, 0, 'exit status, success upload test_file (2)');
@logged = <$log_fh>;
$log = join '', @logged;
like( $log,
  qr/INFO - No dataset updates to apply/,
  'logfile - success upload test_file (2)');

# Rename dataset dir

my $level0ds2 = $level0dir . '/20170628110348';
rename($level0ds1, $level0ds2)
  or die "Cannot rename $level0ds1 to $level0ds2: $!";

# Run full upload again, should find the new dataset now

$test->run( args => "-full -config-path ${tmpdir}/cfg1" );
is( clean_stderr($test->stderr), '', 'stderr, success upload test_file (3)');
is( $test->stdout, '', 'stdout, success upload test_file (3)');
is( $? >> 8, 0, 'exit status, success upload test_file (3)');
@logged = <$log_fh>;
$log = join '', @logged;
like( $log,
  qr/INFO - Job.*finished successfully/,
  'logfile - success upload test_file (3)');

# check the new table content

$res = $dbh->selectall_arrayref(
  'SELECT * FROM bde.crs_parcel_bndry ORDER BY pri_id',
  { Slice => {} }
);
is( @{$res}, 3, 'crs_parcel_bndry has still only 3 entries' );

# Check bde_control.upload

$res = $dbh->selectall_arrayref(
  'SELECT * FROM bde_control.upload ORDER BY id DESC LIMIT 1',
  { Slice => {} }
);
is( $res->[0]{'id'}, '3', 'upload[3].id' );
is( $res->[0]{'schema_name'}, 'bde', 'upload[3].schema-name' );
is( $res->[0]{'status'}, 'C', 'upload[3].status' );

# Rename dataset dir again

my $level0ds3 = $level0dir . '/20170628115115';
rename($level0ds2, $level0ds3)
  or die "Cannot rename $level0ds2 to $level0ds3: $!";

# Run full upload again but only up to -before any dataset is
# available

$test->run( args => "-f -c ${tmpdir}/cfg1 -before 20170625" );
is( $test->stderr, '', 'stderr, no uploads available (4)');
is( $test->stdout, '', 'stdout, no uploads available (4)');
is( $? >> 8, 0, 'exit status, no uploads available (4)');
@logged = <$log_fh>;
$log = join '', @logged;
like( $log,
  qr/INFO - No level 0 uploads available/ms,
  'logfile - success upload test_file (3)');

# Now run full upload again but -before including available dataset

$test->run( args => "-f -c ${tmpdir}/cfg1 -before 20170701" );
is( clean_stderr($test->stderr), '', 'stderr, success upload test_file (4)');
is( $test->stdout, '', 'stdout, success upload test_file (4)');
is( $? >> 8, 0, 'exit status, success upload test_file (4)');
@logged = <$log_fh>;
$log = join '', @logged;
like( $log,
  qr/INFO - Job.*finished successfully/,
  'logfile - success upload test_file (4)');

# Check bde_control.upload

$res = $dbh->selectall_arrayref(
  'SELECT * FROM bde_control.upload ORDER BY id DESC LIMIT 1',
  { Slice => {} }
);
is( $res->[0]{'id'}, '4', 'upload[4].id' );
is( $res->[0]{'schema_name'}, 'bde', 'upload[4].schema-name' );
is( $res->[0]{'status'}, 'C', 'upload[4].status' );

# Run full upload again, using -b for -before
# No new data to upload

$test->run( args => "-f -c ${tmpdir}/cfg1 -b 20170701" );
is( $test->stderr, '', 'stderr, no dataset updates to apply(5)');
is( $test->stdout, '', 'stdout, no dataset updates to apply (5)');
is( $? >> 8, 0, 'exit status, no dataset updates to apply (5)');
@logged = <$log_fh>;
$log = join '', @logged;
like( $log,
  qr/No dataset updates to apply/,
  'logfile - no dataset updates to apply (5)');

# Run again but with -rebuild
# No new data to upload

$test->run( args => "-f -c ${tmpdir}/cfg1 -b 20170701 -rebuild" );
is( clean_stderr($test->stderr), '', 'stderr, success upload test_file (5)');
is( $test->stdout, '', 'stdout, success upload test_file (5)');
is( $? >> 8, 0, 'exit status, success upload test_file (5)');
@logged = <$log_fh>;
$log = join '', @logged;
like( $log,
  qr/INFO - Job.*finished successfully/,
  'logfile - success upload test_file (5)');

# Check bde_control.upload

$res = $dbh->selectall_arrayref(
  'SELECT * FROM bde_control.upload ORDER BY id DESC LIMIT 1',
  { Slice => {} }
);
is( $res->[0]{'id'}, '5', 'upload[5].id' );
is( $res->[0]{'schema_name'}, 'bde', 'upload[5].schema-name' );
is( $res->[0]{'status'}, 'C', 'upload[5].status' );

# Update target table, to check it is properly rebuilt

$res = $dbh->do(
  'UPDATE bde.crs_parcel_bndry set sequence = -sequence',
) or die "Could not update bde.crs_parcel_bndry";

# -rebuild can be also passed as -r

$test->run( args => "-f -c ${tmpdir}/cfg1 -b 20170701 -r" );
is( clean_stderr($test->stderr), '', 'stderr, success upload test_file (6)');
is( $test->stdout, '', 'stdout, success upload test_file (6)');
is( $? >> 8, 0, 'exit status, success upload test_file (6)');
@logged = <$log_fh>;
$log = join '', @logged;
like( $log,
  qr/INFO - Job.*finished successfully/,
  'logfile - success upload test_file (6)');

# Check bde_control.upload

$res = $dbh->selectall_arrayref(
  'SELECT * FROM bde_control.upload ORDER BY id DESC LIMIT 1',
  { Slice => {} }
);
is( $res->[0]{'id'}, '6', 'upload[6].id' );
is( $res->[0]{'schema_name'}, 'bde', 'upload[6].schema-name' );
is( $res->[0]{'status'}, 'C', 'upload[6].status' );

# check actual table content

$res = $dbh->selectall_arrayref(
  'SELECT * FROM bde.crs_parcel_bndry ORDER BY pri_id',
  { Slice => {} }
);
is( @{$res}, 3, 'crs_parcel_bndry has 3 entries' );

is( $res->[0]{'pri_id'}, '4457326', 'crs_parcel_bndry[0].pri_id (6)' );
is( $res->[0]{'sequence'}, '3', 'crs_parcel_bndry[0].sequence (6)' );
is( $res->[0]{'lin_id'}, '11960041', 'crs_parcel_bndry[0].lin_id (6)' );
is( $res->[0]{'reversed'}, 'Y', 'crs_parcel_bndry[0].reversed (6)' );
is( $res->[0]{'audit_id'}, '80401150', 'crs_parcel_bndry[0].audit_id (6)' );

is( $res->[1]{'pri_id'}, '4457327', 'crs_parcel_bndry[1].pri_id (6)' );
is( $res->[1]{'sequence'}, '2', 'crs_parcel_bndry[1].sequence (6)' );
is( $res->[1]{'lin_id'}, '29694578', 'crs_parcel_bndry[1].lin_id (6)' );
is( $res->[1]{'reversed'}, 'N', 'crs_parcel_bndry[1].reversed (6)' );
is( $res->[1]{'audit_id'}, '80401149', 'crs_parcel_bndry[1].audit_id (6)' );

is( $res->[2]{'pri_id'}, '4457328', 'crs_parcel_bndry[2].pri_id (6)' );
is( $res->[2]{'sequence'}, '1', 'crs_parcel_bndry[2].sequence (6)' );
is( $res->[2]{'lin_id'}, '29694591', 'crs_parcel_bndry[2].lin_id (6)' );
is( $res->[2]{'reversed'}, 'Y', 'crs_parcel_bndry[2].reversed (6)' );
is( $res->[2]{'audit_id'}, '80401148', 'crs_parcel_bndry[2].audit_id (6)' );

# Pretend a job is active

$res = $dbh->do(<<END_OF_SQL
INSERT INTO bde_control.upload VALUES (
  nextval('bde_control.upload_id_seq'::regclass),
  'bde',
  '2017-06-28 13:00:00',
  '2017-06-28 13:00:00', 'A')
END_OF_SQL
) or die "Could not INSERT INTO bde_control.upload";

# Check bde_control.upload

$res = $dbh->selectall_arrayref(
  'SELECT * FROM bde_control.upload ORDER BY id DESC LIMIT 1',
  { Slice => {} }
);
is( $res->[0]{'id'}, '7', 'upload[7].id' );
is( $res->[0]{'schema_name'}, 'bde', 'upload[7].schema-name' );
is( $res->[0]{'status'}, 'A', 'upload[7].status' );


# Attempt to run a new job now

$test->run( args => "-f -c ${tmpdir}/cfg1 -r" );
is( $test->stderr, '', 'stderr, another job is already active (8)');
is( $test->stdout, '', 'stdout, another job is already active (8)');
is( $? >> 8, 1, 'exit status, another job is already active (8)');
@logged = <$log_fh>;
$log = join '', @logged;
like( $log,
  qr/Cannot create upload job - another job is already active/,
  'logfile - another job is already active (8)');

# Override lock to run a new job now

$test->run( args => "-f -c ${tmpdir}/cfg1 -r -override-locks" );
is( clean_stderr($test->stderr), '', 'stderr, override-locks (8)');
is( $test->stdout, '', 'stdout, override-locks (8)');
is( $? >> 8, 0, 'exit status, override-locks (8)');
@logged = <$log_fh>;
$log = join '', @logged;
like( $log,
  qr/INFO - Job.*finished successfully/,
  'logfile - override-locks (8)');

# Check bde_control.upload

$res = $dbh->selectall_arrayref(
  'SELECT * FROM bde_control.upload ORDER BY id DESC LIMIT 1',
  { Slice => {} });
is( $res->[0]{'id'}, '8', 'upload[8].id' );
is( $res->[0]{'status'}, 'C', 'upload[8].status' );

# Pretend job 8 is still active

$res = $dbh->do("UPDATE bde_control.upload set status = 'A' where id = 8")
  or die "Could not UPDATE bde_control.upload";

# override-locks can be passed as -o too

$test->run( args => "-f -c ${tmpdir}/cfg1 -r -o" );
is( clean_stderr($test->stderr), '', 'stderr, override-locks (9)');
is( $test->stdout, '', 'stdout, override-locks (9)');
is( $? >> 8, 0, 'exit status, override-locks (9)');
@logged = <$log_fh>;
$log = join '', @logged;
like( $log,
  qr/INFO - Job.*finished successfully/,
  'logfile - override-locks (9)');

# Check bde_control.upload

$res = $dbh->selectall_arrayref(
  'SELECT * FROM bde_control.upload ORDER BY id DESC LIMIT 1',
  { Slice => {} });
is( $res->[0]{'id'}, '9', 'upload[8].id' );
is( $res->[0]{'status'}, 'C', 'upload[9].status' );

# Test keeping temporary schema

open($cfg_fh, ">>", "${tmpdir}/cfg1")
  or die "Can't append to ${tmpdir}/cfg1: $!";
print $cfg_fh "db_upload_complete_sql <<EOT\n";
print $cfg_fh "SELECT bde_control.bde_SetOption({{id}},'keep_temp_schema','yes');\n";
print $cfg_fh "EOT\n";
close($cfg_fh);

$test->run( args => "-f -c ${tmpdir}/cfg1 -r -o" );
is( $? >> 8, 0, 'exit status, keep_temp_schema (10)');
is( clean_stderr($test->stderr), '', 'stderr, keep_temp_schema (10)' );
is( $test->stdout, '', 'stdout, keep_temp_schema (10)' );
@logged = <$log_fh>;
$log = join '', @logged;
like( $log,
  qr/INFO - Job 10 finished successfully/,
  'logfile - keep_temp_schema (10)');

$res = $dbh->selectall_arrayref(
  "SELECT nspname FROM pg_namespace where nspname like 'bde_upload_%'",
  { Slice => {} });
is( scalar @{ $res }, 1, 'kept just one temp schema (10)' );
is( $res->[0]{'nspname'}, 'bde_upload_10', 'kept temp schema (10)' );

close($log_fh);
done_testing(213);
