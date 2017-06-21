#!/usr/bin/perl
################################################################################
#
# $Id$
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

my $script = "./blib/script/linz_bde_uploader";
my $confdir = "conf";

my $tmpdir = tempdir( '/tmp/linz_bde_uploader.t-data-XXXX', CLEANUP => 1);
my $logfname = ${tmpdir}.'/log';
#print "XXX ${tmpdir}\n";

my $test = Test::Cmd->new( prog => $script, workdir => '' );
$test->run();

like( $test->stderr, qr/at least .* -full, -incremental, -purge, or -remove-zombie/,
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

# Provide a configuration
copy($confdir.'/linz_bde_uploader.conf', $tmpdir.'/cfg1')
  or die "Copy failed: $!";
# A configuration with .test suffix will be read by default to
# override the mai configuration
open(my $cfg_fh, ">", "${tmpdir}/cfg1.test")
  or die "Can't write ${tmpdir}/cfg1.test: $!";
print $cfg_fh <<"EOF";
db_connection dbname=nonexistent
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
is( $test->stderr, '', 'stderr, called with -full -config-path');
is( $test->stdout, '', 'stdout, called with -full -config-path');
is( $? >> 8, 1, 'exit status, with -full -config-path' );
open(my $log_fh, "<", "${logfname}") or die "Cannot open ${logfname}";
my $line = <$log_fh>;
like( $line,
  qr/FATAL - Error reading BDE upload dataset configuration .*tables.conf/,
  'logfile, called with -full -config-path');
$line = <$log_fh>;
like( $line,
  qr/Cannot open file.*No such file/,
  'logfile line 2, called with -full -config-path');
$line = <$log_fh>;
is( $line, undef, 'logfile at EOF, called with -full -config-path' );

# Let's write a test tables configuration next
open($cfg_fh, ">", "${tmpdir}/tables.conf")
  or die "Can't write ${tmpdir}/tables.conf: $!";
print $cfg_fh <<"EOF";
TABLE crs_test key=id row_tol=0.20,0.80 files test
EOF
close($cfg_fh);

$test->run( args => "-full -config-path ${tmpdir}/cfg1" );
is( $test->stderr, '', 'stderr, -full -config-path and tables.conf');
is( $test->stdout, '', 'stdout, -full -config-path and tables.conf');
is( $? >> 8, 1, 'exit status, with -full -config-path and tables.conf' );
$line = <$log_fh>;
like( $line,
  qr/ERROR.*FATAL.*database "nonexistent" does not exist/,
  'logfile line 1 - nonexistent db');
$line = <$log_fh>;
is( $line, "\n", 'logfile line 2 - nonexistent db');
$line = <$log_fh>;
like( $line,
  qr/INFO.*Duration of job/,
  'logfile line 3 - nonexistent db (duration)');
$line = <$log_fh>;
is( $line, undef, 'logfile line at EOF, nonexistent db');

close($log_fh);
done_testing();
