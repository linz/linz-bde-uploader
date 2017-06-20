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

my $script = "./blib/script/linz_bde_uploader";

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

done_testing();
