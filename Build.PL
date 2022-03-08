#!/usr/bin/env perl
################################################################################
#
# linz_bde_uploader -  LINZ BDE uploader for PostgreSQL
#
# Copyright 2016 Crown copyright (c)
# Land Information New Zealand and the New Zealand Government.
# All rights reserved
#
# This program is released under the terms of the new BSD license. See the
# LICENSE file for more information.
#
################################################################################
use strict;
use warnings;

use Config;
use lib '.';
use inc::MyBuilder;

my $requires = {
    'Log::Log4perl'                    => 0,
    'Log::Dispatch'                    => 0,
    'Log::Dispatch::FileRotate'        => 0,
    'Log::Dispatch::Email::MailSender' => 0,
    'LINZ::Config'                     => 0,
    'LINZ::Bde'                        => 0,
    'DBD::Pg'                          => 0,
    'Date::Calc'                       => 0,
    'Try::Tiny'                        => 0,
};

my $build =  inc::MyBuilder->new(
    module_name        => 'LINZ::BDEUploader',
    license            => 'bsd',
    dist_author        => 'LINZ',
    dist_version       => '2.11.0',
    dist_abstract      => 'Software for loading LINZ BDE files into a PostgreSQL database',
    sign               => 1,
    requires           => $requires,
    create_makefile_pl => 'small',
);

$build->add_build_element('conf');
$build->add_build_element('sql');

$build->create_build_script;
