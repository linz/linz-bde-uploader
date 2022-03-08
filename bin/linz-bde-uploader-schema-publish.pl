#!/usr/bin/env perl
################################################################################
#
# linz-bde-uploader-schema-publish.pl -  LINZ BDE uploader / schema publisher
#
# Copyright 2019 Crown copyright (c)
# Land Information New Zealand and the New Zealand Government.
# All rights reserved
#
# This program is released under the terms of the new BSD license. See the
# LICENSE file for more information.
#
################################################################################
# Script to publish linz-bde-uploader support tables in PostgreSQL databases
################################################################################

use strict;
use warnings;
use Getopt::Long;

our $PSQL="psql -qXtA --set ON_ERROR_STOP";
our $DB_NAME;
our $SHOW_VERSION = 0;

sub help
{
    my ($exitcode) = @_;
    print STDERR "Usage: $0 { <database> | - }\n";
    print STDERR "       $0 --version\n";
    exit $exitcode;
}

GetOptions (
    "version!" => \$SHOW_VERSION
) || help(0);

$DB_NAME=$ARGV[0];

if ( $SHOW_VERSION )
{
    print "@@VERSION@@ @@REVISION@@";
    exit 0;
}
help(1) if ( ! $DB_NAME );

$ENV{'PGDATABASE'}=$DB_NAME;

my $sql;
if ( $DB_NAME ne '-' ) {
    system("$PSQL -c 'select version()'") == 0
        or die "Could not connect to database ${DB_NAME}\n";
    open($sql, '|-', "$PSQL") or die "Cannot start psql\n";
} else {
    $sql = \*STDOUT;
}

print $sql <<EOF;
DO \$PUBLICATION\$
DECLARE
    v_table NAME;
BEGIN

    IF NOT EXISTS ( SELECT 1 FROM pg_catalog.pg_namespace
                    WHERE nspname = 'bde_control' )
    THEN
        RAISE EXCEPTION
            'Schema bde_control does not exist, '
            'run linz-bde-uploader-schema-load ?';
    END IF;
    IF NOT EXISTS ( SELECT 1 FROM pg_catalog.pg_publication
                    WHERE pubname = 'all_bde_control' )
    THEN
        CREATE PUBLICATION all_bde_control;
    END IF;
    ALTER PUBLICATION all_bde_control OWNER TO bde_dba;

    FOR v_table IN SELECT c.relname from pg_class c, pg_namespace n
        WHERE n.nspname = 'bde_control' and c.relnamespace = n.oid
        AND c.relkind = 'r' AND c.relname NOT IN (
            SELECT tablename FROM pg_catalog.pg_publication_tables
            WHERE pubname = 'all_bde_control'
              AND schemaname = 'bde_control'
        )
    LOOP
        EXECUTE format('ALTER PUBLICATION all_bde_control '
                       'ADD TABLE bde_control.%I',
                       v_table);
    END LOOP;

    RAISE INFO 'Publication "all_bde_control" ready';

END;
\$PUBLICATION\$;
EOF

close($sql);
