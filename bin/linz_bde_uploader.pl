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
# Script to upload BDE files to the postgres database.  Reads options from
# linz_bde_uploader.conf located in the configuration directory
################################################################################
use strict;
use warnings;

our $VERSION = '@@VERSION@@';

use File::Basename qw( dirname );
use FindBin;
use lib $FindBin::Bin;
use lib "$FindBin::Bin/../lib";
use Getopt::Long;
use Log::Log4perl qw(:easy :levels get_logger);
use Log::Log4perl::Layout;
use Try::Tiny;

use LINZ::BdeUpload;
use LINZ::Config;

$SIG{INT}  = \&signal_handler;
$SIG{TERM} = \&signal_handler;

#log levels
my %LOG_LEVELS =
(
    OFF   => $OFF,
    FATAL => $FATAL,
    ERROR => $ERROR,
    WARN  => $WARN,
    INFO  => $INFO,
    DEBUG => $DEBUG,
    TRACE => $TRACE,
    ALL   => $ALL
);

# Main program controls

my $do_purge_old = 0;      # Clean up old jobs if set
my $do_remove_zombie = 0;   # Clean up zombied jobs if set
my $apply_level0 = 0;      # Do level 0 updates if set
my $apply_level0_inc = 0;  # Do level 0 incremental load if set
my $apply_level5 = 0;      # Do level 5 updates is set
my $rebuild = 0;      # Do level 5 updates is set
my $l0_timeout = 0;  # Maximum time for level0 updates
my $l5_timeout = 0;  # Maximum time for level5 updates
my $skip_postupload = 0;  # Skip post upload tasks
my $dry_run = 0;          # Dry run only - print out files to be updated
my $verbose = 0;          # Dry run only - print out files to be updated
my $keep_files = 0;       # Keep working files - for testing
my $cfgext = '';          # Alternative configuration
my $cfgpath = '~/config'; # Configuration path
my $showhelp = 0;         # Show help
my $override_locks = 0;   # Clear existing locks
my $listing_file = '';
my $enddate = '';         # Only use files before this date
my $maintain_db = 0;      # run database maintain after run.
my $enable_hooks = 0;     # if enabled will run any event hooks defined in the config
my $print_version = 0;
my $log_level = undef;
my $status = 0;
my $logger;
my $upload;

GetOptions (
    "help|h" => \$showhelp,
    "config-extension|x=s" => \$cfgext,
    "config-path|c=s" => \$cfgpath,
    "purge|p!" => \$do_purge_old,
    "remove-zombie|z!" => \$do_remove_zombie,
    "skip-postupload-tasks!" => \$skip_postupload,
    "full|f!" => \$apply_level0,
    "full-incremental|j!" => \$apply_level0_inc,
    "incremental|i!" => \$apply_level5,
    "rebuild|r!" => \$rebuild,
    "dry-run|d!" => \$dry_run,
    "full-timeout|t=f" => \$l0_timeout,
    "inc-timeout|u=f" => \$l5_timeout,
    "override-locks|o" => \$override_locks,
    "keep-files|k" => \$keep_files,
    "before|b=s" => \$enddate,
    "maintain-database|m" => \$maintain_db,
    "listing_file|l=s" => \$listing_file,
    "enable-hooks|e!" => \$enable_hooks,
    "verbose|v" => \$verbose,
    "log-level=s" => \$log_level,
    "version" => \$print_version,
    )
    || help(0);

help(1) if $showhelp;

if ($print_version)
{
    print "$VERSION\n";
    exit(0);
}

if (defined $log_level && !exists $LOG_LEVELS{$log_level})
{
    print "Log level must be one of " . join(', ', keys %LOG_LEVELS) . "\n";
    help(0);
}

if($apply_level0_inc && !$apply_level0)
{
    $apply_level0 = 1;
}

if( ! $apply_level0 && ! $apply_level5 && ! $do_purge_old && ! $do_remove_zombie && ! $rebuild)
{
    print STDERR "Need at least one option of -full, -incremental, -purge, or -remove-zombie\n";
    help(0, *STDERR, 1);
}

$enddate .= '000000' if $enddate =~ /^\d{8}$/;
if( $enddate && $enddate !~ /^\d{14}$/ )
{
    print "Invalid value $enddate for --before - must be yyyymmdd or yyyymmddhhmmss\n";
    help(0);
}

if( $rebuild && ! $apply_level0 )
{
    $apply_level0 = 1;
    $apply_level5 = 1;
}

try
{
    my $options =
    {
        _configpath=>$cfgpath,
        _configextra=>$cfgext,
        verbose=>$verbose,
        max_level0_runtime_hours=>$l0_timeout,
        max_level5_runtime_hours=>$l5_timeout,
        override_locks => $override_locks,
        rebuild => $rebuild,
        apply_level0 => $apply_level0,
        apply_level0_inc => $apply_level0_inc,
        apply_level5 => $apply_level5,
        skip_postupload_tasks => $skip_postupload,
        keep_files => $keep_files,
        end_date => $enddate,
        maintain_db => $maintain_db,
        select_tables => join(' ',@ARGV),
        enable_hooks => $enable_hooks,
        log_level => $log_level,
    };

    my $cfg = new LINZ::Config($options);

    # turn off config logging if doing a dry run.
    my $layout = Log::Log4perl::Layout::PatternLayout->new("%d %p> %F{1}:%L - %m%n");
    if ($dry_run)
    {
        Log::Log4perl->easy_init( { level    => $INFO,
                                    file     => "STDOUT" } );
        $logger = get_logger("");
    }
    else
    {
        my $log_config = $cfg->has('log_settings') ?  $cfg->log_settings : "";
        if ($log_config)
        {
            if ( $log_config !~ /^[^#]?log4perl\.(root)?[Ll]ogger\s+\=\s+
                (FATAL|ERROR|WARN|INFO|DEBUG|TRACE|ALL)/mx )
            {
                die "log_setting within the configuration doesn't define a root logger\n";
            }
            Log::Log4perl->init(\$log_config);
            $logger = get_logger("");
        }
        else
        {
            Log::Log4perl->easy_init( { level    => $INFO,
                                        file     => "STDOUT" } );
            $logger = get_logger("");
        }

        if($listing_file)
        {
            my $file_appender = Log::Log4perl::Appender->new(
                "Log::Dispatch::FileRotate",
                name      => "listing_file_log",
                filename  => $listing_file,
                mode      => "append",
                min_level => 'debug',
                max       => 99,
            );
            $file_appender->layout($layout);
            $logger->add_appender( $file_appender );
        }
    }

    if($verbose)
    {
        my $stdout_appender = Log::Log4perl::Appender->new(
            "Log::Log4perl::Appender::Screen",
            name      => "verbose_screen_log",
        );
        $stdout_appender->layout($layout);
        $logger->add_appender($stdout_appender);
    }
    if (defined $log_level)
    {
        $logger->level($LOG_LEVELS{$log_level});
    }

    # Set default value for bde_tables_config
    if ( ! $cfg->has('bde_tables_config') ) {
      my $bde_tables_config = dirname($cfgpath) . '/tables.conf';
      $cfg->bde_tables_config( $bde_tables_config );
    }

    $upload = new LINZ::BdeUpload($cfg);
    if(!$dry_run)
    {
        $upload->RemoveZombiedJobs if $do_remove_zombie;
        $upload->PurgeOldJobs if $do_purge_old;
    }
    $upload->ApplyUpdates($dry_run);
}
catch
{
    if ($upload)
    {
        $upload->FireEvent('error');
        undef $upload;
    }
    Log::Log4perl->initialized() ? ERROR($_) : print STDERR $_;
    $status = 1;
};

INFO("Duration of job: ". runtime_duration());
exit $status;

sub runtime_duration
{
    my $duration = time() - $^T;
    my $str;
    my $day;
    my $hour;
    my $min;
    my $sec;
    {
        use integer;
        $min   = $duration / 60;
        $sec   = $duration % 60;
        $hour  = $min      / 60;
        $min   = $min      % 60;
        $day   = $hour     / 24;
        $hour  = $hour     % 24;
    }

    if ($day) {
        $str = sprintf("%dd:%02d:%02d:%02d",$day, $hour, $min, $sec);
    }
    else
    {
        $str = sprintf("%02d:%02d:%02d", $hour, $min, $sec);
    }
    return $str;
}

sub signal_handler
{
    if ($upload)
    {
        # TODO: might need to kill upload job first
        # $upload->KillUpload();
        $upload->FireEvent('error');
        undef $upload;
    }
    die ("Caught $_[0] signal: $!");
}

sub help
{
    my($full, $stream, $exitcode) = @_;
    my $level = $full ? 2 : 99;
    my $sections = 'Syntax';
    require Pod::Usage;
    Pod::Usage::pod2usage({
        -verbose=>$level,
        -sections=>$sections,
        -output=>$stream,
        -exitval=>'NOEXIT'
    });
    exit $exitcode;
}
__END__

=head1 linz_bde_uploader.pl

Script for updating a database with BDE files generated by Landonline.

=head1 Version

Version: @@VERSION@@

=head1 Syntax

  perl linz_bde_uploader.pl [options..] [tables..]

If no options are given a brief help message is displayed. At least one of the
-full, -incremental, -rebuild, -purge, -remove-zombie options must be supplied.
If tables are included, then only those tables will be updated.

The list of tables is optional and defines the subset of the tables that will
be updated.  Only tables defined in the configuration will be updated -
any additional tables listed are silently ignored(!)

Options:

=over

=item -config-path or -c I<cfgpath>

=item -config-extension or -x  I<cfgext>

=item -purge or -p

=item -remove-zombie or -z

=item -full or -f

=item -full-incremental or -j

=item -incremental or -i

=item -rebuild or -r

=item -before or -b yyyymmdd

=item -maintain-database or -m

=item -dry-run or -d

=item -full-timeout or -t I<timeout>

=item -inc-timeout or -u I<timeout>

=item -override-locks or -o

=item -skip-postupload-tasks

=item -listing_file or -l I<listing_file>

=item -keep-files or -k

=item -version

=item -verbose or -v

=item -log-level

=item -enable-hooks or -e

=item -help or -h

=back

=head1 Options

=over

=item -config-path or -c I<cfgpath>

Select the configuration file that will be used.  Default is
~/config/linz_bde_uploader.cfg, where ~ is the directory in which the
linz_bde_uploader.pl script is located.

=item -config-extension or -x  I<cfgext>

Extra configuration extension.  Overrides selected configuration items with
values from I<cfgpath>.I<cfgext>

=item -purge or -p

Purge old jobs from the database and file system.  The expiry times for
old jobs is defined in the configuration file.  This clears locks and
purges expired jobs from the system

=item -remove-zombie or -z

For jobs that are recorded as active but are no longer running this will release
the locks, delete working directories/temp schemas, and set the job status to
error

=item -full or -f

Apply any pending level 0 updates

=item -full-incremental or -j

Apply any pending level 0 updates not replacing table data, rather apply
differences between the current table data and the pending level 0 data. This
is useful when versioning is enabled on tables.

=item -incremental or -i

Apply any pending level 5 updates. If level 5 file is a full unload of the table
data, then differences will be calculated between the current table and the new,
file data. Those difference will then be applied as as an incremental update.

=item -rebuild or -r

Apply the last level 0 and any subsequent level 5 updates to rebuild the
tables.  If -rebuild and -full are specified, then only the last level 0
is loaded.

=item -dry-run or -d

Just list the updates that will be applied - don't actually make any changes.

=item -before or -b I<date>

Only use BDE files from before the specified date (entered as a string
yyyymmdd or yyyymmddhhmmss).  Used for testing or restoring to a previous date.

=item -maintain-database or -m

After a job has been successfully run and the database has been updated, the
database will be garbage collected and analysed

=item -full-timeout or -t I<timeout>

Specify the timeout in hours for the full (level 0) updates that are applied.
No level 0 jobs will be started after this time has expired.

=item -inc-timeout or -u I<timeout>

Specify the timeout in hours for incremental (level 5) updates.

=item -override-locks or -o

Override any existing locks on files when doing the update. This will
also override constraints on allowing concurrent uploads.

=item -skip-postupload-tasks

Choose not to run any postupload tasks defined for the schema.

=item -listing_file or -l I<listing_file>

Specifies a file for logging. This is in addition to other log appenders set in
the config file.

=item -keep-files or -k

Keeps the files generated during the upload rather than deleting them
for debugging use.

=item -enable-hooks or -e

Fire any defined command line hooks in the configuration.

=item -version

Print the version number for the software.

=item -log-level I<level>

Set the logging level for the software. Will override the defined value in the
config.  Only useful if logging is set in config or if the verbose or
-listing options are used.  I<level> can be one of the following values:
OFF, FATAL, ERROR, WARN, INFO, DEBUG, TRACE, ALL

=item -verbose or -v

Specifies that messages will be sent to standard output (as well as any other
log appenders set in the config)

=back
