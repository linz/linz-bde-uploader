#!usr/bin/perl
################################################################################
#
# $Id$
#
# linz_bde_loader -  LINZ BDE loader for PostgreSQL
#
# Copyright 2011 Crown copyright (c)
# Land Information New Zealand and the New Zealand Government.
# All rights reserved
#
# This program is released under the terms of the new BSD license. See the 
# LICENSE file for more information.
#
################################################################################
# Script to upload BDE files to the postgres database.  Reads options from
# linz_bde_loader.conf located in the configuration directory
################################################################################
use strict;  

# TODO need to update this from git describe
our $VERSION = '1.5.3';

use FindBin;
use lib $FindBin::Bin;
use lib '../lib';
use Getopt::Long;
use Log::Log4perl qw(:easy :levels get_logger);
use Log::Log4perl::Layout;
use Try::Tiny;

use LINZ::BdeUpload;
use LINZ::Config;

$SIG{INT}  = \&signal_handler;
$SIG{TERM} = \&signal_handler;

@ARGV || help(0);

# Main program controls

my $do_purge = 0;      # Clean up old jobs if set
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
my $logger;
my $upload;

GetOptions (
    "help|h" => \$showhelp,
    "config-extension|x=s" => \$cfgext,
    "config-path|c=s" => \$cfgpath,
    "purge|p!" => \$do_purge,
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
    )
    || help(0);

help(1) if $showhelp;

if($apply_level0_inc && !$apply_level0)
{
    $apply_level0 = 1;
}

if( ! $apply_level0 && ! $apply_level5 && ! $do_purge && ! $rebuild)
{ 
    print "Need at least one option of -full, -incremental, or -purge\n";
    help(0);
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
    };

    my $cfg = new LINZ::Config($options);
    
    # turn off config logging if doing a dry run.
    my $layout = Log::Log4perl::Layout::PatternLayout->new("%d %p> %F{1}:%L - %m%n");
    if ($dry_run)
    {
        Log::Log4perl->easy_init($INFO);
        $logger = get_logger("");
    }
    else
    {
        my $log_config = $cfg->log_settings;
        Log::Log4perl->init(\$log_config);
        $logger = get_logger("");
        
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
            DEBUG("File logging turned on");
            #Log::Log4perl::Logger::reset_all_output_methods();
        }
    }
    
    if($verbose || $dry_run)
    {
        my $stdout_appender = Log::Log4perl::Appender->new(
            "Log::Log4perl::Appender::Screen",
            name      => "verbose_screen_log",
        );
        $stdout_appender->layout($layout);
        $logger->add_appender($stdout_appender);
    }
    
    $upload = new LINZ::BdeUpload($cfg);
    $upload->PurgeOldJobs if $do_purge && ! $dry_run;
    $upload->ApplyUpdates($dry_run);
}
catch
{
    if ($upload)
    {
        $upload->FireEvent('error');
        undef $upload;
    }
    ERROR($_);
};

INFO("Duration of job: ". runtime_duration());
exit;

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
    my($full) = @_;
    my $level = $full ? 2 : 99;
    my $sections = 'Syntax';
    require Pod::Usage;
    Pod::Usage::pod2usage({
        -verbose=>$level,
        -sections=>$sections,
        -exitval=>'NOEXIT' 
    });
    exit;
}
__END__

=head1 linz_bde_loader.pl

Script for updating a database with BDE files generated by Landonline.

=head1 Version

Version: $Id$

=head1 Syntax

  perl linz_bde_loader.pl [options..] [tables..]

If no options are a brief help message is displayed. At least one of the 
-full, -incremental, -rebuild, or -purge options must be supplied.  If tables
are included, then only those tables will be updated.

The list of tables is optional and defines the subset of the tables that will
be updated.  Only tables defined in the configuration will be updated - 
any additional tables listed are silently ignored(!)

Options:

=over

=item -config-path or -c I<cfgpath>

=item -config-extension or -x  I<cfgext>

=item -purge or -p

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

=item -verbose or -v

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
values from bde.cfg.I<cfgext> 

=item -purge or -p

Purge old jobs from the database and file system.  The expiry times for 
old jobs is defined in the configuration file.  This clears locks and 
purges expired jobs from the system

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

=item -verbose or -v

Specifies that messages will be sent to standard output (as well as any other
log appenders set in the config)

=back
