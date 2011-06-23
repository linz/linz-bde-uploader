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
our $VERSION = '1.0.1';

use FindBin;
use lib $FindBin::Bin;
use Getopt::Long;
use Net::SMTP;

use LINZ::BdeUpload;
use LINZ::Config;

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

my $of;
if($listing_file)
{
    open($of, ">", $listing_file) ||
		die "Can't not write to listing file $listing_file: $!\n";
    select($of);
};

eval
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
    };

    my $cfg = new LINZ::Config($options);

    my $upload = new LINZ::BdeUpload($cfg);

    eval
    {

        $upload->PurgeOldJobs if $do_purge && ! $dry_run;

        $upload->ApplyUpdates($dry_run);
    };

    $upload->error($@) if $@;

    if( ! $dry_run )
    {
        my $messages = [];
        
        eval
        {
            $messages= $upload->GetMessages;
            sendMessages($cfg,$messages);
        };
        my $error = $@;
        push(@$messages,{message_time=>'',type=>'E',message=>$error}) if $error;
    
        if($verbose || $error)
        {
            print "\nUpload messages\n";
            foreach my $m (@$messages)
            {
                print join("\t",$m->{message_time},$m->{type},$m->{message}),"\n";
            }
        }
    }
};

if( $@ )
{
    # Error not trapped by normal messaging or not sent

    print $@;

};

close($of) if $of;


sub sendMessages 
{
    my ($cfg,$messages) = @_;
  
    my $logtypes = join('',map {$_->{type}} @$messages);
  
    my $msgtype;
    foreach my $mt (split(' ',$cfg->email_message_types) )
    {
        $mt =~ /^(\w+)\:(\w+)(?:\-(\w+))?/ || next;

        my ($type,$levels,$exclude) = ($1,$2,$3);
        next if ! $levels;
        next if $logtypes !~ /[$levels]/i;
        next if $exclude && $logtypes =~ /[$exclude]/i;
        sendMessageType($cfg,$type,$messages);
    }
}


sub messageText
{
    my( $messages, $options ) = @_;
    my $showtimes = $options =~ /T/i;

    my $text = '';
    foreach my $m (@$messages)
    {
        next if $m->{type} !~ /[$options]/i;
        $text .= $m->{message_time}."\t".$m->{type}."\t" if $showtimes;
        $text .= $m->{message}."\n";
    }
    return $text;
}

sub sendMessageType
{
    my( $cfg, $msgtype, $messages ) = @_;

    $msgtype .= '_email_';

    my $smtpserver = $cfg->smtpserver;
    my $fromuser = $cfg->smtpsender;
    my $from = $cfg->smtpsendername." <$fromuser>";
    my $to = $cfg->get($msgtype."address");
    my $subject = $cfg->get($msgtype."subject","linz_bde_uploader log");
    my $text = $cfg->get($msgtype."template","{log:EWIT}");
  
    $text =~ s/\{log\:(\w+)\}/messageText($messages,$1)/eig;
	$text =~ s/\{\_runtime_duration\}/runtime_duration()/eg;
  
    my $smtp = Net::SMTP->new($smtpserver) if $smtpserver ne 'none';
    if (!$smtp)
    {
       print "Unable to connect to smtp server $smtpserver\n\n" 
          if $smtpserver ne 'none';
  
       print "Log file not sent - no SMTP server\n",
          "To: $to\n",
          "Subject: ",$subject,"\n\n",
    $text;
       return;
    }
  
  
    my @to = map { s/^\s+//;s/\s+$//; $_ } split(/\;/, $to );
    if( $smtp )
	{
       $smtp->mail($fromuser);
       $smtp->to(@to,{SkipBad=>1});
       $smtp->data(); 
       $smtp->datasend("To: $to\n");
       $smtp->datasend("From: $from\n");
       $smtp->datasend("Subject: $subject\n");
	   $smtp->datasend("\n");
       $smtp->datasend($text);
       $smtp->dataend();
       $smtp->quit();
    }
}

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

Specifies a file for reporting.  Most reporting is written to the database and
sent as email notifications as defined in the configuration.  
If the verbose option is specified, or if the
email server is unavailable, then this may be sent to the standard output
file.  The I<listing_file> can be used in place of standard output.

=item -keep-files or -k

Keeps the files generated during the upload rather than deleting them - 
for debugging use.

=item -verbose or -v

Specifies that messages will be sent to standard output (or the report file)
as well as to the database.


=back
