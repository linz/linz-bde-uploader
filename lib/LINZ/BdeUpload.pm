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
use Log::Log4perl qw(:easy);

use Try::Tiny;

package BdeUploadTableDef;

use fields qw{ table id key_column row_tol_error row_tol_warning levels files columns level5_is_full };

sub new
{
    my ($class,$table,$id) = @_;
    my $self = fields::new($class);
    $self->{table} = $table;
    $self->{levels} = {'0'=>1,'5'=>1};
    $self->{files} = [];
    $self->{columns} = [];
    $self->{level5_is_full} = 0;
    $self->{key_column} = undef;
    $self->{row_tol_error} = undef;
    $self->{row_tol_warning} = undef;
    $self->{id} = $id;
    return $self;
}

sub set_levels
{
    my($self,@levels) = @_;
    @levels = @{$levels[0]} if ref $levels[0];
    $self->{levels} = {map { $_=>1 } grep { /^[05C]$/ } @levels};
}

sub add_files
{
    my($self,@files) = @_;
    @files = @{$files[0]} if ref $files[0];
    my @f = map {split} @files;
    push(@{$self->{files}},@f);
}

sub add_columns
{
    my($self,@columns) = @_;
    @columns = @{$columns[0]} if ref $columns[0];
    push(@{$self->{columns}},@columns);
}

sub set_level5_is_full { $_[0]->{level5_is_full} = defined($_[1]) ? $_[1] : 1; }
sub set_key_column { $_[0]->{key_column} = $_[1]; }
sub set_row_tol_error { $_[0]->{row_tol_error} = $_[1]; }
sub set_row_tol_warning { $_[0]->{row_tol_warning} = $_[1]; }

sub name { return $_[0]->{table}; }
sub id { return $_[0]->{id}; }
sub files { return wantarray ? @{$_[0]->{files}} : $_[0]->{files}; }
sub columns { return wantarray ? @{$_[0]->{columns}} :$_[0]->{columns}; }
sub level5_is_full { return $_[0]->{level5_is_full}; }
sub key_column { return $_[0]->{key_column}; }
sub row_tol_error { return $_[0]->{row_tol_error}; }
sub row_tol_warning { return $_[0]->{row_tol_warning}; }

sub levels
{
    my @levels = sort keys %{$_[0]->{levels}};
    return wantarray ? @levels : \@levels;
}

sub is_l0table { return $_[0]->{levels}->{0}; }
sub is_l5table { return $_[0]->{levels}->{5}; }
sub is_l5change { return $_[0]->{levels}->{C}; }

sub is_available_in_dataset
{
    my($self,$dataset) = @_;
    my $level = $dataset->level;
    return (0,$self->files) if ! $self->{levels}->{$level};
    my @missing = ();
    foreach my $f ($self->files) { push(@missing,$f) if ! $dataset->has_file($f); }
    return (@missing ? 0 : 1, \@missing);
}

# #####################################################################

package BdeUploadDatasetDef;

use fields qw{ config_file tables };

use Log::Log4perl qw(:easy);
use Try::Tiny;

sub new
{
    my($class,$config_file) = @_;
    my $self = fields::new($class);
    $self->{tables} = [];
    $self->_read_config($config_file) if $config_file;
    return $self;
}

sub _report_config_error
{
    my($self,@message) = @_;
    LOGEXIT("Error reading BDE upload dataset configuration from ",
        $self->{config_file}, "\n", @message)
}

sub _config_error
{
    my($self,$errors,$fh,$message) = @_;
    push(@$errors,"Line ".$fh->input_line_number.": $message\n");
}

sub _read_config
{
    my ($self,$config_file) = @_;
    $self->{config_file} = $config_file;
    $self->{tables}=[];

    open(my $in, "<$config_file") || $self->_report_config_error("Cannot open file:$!");
    my $table;
    my $errors = [];
    my %tables = ();
    my $id = 0;
    while(<$in>)
    {
        next if /^\s*(\#|$)/;
        my ($command,@values) = split;
        $command = lc($command);
        if( $command eq 'table' )
        {
            my $name = shift(@values);
            $name = lc($name);
            $id++;
            $table = new BdeUploadTableDef($name,$id);
            while(my $v = shift(@values))
            {
                $v = lc($v);
                last if $v =~ /^files?$/;
                $table->set_levels('0') if $v eq 'l0_only';
                $table->set_levels('5') if $v eq 'l5_only';
                $table->set_level5_is_full if $v eq 'l5_is_full';
                if($v =~ /^(key)\=(\S+)$/ )
                {
                    my $value = $2;
                    $table->set_key_column($value);
                }
                if($v =~ /^(row_tol)\=(\S+)\,(\S+)$/ )
                {
                    my $real_re = qr/^\s*(?:\d+(?:\.\d*)?|\.\d+)\s*$/;
                    my $error_tol = $2;
                    my $warn_tol = $3;
                    $self->_config_error($errors,$in,"Error tolerance is not valid for table $name")
                        if $error_tol !~ $real_re || $error_tol > 1;
                    $self->_config_error($errors,$in,"Warning tolerance is not valid for table $name")
                        if $warn_tol !~ $real_re || $warn_tol > 1;
                    $table->set_row_tol_error($error_tol);
                    $table->set_row_tol_warning($warn_tol);
                }
            }
            $table->set_levels('C') if $name eq 'l5_change_table';
            $self->_config_error($errors,$in,"No files defined for table $name")
                if ! @values;
            $table->add_files(@values);
            push(@{$self->{tables}},$table);
            foreach my $l ($table->levels)
            {
                my $tl = "$name level $l";
                $self->_config_error($errors,$in,"Definition for $tl repeated")
                    if $tables{$tl};
                $tables{$tl} = 1;
            }
        }
        elsif( $table && $command eq 'column' )
        {
            $table->add_columns(join(' ',@values));
        }
        else
        {
            $self->_config_error($errors,$in,"Invalid or out of sequence command $command");
        }
    }
    close($in);
    if( @$errors )
    {
        my $nerror = @$errors;
        $self->_report_config_error("$nerror errors reading file\n",@$errors);
    }
}

sub tables { return wantarray ? @{$_[0]->{tables}} : $_[0]->{tables} }

sub is_available_in_dataset
{
    my($self,$dataset) = @_;
    my @missing = ();
    foreach my $t ( @{$self->{tables}} )
    {
        my ($status, $temp) = $t->is_available_in_dataset($dataset);
        push @missing, @$temp if @$temp;
    }
    return (@missing ? 0 : 1, \@missing);
}

sub _subset_clone
{
    my($self,@tables) = @_;
    my $clone = fields::new(ref($self));
    $clone->{config_file}=$self->{config_file};
    $clone->{tables} = \@tables;
    return $clone;
}

sub subset
{
    my( $self, @tables ) = @_;
    @tables = @{$tables[0]} if ref $tables[0];
    my %tables = map { lc($_) => 1 } @tables;
    my @subset = grep { $tables{lc($_->name) } } $self->tables;
    return $self->_subset_clone(@subset);
}

sub excluding
{
    my( $self, @tables ) = @_;
    @tables = @{$tables[0]} if ref $tables[0];
    my %tables = map { lc($_) => 1 } @tables;
    my @subset = grep { ! $tables{lc($_->name) } } $self->tables;
    return $self->_subset_clone(@subset);
}

sub level0_subset
{
    my ($self) = @_;
    return $self->_subset_clone( grep {$_->is_l0table} $self->tables );
}

sub level5_subset
{
    my ($self) = @_;
    return $self->_subset_clone( grep {$_->is_l5table} $self->tables );
}

sub level0_tables { return $_[0]->level0_subset->tables; }
sub level5_tables { return $_[0]->level5_subset->tables; }

sub level5_change_table
{
    my ($self) = @_;
    my ($change_table) = grep {$_->is_l5change} $self->tables;
    return $change_table
}

sub table
{
    my ($self,$name) = @_;
    $name = lc($name);
    foreach my $t ($self->tables){ return $t if $t->name eq $name; }
    return undef;
}

# ###################################################################

package BdeUploadSet;
use fields qw/datasets tables dstables/;

sub new
{
    my($class) = @_;
    my $self = fields::new($class);
    $self->{datasets} = {};
    $self->{tables} = {};
    $self->{dstables} = {};
    return $self;
}

sub empty
{
    my($self) = @_;
    return %{$self->{datasets}} ? 0 : 1;
}

sub add
{
    my($self,$dataset,$table) = @_;
    $self->{datasets}->{$dataset->name} = $dataset;
    $self->{tables}->{$table->name} = $table;
    push(@{$self->{dstables}->{$dataset->name}},$table);
}

sub datasets
{
    my ($self) = @_;
    my @datasets = sort {$a->name cmp $b->name} values %{$self->{datasets}};
    return wantarray ? @datasets : \@datasets;
}

sub tables
{
    my($self,$dataset) = @_;
    my @tables = $dataset ?
        @{$self->{dstables}->{$dataset->name}} :
        values %{$self->{tables}};
    @tables =  sort {$a->id <=> $b->id} @tables;
    return wantarray ? @tables : \@tables;
}

sub last_dataset_for
{
    my($self,$table) = @_;
    foreach my $d (reverse $self->datasets)
    {
        foreach my $t( $self->tables($d) )
        {
            return $d if $t->name eq $table->name;
        }
    }
    return undef;
}

sub print
{
    my($self) = @_;

    my $none = 1;
    foreach my $d ($self->datasets)
    {
        print "Dataset ",$d->name,":\n";
        foreach my $t ($self->tables($d))
        {
            print "  ",$t->name,"\n";
            $none = 0;
        }
    }
    print "No updates are required\n" if $none;
}

# ###################################################################

package LINZ::BdeUpload;

use Try::Tiny;

use LINZ::BdeDatabase;
use LINZ::Bde;

use Log::Log4perl qw(:easy);

use IO::Handle;
use File::Path;
use File::Spec;
use Date::Calc;

use fields qw{ cfg db repository tables tmp_base tmp fid current_dataset
               timeout timeout_message dbl0updated dbupdated error_message
               keepfiles changetable jobfinished current_level event_hooks
               upload_id};

our $tmp_prefix='tmp_bde_upload_';

sub new
{
    my($class,$cfg) = @_;

    my $self = fields::new($class);
    $self->{cfg} = $cfg;

    # Load the tables configuration and process any inclusions/exclusions

    my $tables = new BdeUploadDatasetDef($cfg->bde_tables_config);
    my $changetable = $tables->level5_change_table;

    # Override for command line selection
    if( $cfg->select_tables('') =~ /\S/ )
    {
        my @requested = split(' ',$cfg->select_tables);
        $tables = $tables->subset(@requested);
        foreach my $t (@requested )
        {
            WARN("No definition is available for requested table $t")
                if ! $tables->table($t);
        }
    }
    else
    {
        if( $cfg->include_tables('') =~ /\S/)
        {
            $tables = $tables->subset(split(' ',$cfg->include_tables));
        }
        if( $cfg->exclude_tables('') =~ /\S/)
        {
            $tables = $tables->excluding(split(' ',$cfg->_exclude_tables));
        }
    }

    $self->{tables} = $tables;
    $self->{changetable} = $changetable;

    # Load any event hooks
    if ($cfg->enable_hooks)
    {
        foreach my $event (qw(start finish error start_dataset finish_dataset))
        {
            my $cfg_key = "${event}_event_hooks";
            my @hooks = map { s/^[ \t]+|[ \t]+$//g; $_} split("\n", $cfg->$cfg_key);
            if (@hooks) {
                $self->{event_hooks}->{$event} = \@hooks;
            }
        }
    }

    # Set up the repository and the database

    $self->{db} = new LINZ::BdeDatabase( $cfg );
    if ( $cfg->has('application_name') ) {
        # Rely on libpq default otherwise (PGAPPNAME)
        $self->{db}->setApplication($cfg->application_name());
    }

    $self->{repository} = new LINZ::BdeRepository( $cfg->bde_repository );

    # Check for the base scratch directory - create it if it doesn't exist

    my $scratch = $cfg->tmp_base_dir('/tmp');
    if( ! -d $scratch )
    {
        mkpath($scratch);
        die ("Cannot create temporary working folder $scratch") if ! -d $scratch;
        chmod 0755, $scratch;
    }
    $self->{tmp_base} = File::Spec->rel2abs($scratch);

    # Id for working files to ensure unique filenames.

    $self->{fid} = 0;

    $self->{dbupdated} = 0;
    $self->{dbl0updated} = 0;
    $self->{timeout} = 0;
    $self->{jobfinished} = 0;
    $self->{current_dataset} = undef;
    $self->{current_level} = undef;
    $self->{keepfiles} = $cfg->keep_files('') ? 1 : 0;

    return $self;
}

sub DESTROY
{
    my ($self) = @_;

    # Remove the temporary directory if we have created it.
    my $tmp = $self->{tmp};
    rmtree($tmp) if $tmp && ! $self->{keepfiles};
}


sub cfg { return $_[0]->{cfg}};
sub db { return $_[0]->{db}; }
sub tables { return $_[0]->{tables}; }
sub repository { return $_[0]->{repository}; }
sub fid { $_[0]->{fid}++; return $_[0]->{fid}; }

sub tmp
{
    my($self) = @_;
    return $self->{tmp} if defined $self->{tmp};

    my $id = $self->db->uploadId;
    my $tmp = $self->{tmp_base}."/".$tmp_prefix.$id;

    mkpath($tmp);
    die ("Cannot create working directory $tmp") if ! -d $tmp;
    chmod 0755, $tmp;

    $self->{tmp} = $tmp;
    return $tmp;
}

sub _clean_scratch_dirs
{
    my $self = shift;
    my $scratch = $self->{tmp_base};
    my $prefix = "$scratch/$tmp_prefix";
    foreach my $dir (glob($prefix."*"))
    {
        next if ! -d $dir;
        my $job = substr($dir,length($prefix));
        next if $job !~ /^\d+$/;
        $job += 0;
        next if $self->db->uploadIsActive($job);
        rmtree($dir) if ! $self->{keepfiles};
    }
}

sub RemoveZombiedJobs
{
    my($self) = @_;
    my $db = $self->db;
    DEBUG("Removing zombied jobs");
    my $count = $db->releaseExpiredLocks(0);
    if ($count)
    {
        INFO("Cleaned up $count zombied jobs");
    }
    $self->_clean_scratch_dirs;
}

sub PurgeOldJobs
{
    my($self) = @_;

    my $db = $self->db;

    my $expiry_time = $self->cfg->lock_expiry_hours;
    $db->releaseExpiredLocks($expiry_time) if $expiry_time;

    my $job_expiry_time = $self->cfg->job_record_expiry_days;
    $db->removeOldJobData($job_expiry_time);
    $self->_clean_scratch_dirs;
}

sub SetTimeout
{
    my( $self, $hours, $message ) = @_;
    if( $hours > 0 )
    {
        $self->{timeout} = time() + $hours*3600;
        $self->{timeout_message} = $message || "Process has timed out";
    }
    else
    {
        $self->{timeout} = 0;
    }
}

sub CheckTimeout
{
    my($self) = @_;
    my $t = $self->{timeout};
    if( $t && $t < time() )
    {
        my $message = $self->{timeout_message};
        die ($message);
    }
}

sub ApplyUpdates
{
    my($self,$dry_run) = @_;
    my $updates_applied;
    my $errors;
    try
    {
        my $updates = new BdeUploadSet();

        if( $self->cfg->apply_level0(0) )
        {
            $self->GetLevel0Updates($updates);
        }
        if( $self->cfg->apply_level5(0) )
        {
            $self->GetLevel5Updates($updates);
        }

        # Apply updates

        if( $dry_run )
        {
            print "Dataset updates\n";
            $updates->print;
        }
        else
        {
            if (! $updates->empty)
            {
                $self->ApplyDatasetUpdates($updates);
                $updates_applied = 1;
                $self->ApplyPostUploadFunctions;
            }
            else
            {
                INFO("No dataset updates to apply");
            }
        }
    }
    catch
    {
        die "Apply Updates Failed: " . $_;
    }
    finally
    {
        if ( !$dry_run )
        {
            $self->FinishJob($_[0]);
        }
    };
    return 1;
}

sub GetLevel0Updates
{
    my($self, $updates) = @_;

    my $db = $self->db;

    my $enddate = $self->cfg->end_date('');

    my @datasets = $self->repository->level0->datasets;
    if( $enddate ) { @datasets = grep {$_->name lt $enddate} @datasets; }

    if (! @datasets)
    {
        INFO ("No level 0 uploads available");
        return;
    }

    my $dataset = $datasets[-1];

    my $rebuild = $self->cfg->rebuild(0);

    if ( $self->cfg->require_all_dataset_files(1) )
    {
        my $l0_tableset = $self->tables->level0_subset;
        my ($avail, $missing) = $l0_tableset->is_available_in_dataset($dataset);
        if( !$avail )
        {
            die ("Last available Level 0 dataset is not complete. ",
                "The following files are missing: ",
                join(", ",@$missing), " in dataset ", $dataset->name);
        }
    }

    foreach my $t ( $self->tables->level0_tables )
    {
        my $lastl0 = $db->lastUploadStats($t->name)->{last_level0_dataset};
        $updates->add($dataset,$t) if $rebuild || $lastl0 lt $dataset->name;
    }
    return $updates;
}

sub GetLevel5Updates
{
    my($self,$l0updates) = @_;

    my $db = $self->db;

    my $enddate = $self->cfg->end_date('');

    my $l5repository = $self->repository->level5;

    my $rebuild = $self->cfg->rebuild(0);

    my %complete_datasets;
    my $l5_tableset = $self->tables->level5_subset;
    my $require_all = $self->cfg->require_all_dataset_files(1);
    foreach my $t ($l5_tableset->tables )
    {
        my $lastl5;
        if ($l0updates && $rebuild)
        {
            my $ds = $l0updates->last_dataset_for($t);
            $lastl5 = $ds->name if $ds;
        }
        $lastl5  = $db->lastUploadStats($t->name)->{last_upload_dataset}
            if ! $lastl5;

        if( $lastl5 eq '' )
        {
            ERROR("Cannot load incremental updates to ".$t->name.
                " as there is no previous upload");
            next;
        }
        my @datasets = $l5repository->after($lastl5)->datasets;
        if( $enddate ) { @datasets = grep {$_->name lt $enddate} @datasets; }
        @datasets = ($datasets[-1]) if $t->level5_is_full && @datasets;

        foreach my $d ( sort {$a->name cmp $b->name} @datasets )
        {
            if ( !exists $complete_datasets{$d} )
            {
                my ($avail, $missing) = $l5_tableset->is_available_in_dataset($d);
                $complete_datasets{$d} = $avail;
                WARN(
                    "Level 5 dataset " , $d->name, " is not complete",
                    $require_all ? " and will not be loaded" : "",
                    ". The following files are missing: ",
                    join(", ",@$missing)
                ) if ! $avail;
            }
            last if $require_all && ! $complete_datasets{$d};
            $l0updates->add($d,$t);
        }
    }
    return $l0updates;
}

sub ApplyPostLevel0Functions
{
    my($self) = @_;
    $self->db->applyPostLevel0Functions if $self->{dbl0updated};
    $self->{dbl0updated} = 1;
}

sub ApplyDatasetUpdates
{
    my($self,$uploadset) = @_;

    my $db = $self->db;
    $self->{upload_id} = $self->db->uploadId;
    $self->FireEvent('start');

    my $changetable = $self->{changetable};

    # Record status of each table
    my $tablestate = {};

    foreach my $dataset ( sort {$a->name cmp $b->name} $uploadset->datasets )
    {
        my $load_type  = "level " . $dataset->level;
        my $is_level_0_ds = $dataset->level == '0';

        $self->CheckTimeout;
        my $timeout;
        if ( $is_level_0_ds )
        {
            $timeout = $self->cfg->max_level0_runtime_hours;
        }
        else
        {
            $timeout = $self->cfg->max_level5_runtime_hours;
        }
        $self->SetTimeout($timeout,"$load_type updates have timed out");

        INFO("Applying ",$dataset->name," $load_type update (job ",$self->{upload_id},")");

        $self->db->beginDataset($dataset->name);
        $self->{current_dataset} = $dataset->name;
        $self->{current_level} = $dataset->level;
        $self->FireEvent('start_dataset');

        my $change_table_name;

        my @loadtables = ();
        my $need_change_table = 0;

        foreach my $table ($uploadset->tables($dataset))
        {
            my $tablename = $table->name;
            # add tables that have a clean state into the @loadtables array
            # for processing
            if(! $tablestate->{$tablename})
            {
                push(@loadtables,$table);
                $need_change_table = 1 if !$is_level_0_ds && !$table->level5_is_full;
            }
            # tablestate accumulates failed uploads.  This will
            # be cleared if the table is successfully uploaded.
            $tablestate->{$tablename} .= "|".$dataset->name;
        }

        if ( $need_change_table )
        {
            if ( $changetable )
            {
                $change_table_name =
                    $self->CreateLevel5ChangeTable($dataset,$changetable);
            }
            else
            {
                die("Configuration error: missing required changetable " .
                     "for incremental update");
            }
        }

        foreach my $table ( @loadtables )
        {
            $self->CheckTimeout;
            my $tablename = $table->name;
            try
            {
                $self->UploadTable($dataset,$table);
                $tablestate->{$tablename} = '';
            }
            catch
            {
                my $msg = "Failed to load $load_type update for ".
                    $tablename. " from ". $dataset->name. ': '. $_;
                die $msg;
            };
        }

        $db->dropWorkingCopy($change_table_name) if $change_table_name;

        $self->db->endDataset($dataset->name);
        $self->FireEvent('finish_dataset');
        $self->{current_dataset} = undef;
        $self->{current_level} = undef;

        if ( $is_level_0_ds )
        {
            if( $self->cfg->skip_postupload_tasks )
            {
                WARN("Post level0 upload tasks have not been run by user choice");
            }
            else
            {
                $self->ApplyPostLevel0Functions;
            }
        }
    }

    # Record any unreported table errors.
    foreach my $tablename (keys %$tablestate )
    {
        my $state = $tablestate->{$tablename};
        next if $state eq '' || $state eq '|';
        my @dsnames = split(/\|/,substr($state,1));
        my $dsname0 = shift(@dsnames);
        ERROR("Failed to load update for ",$tablename,
            " from ", $dsname0)
            if $dsname0;
        ERROR("Updates of $tablename from ",join(", ",@dsnames),
            " where bypassed due to previous error for that table")
            if @dsnames;
    }

    return 1;
}

sub ApplyPostUploadFunctions
{
    my($self) = @_;
    return if ! $self->{dbupdated};
    if( $self->cfg->skip_postupload_tasks )
    {

        WARN("Post upload tasks have not been run by user choice");
    }
    else
    {
        $self->db->applyPostUploadFunctions;
    }
}

sub FinishJob
{
    my $self = shift;
    my $error = shift;
    $self->db->finishJob($error);
    if (!$error)
    {
        $self->FireEvent('finish');
    }
    $self->{jobfinished} = 1;
    if ( $self->{dbupdated} && $self->cfg->maintain_db )
    {
        $self->db->maintain;
    }
    $self->{dbupdated} = 0;
}

sub CreateLevel5ChangeTable
{
    my($self,$dataset,$table) = @_;

    $self->CheckTimeout();

    my $db = $self->db;
    my $temp = 1;
    $db->beginTable("");
    my $tablename = $db->createL5ChangeTable($temp);
    die ("Cannot create L5 change table") if ! $tablename;

    my @files = $table ? $table->files : ();

    try
    {
        foreach my $file (@files)
        {
            $self->CheckTimeout();
            $self->LoadFile($dataset,$tablename,$file);
        }
    }
    catch
    {
        die "Cannot load change table for dataset ", $dataset->name, ': ', $_;
    }
    finally
    {
        $db->endTable("");
        $db->dropWorkingCopy($tablename) if ! $temp;
    };
    return $tablename;
}


sub UploadTable
{
    my($self,$dataset,$table) = @_;

    $self->CheckTimeout();

    INFO("Loading ",$table->name," from dataset ", $dataset->name);
    my ($available,$missing) = $table->is_available_in_dataset($dataset);
    if( !$available )
    {
        die ("The files ",join(", ",@$missing)," required to update ",$table->name,
            " are not available in dataset ",$dataset->name);
    }


    my $db = $self->db;
    my $tablename = $table->name;
    my $is_level0 = $dataset->level eq '0' || $table->level5_is_full;

    try
    {
        $db->addTable(
            $tablename,
            $table->key_column,
            $table->row_tol_error,
            $table->row_tol_warning
        );

        $db->beginTable($tablename) ||
            die ("Cannot acquire upload lock for $tablename");

        # If this is a level 0 update, then need the last update details
        # in order to check that the start time of the current update
        # matches the end time.

        my %lastdetails = ();
        if( ! $is_level0 )
        {
            my $stats = $db->lastUploadStats($tablename);
            my $details = '';
            $details = $stats->{last_upload_details}
                if $stats->{last_upload_type} eq '5';

            if( $details =~ /^BdeUpload(\s+\S+\s+\d{4}\-\d\d\-\d\d\s+\d\d\:\d\d\:\d\d)+\s*$/ )
            {
                while( $details =~ /(\S+)\s+(\d{4}\-\d\d\-\d\d\s+\d\d\:\d\d\:\d\d)/g )
                {
                    $lastdetails{lc($1)} = $2;
                }
            }
        }

        my $create_temp = $self->cfg->apply_level0_inc || $dataset->level eq '5' || 0;
        $db->createWorkingCopy($tablename,  $create_temp )
            || die ("Cannot create working copy of table ", $table->name);
        my $details = 'BdeUpload';
        my $bdedate = '';
        foreach my $file ($table->files)
        {
            $self->CheckTimeout();

            my $enddate = $self->LoadFile($dataset,$tablename,$file,$lastdetails{lc($file)});
            $details .=" $file $enddate";
            $bdedate = $enddate if $enddate gt $bdedate;
        }

        $self->CheckTimeout();

        if( $is_level0 )
        {
            INFO('Applying level 0 update '. $dataset->name. ' into table '. $tablename);
            my $is_incremental = $self->cfg->apply_level0_inc || $table->level5_is_full;
            $db->applyLevel0Update($tablename,$bdedate,$details, $is_incremental)
                || die ("Cannot apply level ",$dataset->level,
                    " update for ",$tablename," in ",$dataset->name);
            $self->{dbl0updated} = 1;
        }
        else
        {
            INFO('Applying level 5 update '. $dataset->name. ' into table '. $tablename);
            $db->applyLevel5Update($tablename,$bdedate,$details, $self->cfg->fail_if_inconsistent_data(1))
                || die ("Cannot apply level ",$dataset->level,
                    " update for ",$tablename," in ",$dataset->name);
        }
        $self->{dbupdated} = 1;
    }
    catch
    {
        die $_;
    }
    finally
    {
        # Ensure resources are released
        try { $db->dropWorkingCopy($tablename) };
        try { $db->endTable($tablename) };
    }
}

sub LoadFile
{
    my ($self,$dataset,$tablename,$file,$checktime) = @_;

    my $db = $self->db;

    my $result = '';
    my $reader = $dataset->open($file);
    my $tabledatafh;

    INFO("Loading file ",$file," from dataset ",$dataset->name);
    try
    {
        $self->CheckStartDate($dataset,$file,$reader->start_time,$checktime)
            if $checktime;

        # Determine which columns are to be copied
        my $columns = join("|",$reader->fields);

        $columns = $db->selectValidColumns($tablename,$columns);
        $reader->output_fields(split(/\|/,$columns));

        # Open data file (throw on failure)
        $tabledatafh = $self->_OpenDataFile($dataset,$reader);

        # Stream data to the database
        $db->streamDataToTempTable($tablename, $tabledatafh, $columns)
            || die "Error streaming data to ",$tablename;
        DEBUG("Loaded data file into working table $tablename");
        # As $tabledatafh could be a pipe, here
        # we check return status
        $? = 0;
        close($tabledatafh);
        if ($?) {
            die "Command used to output datafile failed";
        }
    }
    catch
    {
        die $_;
    }
    finally
    {
        try
        {
            $reader->close;
        };
    };

    return $reader->end_time;
}


sub CheckStartDate
{
    my ($self,$dataset,$file,$starttime,$checktime) = @_;
    return if $starttime eq $checktime;

    my $warntol = $self->cfg->level5_starttime_warn_tolerance(0);
    my $failtol = $self->cfg->level5_starttime_fail_tolerance(0);
    my $re = qw/^\d{4}\-\d\d\-\d\d\s+\d\d\:\d\d\:\d\d$/;

    if( $starttime !~ $re || $checktime !~ $re )
    {
        WARN("Cannot check start time of $file of ",
            $dataset->name," level 5 update");
        return;
    }

    my $start = Date::Calc::Mktime($starttime =~ /(\d+)/g);
    my $end = Date::Calc::Mktime($checktime =~ /(\d+)/g);
    my $diff = abs($start-$end)/3600.0;
    if( $failtol &&  $diff > $failtol )
    {
        die ("Start time $starttime in $file of dataset ",$dataset->name,
            " differs from previous end time $checktime by more than $failtol hours");
    }
    if( $warntol && $diff > $warntol )
    {
        WARN(
            "Start time $starttime in $file of dataset ",$dataset->name,
            " differs from previous end time $checktime by more than $warntol hours");
    }
}

sub FireEvent
{
    my ($self, $event) = @_;

    # if no upload id is defined then don't fire the event
    if (! $self->{upload_id}) {
        return;
    }
    if (! exists $self->{event_hooks}->{$event})
    {
        DEBUG("Event does not exist");
        return;
    }
    my $hooks = $self->{event_hooks}->{$event};
    if (@$hooks)
    {
        foreach my $event_hook (@$hooks)
        {
            my $pid = $$;
            my $upload_id = $self->{upload_id};
            my $dataset = $self->{current_dataset} || 'undef';
            my $level = $self->{current_level};
            if (!defined $level)
            {
                $level = 'undef';
            }
            $event_hook =~ s/\{pid\}/$pid/g;
            $event_hook =~ s/\{id\}/$upload_id/g;
            $event_hook =~ s/\{level\}/$level/g;
            $event_hook =~ s/\{dataset\}/$dataset/g;
            INFO("Running $event hook: " . $event_hook);
            my $event_output = qx($event_hook 2>&1);
            my $rv=$?;
            $rv = ($rv == -1 ? $rv : $rv>>8);  # see system()
            INFO("Event $event hook result: $rv\n" . $event_output);
            if ($rv != 0)
            {
                ERROR("Failed to run $event hook: " . $event_hook . ". Return status $rv. Output:\n" . $event_output);
            }
        }
    }
    return;
}

sub BuildTempFile
{
    my($self,$dataset,$reader) = @_;
    my $tmpname = $self->tmp."/".$reader->name."_".$dataset->name."_".$self->fid.".unl";
    my $cfg = $self->cfg->bde_copy_configuration('');
    # Ensure file_separator and line_terminator configurations
    # are those expected by the COPY command as issued by the
    # BdeDatabase::streamDataToTempTable function
    # See https://github.com/linz/linz_bde_uploader/issues/90
    $cfg .= "field_separator |\n";
    $cfg .= "line_terminator \\x0A\n";
    my $log = $tmpname.".log";
    my $result = $reader->copy
        (
        $tmpname,
        log_file => $log,
        config => $cfg
        );
    if ($result->{nerrors} > 0)
    {
        die (@{$result->{errors}});
    }
    foreach my $msg (@{$result->{warnings}})
    {
        WARN($msg);
    }
    my $nrec;
    my $nerrors;
    unlink($log) if ! $self->{keepfiles};
    die ("Data file not built") if ! -r $tmpname;
    chmod 0755, $tmpname;
    INFO($result->{nrec}," records copied from ",$reader->path,
            " with ",$result->{nerrors}," errors");

    return $tmpname;
}

sub _OpenDataPipe
{
    my($self,$dataset,$reader) = @_;
    my $cfg = $self->cfg->bde_copy_configuration('');
    # Ensure file_separator and line_terminator configurations
    # are those expected by the COPY command as issued by the
    # BdeDatabase::streamDataToTempTable function
    # See https://github.com/linz/linz_bde_uploader/issues/90
    $cfg .= "field_separator |\n";
    $cfg .= "line_terminator \\x0A\n";
    my $log = $self->tmp."/".$reader->name."_".$dataset->name."_".$self->fid.".unl.log";
    my $fh = $reader->pipe
        (
        log_file => $log,
        config => $cfg
        );

    return ($fh,$log);
}

sub _OpenDataFile
{
    my($self,$dataset,$reader) = @_;
    my $ret;
    if ( $reader->can('pipe') )
    {
        # pipe method was added in linz-bde-perl 1.1.0
        my ($fh, $logfile) = $self->_OpenDataPipe($dataset,$reader);
        # TODO: save logfile somewhere ?
        #       it'll keep being written to while
        #       streaming !
        $ret = $fh;
    }
    else
    {
        my $tmpfile = $self->BuildTempFile($dataset,$reader);
        open(my $tabledatafh, "<$tmpfile") || die ("Cannot open $tmpfile: $!");
        unlink $tmpfile if $tmpfile && ! $self->{keepfiles};
        $ret = $tabledatafh;
    }
    $ret->binmode(":encoding(UTF-8)");
    return $ret;
}

1;

__END__

=head1 NAME

LINZ::BdeUpload - A module to manage a BDE upload job.

=head1 Synopsis

Module to manage a BDE upload job.  Manages the configuration of tables
to upload, the target database, and the repository from which files are
uploaded.

=head2 Public Functions

=over

=item $upload = new LINZ::BdeUpload($cfg);

=item $upload->SetTimeout

=item $upload->RemoveZombiedJobs

=item $upload->PurgeOldJobs

=item $upload->GetMessages

=item $upload->ApplyUpdates

=back

=head2 Configuration items

=over

=item apply_level0

=item apply_level0_inc

=item apply_level5

=item bde_repository

=item bde_connection_string

=item bde_schema

=item bde_tables_config

=item exclude_tables

=item job_record_expiry_days

=item level5_starttime_fail_tolerance

=item level5_starttime_warn_tolerance

=item lock_expiry_hours

=item max_file_errors

=item max_level0_runtime_hours

=item max_level5_runtime_hours

=item override_locks

=item require_all_dataset_files

=item rebuild

=item subset

=item tmp_base_dir

=item upload_tables

=item enable_hooks

=back

=head2 Upload process

=head2 Post upload functions

The post upload functions are functions in the target schema that are
run when after files have been affected by level 0 or level 5 updates.

Functions with a signature

   INT bde_PostUpload_xxxxx( INT upload_id )

are run (in alphabetical order) at the completion of every upload.

Functions with a signature

   INT bde_PostLevel0_xxxxx( upload_id INT )

are run at the completion of every level 0 upload.

Two useful functions that these may use are:

=over

=item INT bde_control.bde_TablesAffected( upload_id INT, tables name[], test TEXT )

Tests tables that are affected by an upload. I<tables> is a list of
table names to check. Names must NOT be qualified with schema name as
the schema name is taken from the target schema of the given upload.

<test> is a string specifying the test to apply and can include the following
space separated items:

=over

=item 'any'

The test will return true if any of the tables meet the criteria.  The default
is to return true if all the tables pass.

=item 'all'

Selects the default criteria, to require all tables to pass

=item 'loaded'

The test will true if the table has been loaded, even if this doesn't change
any data.  The default is only to return true if a table has been changed by
the upload.

=item 'level0'

The test will apply only to level 0 files loaded in the upload.  It will return
false if there are no level 0 files have been uploaded.

=item 'level0_dataset'

The test will apply only to level 0 files in the upload, or level 0 files
in other uploads from the same level 0 dataset.

=back

Using this function may require multiple calls, for example to see if
all tables are loaded and any are affected.

=back

=head2 Internal functions

=over

=item $upload->cfg

=item $upload->db

=item $upload->tables

=item $upload->repository

=item $upload->fid

=item $upload->tmp

=item $upload->CheckTimeout

=item $upload->GetLevel0Updates

=item $upload->GetLevel5Updates

=item $upload->ApplyLevel0Updates

=item $upload->ApplyPostLevel0Functions

=item $upload->ApplyLevel5Updates

=item $upload->ApplyPostUploadFunctions

=item $upload->CreateLevel5ChangeTable

=item $upload->UploadTable

=item $upload->LoadFile

=item $upload->CheckStartDate

=item $upload->BuildTempFile

=item #upload->FireEvent

=back

=head1 Classes used by LINZ::BdeUpload

=head2 BdeUploadTableDef

Holds a definition of a file for uploading.

=over

=item $def = new BdeUploadTableDef($tablename);

Creates a new table definition

=item $def->levels('0','5')

Defines the levels which will apply for this table, 0, 5, or both.

=item $def->add_files($file,...)

Adds a list of files to the files to be uploaded.

=item $def->add_columns($column,...)

Adds one or more column definitions to the definition.  If columns are
specified they override those in the BDE header (use with care!)

=item $def->set_level5_is_full

Specifies that level 5 files actually contain a complete table dump -
equivalent to a level 0 unload.

=item $def->is_available_in_dataset($dataset)

Detemines whether a BDE dataset contains the files necessary to update
the table.  Returns a status 1 or 0, and an array reference for a list of
missing files.

=item $def->is_l0table
=item $def->is_l5table
=item $def->is_l5change

Tests whether the table applies to level 0 updates, level 5 updates, and
level 5 changes tables (for deletions in incremental updates)

=back

Each field can be retrieved with an accessor function, eg

  $def->name
  $def->files
  $def->columns
  $def->level5_is_full

=head2 BdeUploadDatasetDef

Loads and manages a set of BdeUploadTableDef definitions

=over

=item $datasetdef = new BdeUploadDatasetDef($config_file)

Loads configuration file containing definitions of data sets.  Will die
if the file cannot be loaded or contains errors.

=item $datasetdef->tables

Returns a list of the BdeUploadTableDef items in the definition

=item $subset = $datasetdef->subset($table1, $table2, ... )

Returns a new dataset definition containing only the specified tables
(specified by name).

=item $subset = $datasetdef->excluding($table1, $table2, ... )

Returns a new dataset definition excluding the specified tables
(specified by name).

=item $subset = $dataset->level0_subset

=item $subset = $dataset->level5_subset

Return subsets consisting of only tables used applicable to the specified
level

=item @tables = $dataset->level0_tables

=item @tables = $dataset->level5_tables

Returns an array or array reference of the tables applicable to the
specified level

=item $subset = $dataset->level5_change_table

Returns the table definition used for the level 5 changes table.

=back

=head2 BdeUploadSet

Manages a list of datasets/tables to be uploaded.

=over

=item $set = new BdeUploadSet

Create a new empty upload set

=item $set->add(dataset,table)

Adds a BdeDataset and BdeUploadTableDef to the dataset

=item $set->datasets

Returns an array or array reference containing a list of the datasets in the set.

=item $set->tables($dataset)

Returns an array or array reference containing the tables to be loaded for a given dataset.

=item $set->last_dataset_for($table)

Returns the last dataset selected to update the table.

=item $set->empty

Returns true if there is no data defined in the dataset

=item $set->print

Prints a list of datasets and files to be uploaded

=back

