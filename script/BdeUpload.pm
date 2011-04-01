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
use strict;
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
    die "Error reading BDE upload dataset configuration from ",
        $self->{config_file},"\n",@message;
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

    open(my $in, "<$config_file") || $self->_report_config_error("Cannot open file\n"); 
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
            $self->_config_error($errors,$in,"No files defined for table $name\n")
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

package BdeUpload;

use BdeDatabase;
use LINZ::Bde;

use IO::Handle;
use File::Path;
use File::Spec;
use Date::Calc;

use fields qw{ cfg db repository tables tmp_base tmp fid 
               timeout timeout_message dbl0updated dbupdated error_message 
               keepfiles changetable jobfinished messages};

our $tmp_prefix='tmp_bde_upload_';

##################################################################
# This lot of code is to handle what appears to be a win32 perl bug in
# propogating through die/eval, where in at least one case the 
# $@ variable is cleared between the die and the corresponding $@

sub set_error
{
    my $self = shift(@_);
    my $msg = join('',@_);
    return if ! $msg;
    $self->{error_message} = $msg if ! $self->error_message;
    return $msg;
}

sub die_error
{
    my($self,@message) = @_;
    my $msg = $self->set_error(@message);
    return if ! $msg;
    die $msg,"\n";
}

sub error_message
{
    return $_[0]->{error_message};
}
sub clear_error
{
    my($self) = @_;
    my $error = $self->error_message;
    $self->{error_message} = '';
    return $error;
}

sub send_error
{
    my($self,@prefix) = @_;
    my $error = $self->clear_error;
    $self->error(@prefix,$error) if $error;
}

sub propogate_error
{
    my($self) = @_;
    die "Propogating ...\n" if $self->error_message;
}

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
            $self->warning("No definition is available for requested table $t")
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
    
    # Set up the repository and the database

    $self->{db} = new BdeDatabase( $cfg );
    $self->{db}->setApplication($cfg->application_name);

    $self->{repository} = new LINZ::BdeRepository( $cfg->bde_repository );

    # Check for the base scratch directory - create it if it doesn't exist
    
    my $scratch = $cfg->tmp_base_dir;
    if( ! -d $scratch )
    {
        mkpath($scratch);
        $self->die_error("Cannot create temporary working folder $scratch") if ! -d $scratch;
    }
    $self->{tmp_base} = File::Spec->rel2abs($scratch);

    # Id for working files to ensure unique filenames.

    $self->{fid} = 0;

    $self->{dbupdated} = 0;
    $self->{dbl0updated} = 0;
    $self->{timeout} = 0;
    $self->{jobfinished} = 0;
    $self->{messages} = [];
    $self->{keepfiles} = $cfg->keep_files('') ? 1 : 0;

    return $self;
}

sub DESTROY
{
    my ($self) = @_;
    $self->ApplyPostUploadFunctions;

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
    $self->die_error("Cannot create working directory $tmp") if ! -d $tmp;

    $self->{tmp} = $tmp;
    return $tmp;
}
sub writeLog
{
    my($self,$type,@messages) = @_;
    my $db = $self->db;
    return 0 if ! $db || ! $db->jobCreated;
    $db->writeLog($type,@messages);
    return 1;
}

sub writeMessages
{
    my($self,@messages) = @_;
    print @messages;
    print "\n" if $messages[-1] !~ /\n$/;
}

sub writeDatasetLogMessages
{
    my($self,$messages) = @_;
    print "\nDataset upload messages before rollback\n";
    foreach my $msg (@$messages)
    {
        my $text;
        $text .= $msg->{message_time}."\t".$msg->{type}."\t";
        $text .= $msg->{message}."\n";
        print $text;
    }
}

sub warning 
{
    my($self,@messages) = @_;
    my $logged = $self->writeLog('W',@messages);
    $self->writeMessages("Warning: ",@messages)
        if $self->cfg->verbose || ! $logged;
}

sub error
{
    my($self,@messages) = @_;
    my $logged = $self->writeLog('E',@messages);
    $self->writeMessages("Error: ",@messages)
        if $self->cfg->verbose || ! $logged;
}

sub info 
{
    my($self,$level,@messages) = @_;
    my $logged = $self->writeLog($level,@messages);
    $self->writeMessages(@messages)
        if $self->cfg->verbose || ! $logged;
}

sub PurgeOldJobs
{
    my($self) = @_;

    my $db = $self->db;

    my $expiry_time = $self->cfg->lock_expiry_hours;
    $db->releaseExpiredLocks($expiry_time) if $expiry_time;

    my $job_expiry_time = $self->cfg->job_record_expiry_days;
    $db->removeOldJobData($job_expiry_time);

    # Clean up any old scratch directories

    my $scratch = $self->{tmp_base};
    my $prefix = "$scratch/$tmp_prefix";
    foreach my $dir (glob($prefix."*"))
    {
        next if ! -d $dir;
        my $job = substr($dir,length($prefix));
        next if $job !~ /^\d+$/;
        $job += 0;
        next if $db->uploadIsActive($job);
        rmtree($dir) if ! $self->{keepfiles};
    }

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
        $self->die_error($message);
    }
}

sub GetMessages
{
    my($self) = @_;
    my $messages;
    if ($self->{jobfinished})
    {
        $messages = $self->{messages};
    }
    else
    {
        $messages = $self->db->getLogMessages;
    }
    return wantarray ? @$messages : $messages;
}

sub ApplyUpdates
{
    my($self,$dry_run) = @_;

    eval
    {

        my $l0updates;

        if( $self->cfg->apply_level0(0) )
        {
            $l0updates = $self->GetLevel0Updates; 
            if( $dry_run )
            {
                print "Level 0 updates\n";
                $l0updates->print
            }
            elsif(! $l0updates->empty )
            {
                my $timeout = $self->cfg->max_level0_runtime_hours;
                $self->SetTimeout($timeout,"Level 0 updates have timed out\n");
                $self->ApplyLevel0Updates($l0updates);
            }
        }
    
        # Apply level 5 updates
    
        if( $self->cfg->apply_level5(0) )
        {
            # If this is not a dry run, then base level 5 updates
            # on actual state - don't assume that level 0 updates
            # all successfully applied.
            $l0updates = undef if ! $dry_run;
            my $l5updates = $self->GetLevel5Updates($l0updates);

            if( $dry_run )
            {
                print "Level 5 updates\n";
                $l5updates->print
            }
            elsif(! $l5updates->empty )
            {
                my $timeout = $self->cfg->max_level5_runtime_hours;
                $self->SetTimeout($timeout,"Level 5 updates have timed out\n");
                $self->ApplyLevel5Updates($l5updates);
            }
        }

    };
    $self->set_error($@);
    $self->send_error();

    eval
    {
        $self->ApplyPostUploadFunctions;
        $self->FinishJob;
    };
    $self->set_error($@);
    $self->send_error();

}

sub GetLevel0Updates
{
    my($self) = @_;

    my $db = $self->db;

    my $enddate = $self->cfg->end_date('');

    my @datasets = $self->repository->level0->datasets;
    if( $enddate ) { @datasets = grep {$_->name lt $enddate} @datasets; }

    $self->die_error("No level 0 uploads available") if ! @datasets;

    my $dataset = $datasets[-1];

    my $uploadset = new BdeUploadSet();

    my $rebuild = $self->cfg->rebuild(0);
    
    if ( $self->cfg->require_all_dataset_files )
    {
        my $l0_tableset = $self->tables->level0_subset;
        my ($avail, $missing) = $l0_tableset->is_available_in_dataset($dataset);
        if( !$avail )
        {
            $self->die_error("Last available Level 0 dataset is not complete. ",
                "The following files are missing: ",
                join(", ",@$missing), " in dataset ", $dataset->name);
        }
    }

    foreach my $t ( $self->tables->level0_tables )
    {
        my $lastl0 = $db->lastUploadStats($t->name)->{last_level0_dataset};
        $uploadset->add($dataset,$t) if $rebuild || $lastl0 lt $dataset->name;
    }
    return $uploadset;
}

sub GetLevel5Updates
{
    my($self,$l0updates) = @_;

    my $db = $self->db;

    my $enddate = $self->cfg->end_date('');

    my $l5repository = $self->repository->level5;

    my $uploadset = new  BdeUploadSet();
    
    my %complete_datasets;
    my $l5_tableset = $self->tables->level5_subset;
    my $require_all = $self->cfg->require_all_dataset_files;
    foreach my $t ($l5_tableset->tables )
    {
        my $lastl5;
        if ($l0updates)
        {
            my $ds = $l0updates->last_dataset_for($t);
            $lastl5 = $ds->name if $ds;
        }
        $lastl5  = $db->lastUploadStats($t->name)->{last_upload_dataset}
            if ! $lastl5;

        if( $lastl5 eq '' )
        {
            $self->error("Cannot load incremental updates to ".$t->name.
                " as there is no previous upload\n");
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
                $self->warning(
                    "Level 5 dataset " , $d->name, " is not complete",
                    $require_all ? " and will not be loaded" : "",
                    ". The following files are missing: ",
                    join(", ",@$missing)
                ) if ! $avail;
            }
            last if $require_all && ! $complete_datasets{$d};
            $uploadset->add($d,$t);
        }
    }

    return $uploadset;
}

sub ApplyLevel0Updates
{
    my($self,$uploadset) = @_;

    my $dataset = $uploadset->datasets->[0];
    return if ! $dataset;

    $self->CheckTimeout;

    $self->info("I","Applying ",$dataset->name," level 0 update (job ",
        $self->db->uploadId,")\n");

    $self->db->beginDataset($dataset->name);

    my %loaded;
    foreach my $table ( $uploadset->tables($dataset) )
    {
        $loaded{$table->name} = 0;
    }
    
    my $count = 0;
    my $error = 0;
    eval
    {
        foreach my $table ($uploadset->tables($dataset))
        {
            $self->CheckTimeout;

            my $tablename = $table->name;
            eval
            {
                $self->UploadTable($dataset,$table);
                $loaded{$tablename} = 1;
                $count++;
            };
            $self->set_error($@);
            if( $self->error_message )
            {
                my $error = 1;
                $self->send_error("Failed to load level 0 update for ",$tablename,
                    " from ",$dataset->name,"\n");
                last if $self->db->datasetInTransaction;
            }
        }
    };
    $self->set_error($@);
    $self->send_error();

    # Record any unreported table errors.
    foreach my $tablename (keys %loaded)
    {
        next if $loaded{$tablename};
        $error = 1;
        $self->error("Failed to load level 0 update for ",$tablename,
            " from ", $dataset->name);
    }
    
    if ( $error && $self->db->datasetInTransaction )
    {
        $self->writeDatasetLogMessages($self->db->getDatasetMessages);
        $self->db->rollBackDataset;
        $self->error("Failed to load level 0 update for " . $dataset->name .
            ". The transaction has been rolled back");
        return;
    }

    $self->db->endDataset($dataset->name);
    
    if( $self->cfg->skip_postupload_tasks )
    {
        $self->warning("Post level0 upload tasks have not been run by user choice\n");
    }
    else
    {
        $self->ApplyPostLevel0Functions;
    }
}

sub ApplyPostLevel0Functions
{
    my($self) = @_;
    $self->db->applyPostLevel0Functions if $self->{dbl0updated};
    $self->{dbl0updated} = 1;
}

sub ApplyLevel5Updates
{
    my($self,$uploadset) = @_;

    my $db = $self->db;
    my $changetable = $self->{changetable};

    # Record status of each table
    # 0 = Ok to load
    # 1 = Load has failed
    # 2 = Load has failed and subsequent missing loads reported.

    my $tablestate = {};

    my $count = 0;
    foreach my $dataset ($uploadset->datasets)
    {
        $self->CheckTimeout;

        $self->info("I","Applying ",$dataset->name," incremental update (job ",
            $self->db->uploadId,")\n");

        $self->db->beginDataset($dataset->name);
        
        my $error = 0;
        my $change_table_name;
        
        eval
        {
            my @loadtables = ();
            my $need_change_table = 0;

            foreach my $table ($uploadset->tables($dataset))
            {
                my $tablename = $table->name;
                if(! $tablestate->{$tablename})
                {
                    push(@loadtables,$table); 
                    $need_change_table = 1 if ! $table->level5_is_full;
                }
                # tablestate accumulates failed uploads.  This will
                # be cleared if the table is successfully uploaded.
                $tablestate->{$tablename} .= "|".$dataset->name;
            }
        
            $change_table_name = $self->CreateLevel5ChangeTable($dataset,$changetable)
                if $need_change_table;

            foreach my $table ( @loadtables )
            {
                $self->CheckTimeout;
                my $tablename = $table->name;
                eval
                {
                    $self->UploadTable($dataset,$table);
                    $count++;
                    $tablestate->{$tablename} = '';
                };
                $self->set_error($@);
                if( $self->error_message)
                {
                    $error = 1;
                    $self->send_error("Failed to load incremental update for ",$tablename,
                        " from ",$dataset->name,"\n");
                    $tablestate->{$tablename} = '|';
                    last if $self->db->datasetInTransaction;
                }
            }
        };
        $self->set_error($@);
        $self->send_error;

        if ( $error && $self->db->datasetInTransaction )
        {
            $self->writeDatasetLogMessages($self->db->getDatasetMessages);
            $self->db->rollBackDataset;
            $self->error("Failed to load level 5 update for " . $dataset->name .
                ". The transaction has been rolled back");
            last;
        }
        else
        {
            $db->dropWorkingCopy($change_table_name) if $change_table_name;
            $self->db->endDataset($dataset->name);
        }
    }
    # Record any unreported table errors.
    foreach my $tablename (keys %$tablestate )
    {
        my $state = $tablestate->{$tablename};
        next if $state eq '' || $state eq '|';
        my @dsnames = split(/\|/,substr($state,1));
        my $dsname0 = shift(@dsnames);
        $self->error("Failed to load incremental update for ",$tablename,
            " from ", $dsname0) 
            if $dsname0; 
        $self->error("Incremental updates of $tablename from ",join(", ",@dsnames),
            " where bypassed due to previous error for that table\n")
            if @dsnames;
    }

}

sub ApplyPostUploadFunctions
{
    my($self) = @_;
    return if ! $self->{dbupdated};
    if( $self->cfg->skip_postupload_tasks )
    {
         
        $self->warning("Post upload tasks have not been run by user choice\n");
    }
    else
    {
        $self->db->applyPostUploadFunctions;
    }
}

sub FinishJob
{
    my $self = shift;
    $self->{messages} = $self->db->getLogMessages;
    $self->db->finishJob;
    $self->{jobfinished} = 1;
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
    $self->die_error("Cannot create L5 change table") if ! $tablename;

    my @files = $table ? $table->files : ();

    eval
    {
        foreach my $file (@files)
        {
            $self->CheckTimeout();
            $self->LoadFile($dataset,$tablename,$file);
        }
    
    };
    $self->set_error($@);
    $db->endTable("");
    my $msg = $self->clear_error;
    if( $msg )
    {
        $db->dropWorkingCopy($tablename) if ! $temp;
        $self->die_error("Cannot load change table for dataset ",$dataset->name,"\n",$msg);
    }
    return $tablename;
}


sub UploadTable
{
    my($self,$dataset,$table) = @_;
    
    $self->CheckTimeout();

    $self->info('1',"Loading ",$table->name," from dataset ", $dataset->name );
    my ($available,$missing) = $table->is_available_in_dataset($dataset);
    if( !$available ) 
    {
        $self->die_error("The files ",join(", ",@$missing)," required to update ",$table->name,
            " are not available in dataset ",$dataset->name);
    }


    my $db = $self->db;
    my $tablename = $table->name;
    my $is_level0 = $dataset->level eq '0' || $table->level5_is_full;

    eval
    {
        $db->addTable(
            $tablename,
            $table->key_column,
            $table->row_tol_error,
            $table->row_tol_warning
        );
        
        $db->beginTable($tablename) ||
            $self->die_error("Cannot acquire upload lock for $tablename");

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
            || $self->die_error("Cannot create working copy of table ", $table->name);
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
            my $is_incremental = $self->cfg->apply_level0_inc || $table->level5_is_full;
            $db->applyLevel0Update($tablename,$bdedate,$details, $is_incremental)
                || $self->die_error("Cannot apply level ",$dataset->level, 
                    " update for ",$tablename," in ",$dataset->name);
            $self->{dbl0updated} = 1;
        }
        else
        {
            $db->applyLevel5Update($tablename,$bdedate,$details)
                || $self->die_error("Cannot apply level ",$dataset->level, 
                    " update for ",$tablename," in ",$dataset->name);
        }
        $self->{dbupdated} = 1;
    
    };
    $self->set_error($@);

    # Ensure resources are released
    eval { $db->dropWorkingCopy($tablename) };
    eval { $db->endTable($tablename) };

    $self->propogate_error;
}


sub LoadFile
{
    my ($self,$dataset,$tablename,$file,$checktime) = @_;

    my $db = $self->db;

    my $tmpfile = '';
    my $result = '';
    my $reader = $dataset->open($file);

    $self->info('2',"Loading file ",$file," from dataset ",$dataset->name);
    eval
    {
        $self->CheckStartDate($dataset,$file,$reader->start_time,$checktime)
            if $checktime;

        # Determine which columns are to be copied
        my $columns = join("|",$reader->fields);

        $columns = $db->selectValidColumns($tablename,$columns);
        $reader->output_fields(split(/\|/,$columns));

        # Create the temporary file
        $tmpfile = $self->BuildTempFile($dataset,$reader);

        # Upload to the database
        $db->uploadDataToTempTable($tablename,$tmpfile,$columns)
            || $self->die_error("Error uploading data from ",$file," in ",$dataset->name," to ",$tablename);
        
        
    };
    $self->set_error($@);

    eval {$reader->close; };
    eval { unlink $tmpfile if $tmpfile && ! $self->{keepfiles}; };

    $self->propogate_error;

    return $reader->end_time;
}


sub CheckStartDate
{
    my ($self,$dataset,$file,$starttime,$checktime) = @_;
    return if $starttime eq $checktime;

    my $warntol = $self->cfg->level5_starttime_warn_tolerance;
    my $failtol = $self->cfg->level5_starttime_fail_tolerance;
    my $re = qw/^\d{4}\-\d\d\-\d\d\s+\d\d\:\d\d\:\d\d$/;
    
    if( $starttime !~ $re || $checktime !~ $re )
    {
        $self->warning("Cannot check start time of $file of ",
            $dataset->name," level 5 update");
        return;
    }
        
    my $start = Date::Calc::Mktime($starttime =~ /(\d+)/g);
    my $end = Date::Calc::Mktime($checktime =~ /(\d+)/g);
    my $diff = abs($start-$end)/3600.0;
    if( $failtol &&  $diff > $failtol )
    {
        $self->die_error("Start time $starttime in $file of dataset ",$dataset->name,
            " differs from previous end time $checktime by more than $failtol hours");
    }
    if( $warntol && $diff > $warntol )
    {
        $self->warning(
            "Start time $starttime in $file of dataset ",$dataset->name,
            " differs from previous end time $checktime by more than $warntol hours");
    }
}

sub BuildTempFile
{
    my($self,$dataset,$reader) = @_;
    my $tmpname = $self->tmp."/".$reader->name."_".$dataset->name."_".$self->fid.".unl";
    my $cfg = $self->cfg->bde_copy_configuration('');
    my $log = $tmpname.".log";
    my $result = $reader->copy
        (
        $tmpname,
        log_file => $log,
        config => $cfg
        );
    foreach my $msg (@{$result->{errors}})
    {
        $self->error($msg);
    }
    foreach my $msg (@{$result->{warnings}})
    {
        $self->warning($msg);
    }
    my $nrec;
    my $nerrors;
    unlink($log) if ! $self->{keepfiles};
    $self->die_error("Data file not built") if ! -r $tmpname;
    $self->info('2',$result->{nrec}," records copied from ",$reader->path,
            " with ",$result->{nerrors}," errors");
    
    return $tmpname;
}

1;

__END__

=head1 NAME

BdeUpload - A module to manage the a BDE upload job.

=head1 Synopsis

Module to manage the a BDE upload job.  Manages the configuration of tables
to upload, the target database, and the repository from which files are 
uploaded.

=head2 Public Functions

=over

=item $upload = new BdeUpload($cfg);

=item $upload->SetTimeout

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

=item verbose

=back

=head2 Upload process

=head2 Post upload functions 

The post upload functions are functions in the target schema that are
run when after files have been affected by level0 or level 5 updates.

Functions with a signature

   INT bde_PostUpload_xxxxx( INT upload_id )

are run (in alphabetical order) at the completion of every upload.  

Functions with a signature 

   INT bde_PostLevel0_xxxxx( upload_id INT )

are run at the completion of every level 0 upload.  

Two useful functions that these may use are:

=over

=item INT bde_control.bde_TablesAffected( upload_id INT, tables name[], test TEXT ) 

Tests tables that are affected by an upload. I<tables> is a list of tables to check.  <test> is a string specifying the test to apply and can include the following space separated items:

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

=item INT bde_control.bde_WriteUploadLog(upload_id INT, type CHAR(1), message TEXT )

Writes an entry into the log for the upload.  The type should be one of 'E'=error, 'W'=warning, 'I'=information, '1', '2', and '3' for more verbose information.


=back

=head2 Internal functions

=over

=item $upload->cfg

=item $upload->db

=item $upload->tables

=item $upload->repository

=item $upload->fid

=item $upload->tmp 

=item $upload->writeLog

=item $upload->writeMessages

=item $upload->warning 

=item $upload->error

=item $upload->info 

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

=back

=head1 Classes used by BdeUpload

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

