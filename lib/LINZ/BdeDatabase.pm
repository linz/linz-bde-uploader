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

=head1 LINZ::BdeDatabase

Interface between the BdeUpload process and a database

=head1 Version

Version: @@VERSION@@ @@REVISION@@

=over

=item $db = new LINZ::BdeDatabase($cfg)

Creates a new BdeDatabase object, which provides a database connection
and a set of functions for accessing the BDE functions in the database.
Parameters are the connection string for the database, and the name of
the schema holding the BDE tables to be updated.

The following configuration functions are used:

=over

=item  $cfg->db_connection

The connection string (minus dbi:Pg:)

=item  $cfg->db_user

The database user

=item  $cfg->db_pwd

The database password

=item  $cfg->db_connect_sql

A set of ";" separated SQL commands that are run once the connection is
established.

=item  $cfg->db_upload_complete_sql

A set of ";" separated SQL commands that are after a successful upload
(one that has been applied to at least one table). Each SQL command
can be preceded by a conditional statement of the form

   "if" [any|all] [level0|level0_dataset] table ... table [loaded|affected] "?"

=item   $cfg->dataset_load_start_sql

SQL to be run each time a dataset load is started.

=item   $cfg->dataset_load_end_sql

SQL to be run each time a dataset load is completed.

=item   $cfg->override_locks

Override any existing locks on files when doing the update. This will also
override constraints on allowing concurrent uploads.

=item   $cfg->use_table_transaction

Enclose each table update in a transaction

=item   $cfg->use_dataset_transaction

Enclose each dataset update in a transaction. This overrides the table
transaction option.

=item   $cfg->table_exclusive_lock_timeout

Timeout for acquiring exclusive locks on tables (seconds).  Use -1 to wait
indefinitely.

=item   $cfg->allow_concurrent_uploads

Allow simultaneous jobs to load.  The linz_bde_uploader job should generally be
run with -purge if this is not allowed, as otherwise an expired job will
prevent the upload running

=back

=item $result = $db->dbfunc(@params)

This is defined automagically for the following set of functions. Each
of these is mapped to a function "bde_..." in the database.  If the first
parameter of the database function is called upload or bde_schema, then
it is not explicitly included in the perl function - instead it is supplied
by the BdeDatabase object. An upload job is created automatically when
its id is first required to execute a function.

    addTable
    anyUploadIsActive
    applyLevel0Update
    applyLevel5Update
    applyPostLevel0Functions
    applyPostLevel5Functions
    bdeSchema
    bdeTableExists
    beginUploadTable
    checkSchemaName
    createL5ChangeTable
    createUpload
    createWorkingCopy
    getWorkingCopyOid
    dropWorkingCopy
    endUploadTable
    finishUpload
    getOption
    uploadIsActive
    lastUploadStats
    releaseExpiredLocks
    removeOldJobData
    selectValidColumns
    setOption
    startDataset
    tempTableExists
    tmpSchema


The functions return either a scalar, or if the function returns a
row, then a hash reference to the row.

The C<lastUploadStats> returns a hash reference with the fields

       'last_upload_dataset' => '20100406120916',
       'last_upload_bdetime' => '2010-04-19 12:50:50.025',
       'last_uploadId' => '100',
       'last_level0_dataset' => '20100405120915',
       'last_upload_details' => 'Upload details for sco level 5',
       'last_upload_time' => '2010-04-19 12:50:50.025',
       'last_upload_type' => '5'

=item $success = $db->setApplication($app_name)

Sets the application name for the SQL session. Returns true if this was
successful

=item $id = $db->uploadId

Returns the id for the upload. If a job is not currently active this method will
create a new upload id.

=item $status = $db->jobCreated

Will return true if a job upload for this database has been created

=item $db->finishJob

If a job has been created, then the finish SQL is run and the database upload
job is cleaned up.

=item $db->maintain

Will run garbage collection and analyse on the BDE database.

=item $success = $db->beginTable($table_name)

Starts a load for a table. If the table transaction option is set the cfg then a
transaction is started. Returns true if successful.

=item $db->endTable($table_name)

Ends a load for a table. If the table transaction option is set the cfg then a
transaction is committed. Returns true if successful.

=item $success = $db->beginDataset($dataset_name)

Starts a load for a dataset. If the dataset transaction option is set the cfg
then a long transaction is started. Also if dataset_load_start_sql is set in the
cfg these SQL commands are executed. Returns true if successful.

=item $db->endDataset($dataset_name)

Ends a load for a dataset. If the dataset transaction option is set the cfg
then a transaction is committed. Also if dataset_load_end_sql is set in the cfg
these SQL commands are executed.

=item $db->datasetInTransaction()

Returns true if the database is current in a long database transation.

=item $message = $db->rollBackDataset()

If the database is in a dataset long transaction then the transaction will be
rolled back.

=back

=cut

package LINZ::BdeDatabase;

use Log::Log4perl qw(:easy :levels get_logger);
use fields qw{
    _connection _user _pwd _dbh _pg_server_version _startSql
    _finishSql _startDatasetSql _endDatasetSql _errorLevel _dbschema _lastUploadId
    _overrideLocks _usetbltransaction _usedstransaction _intransaction
    _locktimeout _allowConcurrent schema uploadId stack
};

use Try::Tiny;
use DBI;

our @sqlFuncs = qw{
    addTable
    anyUploadIsActive
    applyLevel0Update
    applyLevel5Update
    applyPostLevel0Functions
    applyPostUploadFunctions
    bdeSchema
    bdeTableExists
    beginUploadTable
    checkSchemaName
    createL5ChangeTable
    createUpload
    createWorkingCopy
    getWorkingCopyOid
    dropWorkingCopy
    endUploadTable
    finishUpload
    getOption
    uploadIsActive
    lastUploadStats
    releaseExpiredLocks
    removeOldJobData
    selectValidColumns
    setOption
    startDataset
    tablesAffected
    tempTableExists
    tmpSchema
    };

our $funcsLoaded = 0;

my %pg_log_message_map = (
    DEBUG   => 'debug',
    DEBUG1  => 'debug',
    DEBUG2  => 'debug',
    DEBUG3  => 'debug',
    DEBUG4  => 'debug',
    DEBUG5  => 'debug',
    LOG     => 'debug',
    NOTICE  => 'info',
    INFO    => 'info',
    WARNING => 'warn'
);


my %log_pg_message_map = (
    $OFF   => 'ERROR',
    $FATAL => 'ERROR',
    $ERROR => 'ERROR',
    $WARN  => 'WARNING',
    $INFO  => 'INFO',
    $DEBUG => 'DEBUG',
    $TRACE => 'DEBUG5',
    $ALL   => 'DEBUG5',
);

sub new
{
    my($class,$cfg) = @_;
    my $self = fields::new($class);
    $self->{_connection} = $cfg->db_connection; # no default, less problems
    $self->{_user} = $cfg->db_user('');
    $self->{_pwd} = $cfg->db_pwd('');
    $self->{_startSql} = $cfg->db_connect_sql('');
    $self->{_finishSql} = $cfg->db_upload_complete_sql('');
    $self->{_startDatasetSql} = $cfg->dataset_load_start_sql('');
    $self->{_endDatasetSql} = $cfg->dataset_load_end_sql('');
    $self->{_dbschema} = $cfg->db_schema('bde_control');
    $self->{_overrideLocks} = $cfg->override_locks(0) ? 1 : 0;
    $self->{_usetbltransaction} = $cfg->use_table_transaction(0) ? 1 : 0;
    $self->{_usedstransaction} = $cfg->use_dataset_transaction(1) ? 1 : 0;
    $self->{_locktimeout} = $cfg->table_exclusive_lock_timeout(60)+0;
    $self->{_allowConcurrent} = $cfg->allow_concurrent_uploads(0);
    $self->{_errorLevel} = $cfg->db_error_level('1') + 0;

    $self->{schema} = $cfg->bde_schema('bde');

    if( $self->{_usedstransaction} && $self->{_usetbltransaction} )
    {
        $self->{_usetbltransaction} = 1;
    }

    $self->{uploadId} = undef;
    $self->{stack} = {};
    $self->{_dbh} = undef;
    $self->{_intransaction} = 0;

    my $dbh = DBI->connect("dbi:Pg:".$self->{_connection},
        $self->{_user}, $self->{_pwd},
        {
            AutoCommit    =>1,
            PrintError    =>0,
            PrintWarn     =>1,
            RaiseError    =>1,
            pg_errorlevel => $self->{_errorLevel},
        }
    )
       || die "Cannot connect to database\n",DBI->errstr;

    my $pg_server_version = $dbh->{'pg_server_version'};
    if ( $pg_server_version =~ /\d/ )
    {
        $self->{_pg_server_version} = $pg_server_version;
    }
    else
    {
        WARN "WARNING: no pg_server_version!  Assuming >= 8.4";
        $self->{_pg_server_version} = 80400;
    }


    # Immediately switch to the role meant to run uploads (`bde_admin`)
    $dbh->do("SET SESSION AUTHORIZATION bde_admin");

    if ( $self->{_pg_server_version} >= 90000 )
    {
        my $row = $dbh->selectcol_arrayref("SELECT pg_is_in_recovery()");
        if ($$row[0])
        {
            die "PostgreSQL is still in recovery after a database crash or ".
                "you are connected to a read-only slave";
        }
    }

    $dbh->do("set search_path to ".$self->{_dbschema}.", public");
    my $schema2 = $dbh->selectrow_array("SELECT bde_CheckSchemaName(?)",{},
        $self->{_dbschema});
    die "Invalid schema ",$self->{_dbschema}," specified for upload\n"
        if ! $schema2;

    $self->{_dbschema} = $schema2;
    $self->{_dbh} = $dbh;
    $self->_setupFunctions() if ! $funcsLoaded;

    my $logger = get_logger();
    my $pg_msg_level = $log_pg_message_map{$logger->level};
    $dbh->do("SET client_min_messages = $pg_msg_level") if $pg_msg_level;

    $self->_runSQLBlock($self->{_startSql});

    return $self;
}

sub DESTROY
{
    my($self) = @_;
    if( $self->_dbh )
    {
        $self->_commitTransaction;
        $self->_dbh->disconnect;
    }
}

sub uploadId
{
    my($self) = @_;
    if(! $self->jobCreated )
    {
        if( ! $self->{_allowConcurrent} && ! $self->{_overrideLocks} && $self->anyUploadIsActive )
        {
            die "Cannot create upload job - another job is already active (or zombied)\n";
        }
        $self->{uploadId} = $self->createUpload;
        INFO('Job ' . $self->{uploadId} . ' created');
        $self->setOption('exclusive_lock_timeout',$self->{_locktimeout});
    }
    $self->{_lastUploadId} = $self->{uploadId};
    return $self->{uploadId};
}

sub jobCreated
{
    my($self) = @_;
    return defined($self->{uploadId});
}

sub maintain
{
    my($self) = @_;
    $self->_dbh_do("VACUUM ANALYSE") ||
        ERROR "Cannot vacuum database\n", $self->_dbh->errstr,"\n";
}

sub finishJob
{
    my $self = shift;
    my $error = shift;
    return if ! $self->jobCreated;
    if ($error)
    {
        $self->_rollbackTransaction;
    }
    else
    {
        try
        {
            $self->_runFinishSql;
        }
        catch
        {
            ERROR("Could not run finish SQL: $_");
        };
    }

    try
    {
        $self->finishUpload($error ? 1: 0);
    }
    catch
    {
        ERROR("Could not finish the job upload: $_");
    };

    my $msg = 'Job ' . $self->{uploadId} . ' finished ' .
        ($error ? 'with errors' : 'successfully');
    INFO($msg);
    $self->{uploadId} = undef;
}

sub setApplication
{
    my($self,$app_name) = @_;
    my $result = 0;
    if ( $self->{_pg_server_version} >= 90000 )
    {
        my $rv = $self->_dbh_do("SET application_name='$app_name'");
        $result = 1 if (defined $rv);
    }
    return $result;
}

sub beginTable
{
    my($self,$table) = @_;

    # Empty table name used for uploading audit table
    my $result = $table eq "" ||
                 $self->beginUploadTable($table,$self->{_overrideLocks});
    if( $result && $self->{_usetbltransaction})
    {
        $self->_beginTransaction;
    }
    return $result;
}

sub endTable
{
    my($self,$table) = @_;
    $self->_commitTransaction if $self->{_usetbltransaction};
    $self->endUploadTable($table) if $table ne "";
}

sub beginDataset
{
    my($self,$name) = @_;
    if( $self->{_usedstransaction})
    {
        $self->_beginTransaction;
    }
    my $result = $self->startDataset($name);
    $self->_runSQLBlock($self->{_startDatasetSql});
    return $result;
}

sub endDataset
{
    my($self,$name) = @_;
    $self->_runSQLBlock($self->{_endDatasetSql});
    $self->_commitTransaction if $self->{_usedstransaction};
}

sub datasetInTransaction
{
    my $self = shift;
    return $self->{_usedstransaction} && $self->{_intransaction};
}

sub rollBackDataset
{
    my($self) = @_;
    my $result;
    if ( $self->datasetInTransaction )
    {
        $self->_rollbackTransaction;
    }
    return $result;
}

sub streamDataToTempTable
{
    my($self,$tablename,$datafh,$columns) = @_;

    $self->_setDbMessageHandler;

    DEBUG('Streaming data into table ' . $tablename);

    my $uploadId = $self->uploadId;
    my $dbh = $self->_dbh;

    my @quotedColumns;
    foreach my $col (split(/\|/, $columns)) {
        push(@quotedColumns, $dbh->quote_identifier( $col ));
    }
    #DEBUG('quoted columns are ' . join(',',@quotedColumns));

    my $tmptable = $self->getWorkingCopyOid($tablename);

    DEBUG('Temp table is ' . $tmptable);
    if ( ! $tmptable ) {
        ERROR 'Cannot get working copy of table ' . $tablename;
        $self->_clearDbMessageHandler;
        return 0;
    }

    my $sql = 'LOCK TABLE ' . $tmptable . ' IN ACCESS EXCLUSIVE MODE';
    $dbh->do( $sql );

    $sql = 'COPY ' . $tmptable . ' (' . join(',', @quotedColumns)
         . ") FROM stdin WITH DELIMITER '|' NULL AS ''";
    #DEBUG 'SQL: ' . $sql;
    $dbh->do($sql);

    # See https://metacpan.org/pod/DBD::Pg#COPY-support
    my @buf;
    while(<$datafh>) {
        # We'll keep 16 lines of data to show upon catching
        # an error
        shift(@buf) if ( @buf > 16 );
        push (@buf, $_);
        $dbh->pg_putcopydata($_);
    }
    # NOTE: will throw, on error
    try {
        $dbh->pg_putcopyend();
    }
    catch {
        die $_ . "\nLast 16 lines of sent COPY data: @buf";
    };

    $self->_clearDbMessageHandler;
    return 1;
}

sub schema { return $_[0]->{schema} }

sub _dbh { return $_[0]->{_dbh} }

sub _runSQLBlock
{
    my ($self, $sql_block) =  @_;
    return if ! $sql_block;
    my $id;
    foreach my $cmd (grep {/\S/} split(/\;\n?/,$sql_block))
    {
        if ($cmd =~ /\{id\}/)
        {
            if (!defined $id)
            {
                $id = $self->uploadId;
            }
            $cmd =~ s/\{id\}/$id/g;
        }
        try
        {
            $self->_dbh_do($cmd);
        }
        catch
        {
            die "Cannot run SQL command: $cmd\n", $self->_dbh->errstr;
        };
    }
}

sub _runFinishSql
{
    my($self) = @_;
    return if ! $self->jobCreated;
    my $id = $self->uploadId;
    my $sql = $self->{_finishSql};
    foreach my $cmd (grep {/\S/} split(/\;/,$sql))
    {
        if( $cmd =~ /^\s*if\s+
                        (
                            (?:any\s+|all\s+|)?
                            (?:level_0(?:_dataset)?\s+)?
                        )
                        (
                            \w+(?:\s+\w+)*?
                        )
                        (
                            \s+(?:loaded|affected)
                        )?
                        \s*\?\s*(.*?)\s*$/ixs)
        {
            my $tables = $2;
            my $test = $1.$3;
            $cmd = $4;
            $test =~ s/^\s+//;
            $test =~ s/\s+$//;
            $test =~ s/\s+/ /;
            next if ! $self->tablesAffected($test,$tables);
        }
        $cmd =~ s/\{id\}/$id/g;
        try
        {
            $self->_dbh_do($cmd);
        }
        catch
        {
            die "Cannot run finishing SQL: $cmd: ", $self->_dbh->errstr;
        };
    }
}

sub _setupFunctions
{
    my($self) = @_;

    return if $funcsLoaded;

    my $dbschema = $self->{_dbschema};
    my $sql = 'SELECT * FROM '.$dbschema.'.bde_GetBdeFunctions(?)';
    my $sth = $self->_dbh->prepare($sql) || die $self->_dbh->errstr;
    $sth->execute($dbschema);

    my %funcName = map {"bde_".lc($_) => $_ } @sqlFuncs;
    my $schema = $self->schema;

    while( my($func,$nparam,$isjobfunc,$isschemafunc,$returntype) = $sth->fetchrow_array())
    {
        my $name = $funcName{lc($func)};
        delete $funcName{lc($func)};
        next if ! $name;
        $name =~ s/^bde_//i;

        my $sqlf = $dbschema.'.'.$func.'('.join(",",("?")x$nparam).')';
        $sqlf = '* FROM '.$sqlf if ($returntype eq 'RECORD' || $returntype eq 'TABLE');
        $sqlf = 'SELECT '.$sqlf;

        my $sub = sub
            {
                my($self) = shift(@_);
                return if $self->{stack}->{$name};
                $self->{stack}->{$name} = 1;
                my $result = $self->_executeFunction($name,$sqlf,\@_,$nparam,$returntype);
                $self->{stack}->{$name} = 0;
                return $result;
            };

        my $sqlsub = $sub;
        $sqlsub = sub { my $self=shift(@_); return $sub->($self,$self->schema,@_) }
            if $isschemafunc;

        $sqlsub = sub { my $self=shift(@_); return $sub->($self,$self->uploadId,@_) }
            if $isjobfunc;

        # Insert the function into the symbol table
        no strict qw/ refs /;
        *{$name} = $sqlsub;
        use strict qw/ refs /;
    }
    if( %funcName )
    {
        die "The following functions are missing in the database\n  ",
            join("\n  ",map {"bde_".$_ } sort values %funcName ),"\n";
    }
    $funcsLoaded = 1;
}

sub _executeFunction
{
    my($self,$name,$sql,$params,$nparam,$returntype) = @_;
    die "Invalid number of parameters (",join(", ",@$params),") in call to $name"
        if scalar(@$params) != $nparam;

    my $result;
    try
    {
        $self->_setDbMessageHandler;
        if( $returntype eq 'RECORD' )
        {
            $result = $self->_dbh->selectrow_hashref($sql,{},@$params);
            $result = {} if ! $result;
        }
        elsif( $returntype eq 'TABLE' )
        {
            my @row_set = ();
            my $sth = $self->_dbh->prepare( $sql );
            $sth->execute( @$params );
            while ( my $row = $sth->fetchrow_hashref() )
            {
                push @row_set, $row;
            }
            $sth->finish;
            $result = \@row_set;
        }
        else
        {
            ($result) = $self->_dbh->selectrow_array($sql,{},@$params);
        }
        $self->_clearDbMessageHandler;
    }
    catch
    {
        my $error = $self->_dbh->errstr;
        die "Database function $name failed: $error";
    };
    return $result;
}

sub _setDbMessageHandler
{
    my $self = shift;
    $SIG{__WARN__} = sub { &_dbMessageHandler($self, @_); };
}

sub _clearDbMessageHandler
{
    $SIG{__WARN__} = undef;
}

sub _dbMessageHandler
{
    my $self = shift;
    my $db_message = shift;
    $db_message =~ s/\r\n/ /g;
    $db_message =~ s/\n/ /g;
    my ($type, $text, $extra) = $db_message
        =~ /^(\w+)\:(?:\s+0{5}\:)?\s+(.*?)\s*((?:CONTEXT|LOCATION)\:(?:.*))?$/;
    if ($type)
    {
        my $logger = get_logger();
        my $msg_func = $pg_log_message_map{$type};
        if ($msg_func)
        {
            $logger->$msg_func($text);
            if ($extra)
            {
                my $level = $msg_func eq 'warn' ? $msg_func : 'debug';
                $logger->$level($extra);
            }
        }
        else
        {
            die $db_message;
        }
    }
    return;
}

sub _beginTransaction
{
    my($self) = @_;
    $self->_commitTransaction;
    $self->_dbh->begin_work;
    $self->{_intransaction} = 1;
}

sub _commitTransaction
{
    my($self) = @_;

    if( $self->{_intransaction} )
    {
        $self->_dbh->commit;
        $self->{_intransaction} = 0;
    }
}

sub _rollbackTransaction
{
    my $self = shift;
    my $result;
    try
    {
        $result = $self->_dbh->rollback;
    };
    $self->{_intransaction} = 0;
    return $result;
}

sub _dbh_do
{
    my ($self, $sql) = @_;
    $self->_setDbMessageHandler;
    DEBUG("Running: $sql");
    my $rv = $self->_dbh->do($sql);
    $self->_clearDbMessageHandler;
    return $rv;
}

1;
