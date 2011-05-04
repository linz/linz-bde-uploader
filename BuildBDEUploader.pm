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
package BuildBDEUploader;

use base qw(Module::Build);

use File::Spec;

my $PACKAGE_DIR = 'linz-bde-uploader';

sub new
{
	my $class = shift;
	my $self = $class->SUPER::new(@_);
	$self->_set_extra_install_paths();
    return $self;
}

sub resume
{
	my $class = shift;
	my $self = $class->SUPER::resume(@_);
	$self->_set_extra_install_paths();
    return $self;
}

sub find_conf_files
{
    shift->_find_files('conf', 'conf');
}

sub find_sql_files
{
    shift->_find_files('sql', 'sql');
}

sub _set_extra_install_paths
{
	my $self = shift;
    my $prefix = $self->install_base || $self->prefix || '';
    my $sysconfdir =  $prefix eq '/usr' ? '/etc' : File::Spec->catdir($prefix, 'etc');
    my $datadir = File::Spec->catdir($prefix, 'share');
    
    $self->install_path('conf' => File::Spec->catdir($sysconfdir, $PACKAGE_DIR));
    $self->install_path('sql'  => File::Spec->catdir($datadir, $PACKAGE_DIR, 'sql'));
}

sub _find_files
{
    my ($self, $type, $dir) = @_;
    
    if (my $files = $self->{properties}{"${type}_files"}) {
      return { map $self->localize_file_path($_), %$files };
    }
  
    return {} unless -d $dir;
    return { map {$_, $_}
        map $self->localize_file_path($_),
        grep !/\.\#/,
        @{ $self->rscan_dir($dir, qr{\.$type$}) } };
}

1;
