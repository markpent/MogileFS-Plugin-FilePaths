# FilePaths plugin for MogileFS, by Mark Pentland, xb95 and hachi

#
# This plugin enables full pathing support within MogileFS, for creating files,
# listing files in a directory, deleting files, etc.
#
# Supports most functionality you'd expect.


#this is a heavily modified version of the FilePaths 0.3 plugin
#Symlink support added
#Changed (optimised) database layout to remove n+1 lookups caused by meta data lookup
#Create Directory Support
#Cache Invalidation Notification

#THIS IS NOT BACKWARDS COMPATABILE WITH v0.3 FilePaths!


package MogileFS::Plugin::FilePaths;

use strict;
use warnings;

our $VERSION = '0.04';
$VERSION = eval $VERSION;

use MogileFS::Worker::Query;
use MogileFS::Util qw(error debug);


# called when this plugin is loaded, this sub must return a true value in order for
# MogileFS to consider the plugin to have loaded successfully.  if you return a
# non-true value, you MUST NOT install any handlers or other changes to the system.
# if you install something here, you MUST un-install it in the unload sub.

sub _parse_path {
    my $fullpath = shift;
    return unless defined($fullpath) and length($fullpath);
    
    #im pretty liberal with paths... as long as it starts with a / ... 
    #its up to the client to make sure its a valid path as far as OS concerns go becuase we use a DB to store the paths so anything goes...
    
    return unless index($fullpath, '/') == 0;
    
    my $pathpos = rindex($fullpath, '/');
    return unless $pathpos >= 0;
    
    my $path = substr($fullpath, 0, $pathpos + 1);
    my $file = substr($fullpath, $pathpos + 1);
    
    #error("split $fullpath into $path and $file");
    return ($path, $file);
    
    #my ($path2, $file2) = $fullpath =~
    #    m!^(/(?:[\~\w\s\-\.\|\#\+]+/)*)([\~\w\s\-\.\|\#\+]+)$!;
        
    #error("regex split $fullpath into $path2 and $file2");
    #return ($path2, $file2);
}

#this uses the 'error' channel to report a cache invalidation to any clients !watch ing the server.
sub cache_invalidate {
    my ($fullpath, $client_id) = @_;
    $client_id = "unknown" unless defined($client_id);
   
    if (my $worker = MogileFS::ProcManager->is_child) {
        $worker->send_to_parent("error [cache][$client_id] $fullpath");
    } else {
        my $dbg = "[cache][$client_id] $fullpath";
        MogileFS::ProcManager->NoteError(\$dbg);
    }
}

sub load {

    # we want to remove the key being passed to create_open, as it is going to contain
    # only a path, and we want to ignore that for now
    MogileFS::register_global_hook( 'cmd_create_open', sub {
        my $args = shift;
        return 1 unless _check_dmid($args->{dmid});

        my $fullpath = delete $args->{key};
        my ($path, $filename) = _parse_path($fullpath);
        unless (defined($path) && length($path) && defined($filename) && length($filename)) {
          error("$fullpath is not a valid absolute path");
          die "Filename is not a valid absolute path." 
        }
        
        
        return 1;
    });

    # when people try to create new files, we need to intercept it and rewrite the
    # request a bit in order to do the right footwork to support paths.
    MogileFS::register_global_hook( 'cmd_create_close', sub {
        my $args = shift;
        return 1 unless _check_dmid($args->{dmid});

        # the key is the path, so we need to move that into the logical_path argument
        # and then set the key to be something more reasonable
        $args->{logical_path} = $args->{key};
        $args->{key} = "fid:$args->{fid}";
    });

    # called when we know a file has successfully been uploaded to the system, it's
    # a done deal, we don't have to worry about anything else
    MogileFS::register_global_hook( 'file_stored', sub {
        my $args = shift;
        return 1 unless _check_dmid($args->{dmid});
        
        # we need a path or this plugin is moot
        return 0 unless $args->{logical_path};

        # ensure we got a valid seeming path and filename
        my ($path, $filename) = _parse_path($args->{logical_path});
        return 0 unless defined($path) && length($path) && defined($filename) && length($filename);

        # great, let's vivify that path and get the node to it
        my $parentnodeid = MogileFS::Plugin::FilePaths::vivify_path( $args->{dmid}, $path );
        return 0 unless defined $parentnodeid;

        # see if this file exists already
        my $oldfid = MogileFS::Plugin::FilePaths::get_file_mapping( $args->{dmid}, $parentnodeid, $filename );
        if (defined $oldfid && $oldfid) {
          
            my $sto = Mgd::get_store();
            $sto->delete_fidid($oldfid);
    
            #my $dbh = Mgd::get_dbh();
            #$dbh->do("DELETE FROM file WHERE fid=?", undef, $oldfid);
            #$dbh->do("REPLACE INTO file_to_delete SET fid=?", undef, $oldfid);
        }

        my $fid = $args->{fid};

        # and now, setup the mapping
        my $nodeid = MogileFS::Plugin::FilePaths::set_file_mapping( $args->{dmid}, $parentnodeid, $filename, $fid, $args->{mtime} );
        return 0 unless $nodeid;

        if (my $keys = $args->{"plugin.meta.keys"}) {
            my %metadata;
            for (my $i = 0; $i < $keys; $i++) {
                my $key = $args->{"plugin.meta.key$i"};
                my $value = $args->{"plugin.meta.value$i"};
                $metadata{$key} = $value;
            }

            MogileFS::Plugin::MetaData::set_metadata($fid, \%metadata);
        }

        
        cache_invalidate(resolve_path($args->{logical_path},$nodeid), $args->{client_id});
        
        # we're successful, let's keep the file
        return 1;
    });

    # and now magic conversions that make the rest of the MogileFS commands work
    # without having to understand how the path system works
    MogileFS::register_global_hook( 'cmd_get_paths', \&_path_to_key );
    MogileFS::register_global_hook( 'cmd_delete', sub {
        my $args = shift;
        return 1 unless _check_dmid($args->{dmid});

        # ensure we got a valid seeming path and filename
        my ($path, $filename) = _parse_path($args->{key});
        return 0 unless defined($path) && length($path) && defined($filename) && length($filename);

        # now try to get the end of the path
        my $parentnodeid = MogileFS::Plugin::FilePaths::load_path( $args->{dmid}, $path );
        if(defined $parentnodeid) {
          # get the fid of the file, bail out if it doesn't have one (directory nodes)
          my $fid = MogileFS::Plugin::FilePaths::get_file_mapping( $args->{dmid}, $parentnodeid, $filename );
          if($fid) {
  
            # great, delete this file
            delete_file_mapping( $args->{dmid}, $parentnodeid, $filename );
            # FIXME What should happen if this delete fails?
    
            cache_invalidate($args->{key}, $args->{client_id}); #ideally this would be done after mfs deleted...
            
            # now pretend they asked for it and continue
            $args->{key} = "fid:$fid";
            return 1;
          }
        }
        $args->{key} = "MISSING_PATH";
        return 1;
    });

    MogileFS::register_worker_command( 'filepaths_enable', sub {
        # get parameters
        my MogileFS::Worker::Query $self = shift;
        my $args = shift;

        # verify domain firstly
        my $dmid = $self->check_domain($args)
            or return $self->err_line('domain_not_found');

        my $dbh = Mgd::get_dbh();
        return undef unless $dbh;

        $dbh->do("REPLACE INTO plugin_filepaths_domains (dmid) VALUES (?)", undef, $dmid);

        return $self->err_line('unable_to_enable', "Unable to enable the filepaths plugin: " . $dbh->errstr)
            if $dbh->err;

        return $self->ok_line;
    });

    MogileFS::register_worker_command( 'filepaths_disable', sub {
        # get parameters
        my MogileFS::Worker::Query $self = shift;
        my $args = shift;

        # verify domain firstly
        my $dmid = $self->check_domain($args)
            or return $self->err_line('domain_not_found');

        my $dbh = Mgd::get_dbh();
        return undef unless $dbh;

        $dbh->do("DELETE FROM plugin_filepaths_domains WHERE dmid = ?", undef, $dmid);

        return $self->err_line('unable_to_disable', "Unable to enable the filepaths plugin: " . $dbh->errstr)
            if $dbh->err;

        return $self->ok_line;
    });

    # now let's define the extra plugin commands that we allow people to interact with us
    # just like with a regular MogileFS command
    MogileFS::register_worker_command( 'filepaths_list_directory', sub {
        # get parameters
        my MogileFS::Worker::Query $self = shift;
        my $args = shift;

        # verify domain firstly
        my $dmid = $self->check_domain($args)
            or return $self->err_line('domain_not_found');

        return $self->err_line("plugin_not_active_for_domain")
            unless _check_dmid($dmid);

        # verify arguments - only one expected, make sure it starts with a /
        my $path = $args->{arg1};
        return $self->err_line('bad_params')
            unless $args->{argcount} == 1 && $path && $path =~ /^\//;

        # now find the id of the path
        my $nodeid = MogileFS::Plugin::FilePaths::load_path( $dmid, $path );
        return $self->err_line('path_not_found', 'Path provided was not found in database')
            unless defined $nodeid;

#       TODO This is wrong, but we should throw an error saying 'not a directory'. Requires refactoring
#            a bit of code to make the 'fid' value available from the last node we fetched.
#        if (get_file_mapping($nodeid)) {
#            return $self->err_line('not_a_directory', 'Path provided is not a directory');
#        }

        # get files in path, return as an array
        my %res;
        my $ct = 0;
        my @nodes = MogileFS::Plugin::FilePaths::list_directory( $dmid, $nodeid );
        my $dbh = Mgd::get_dbh();

        my $node_count = $res{'files'} = scalar @nodes;

        for(my $i = 0; $i < $node_count; $i++) {
            my ($nodename, $fid, $type, $lastmodified, $size, $link, $nid) = @{$nodes[$i]};
            my $prefix = "file$i";
            $res{$prefix} = $nodename;
            $res{"$prefix.mtime"} = $lastmodified if defined($lastmodified);
            $res{"$prefix.nid"} = $nid;
            if ($type == 0) {
                #its a file!
                $res{"$prefix.type"} = "F";
                $res{"$prefix.size"} = $size if defined($size);
                
                #my $metadata = MogileFS::Plugin::MetaData::get_metadata($fid);
                #$res{'mtime'} = $metadata->{mtime} if $metadata->{mtime};
            } elsif ($type == 1) {
                $res{"$prefix.type"} = "D";
            } elsif ($type == 2) {
                $res{"$prefix.type"} = "L";
                $res{"$prefix.link"} = $link if defined($link);
            }
        }

        return $self->ok_line( \%res );
    });

    MogileFS::register_worker_command( 'filepaths_rename', sub {
        my MogileFS::Worker::Query $self = shift;
        my $args = shift;

        my $dmid = $self->check_domain($args)
            or return $self->err_line('domain_not_found');

        return $self->err_line("plugin_not_active_for_domain")
            unless _check_dmid($dmid);

        return $self->err_line("bad_argcount")
            unless $args->{argcount} >= 2;

        my ($old_path, $old_name) = _parse_path($args->{arg1});

        return $self->err_line("badly_formed_orig")
            unless defined($old_path) && length($old_path) &&
                   defined($old_name) && length($old_name);

        my ($new_path, $new_name) = _parse_path($args->{arg2});

        return $self->err_line("badly_formed_new")
            unless defined($new_path) && length($new_path) &&
                   defined($new_name) && length($new_name);

        # I'd really like to lock on this operation at this point, but I find the whole idea to be rather
        # sad for the number of locks I would want to hold. Going to think about this instead and hope
        # nobody finds a way to make this race.

        # LOCK rename

        my $old_parentid = load_path($dmid, $old_path);
        my $new_parentid = vivify_path($dmid, $new_path);

        my $dbh = Mgd::get_dbh();
        return undef unless $dbh;
        
        #check if we are renaming a file over the top of en existing file (valid)
        my ($conflict_id, $conflict_type, $conflict_fid) = $dbh->selectrow_array('SELECT nodeid, type, fid FROM plugin_filepaths_paths ' .
                                       'WHERE dmid = ? AND parentnodeid = ? AND nodename = ?',
                                       undef, $dmid, $new_parentid, $new_name);
        
        if($conflict_id) {
          if($conflict_type == 1) { #cannot override directory
            return $self->err_line("duplicate_entry", "The destination (directory) already existed");
          } else {
            if($conflict_type == 0) { #remove the file....
              # get the file handle of the overriden file
              my $del_fid = MogileFS::FID->new_from_dmid_and_key($dmid, "fid:$conflict_fid");
              $del_fid->delete;
            }
            $dbh->do('DELETE FROM plugin_filepaths_paths WHERE nodeid = ?', undef, $conflict_id); 
          }
        }
        

        eval {
          $dbh->do('UPDATE plugin_filepaths_paths SET parentnodeid=?, nodename=? WHERE dmid=? AND parentnodeid=? AND nodename=?', undef,
                 $new_parentid, $new_name, $dmid, $old_parentid, $old_name);
        };
        if($@) {
          my $estr = $@;
          if($estr =~ /Duplicate/) {
            #print STDERR "filepaths_rename: rename failed: duplicate entry\n";
            return $self->err_line("duplicate_entry", "The destination node already existed");
          }
         # print STDERR "filepaths_rename: rename failed $estr\n";
          return $self->err_line("rename_failed", $estr)
        }

        # UNLOCK rename
        
        cache_invalidate($args->{arg1},  $args->{client_id});
        cache_invalidate($args->{arg2},  $args->{client_id});

        if($dbh->err) {
            #print STDERR "filepaths_rename: rename failed\n";
            return $self->err_line("rename_failed") 
        }

        return $self->ok_line();
    });
    
    #set the mtime of a node
    MogileFS::register_worker_command( 'filepaths_set_mtime', sub {
        my MogileFS::Worker::Query $self = shift;
        my $args = shift;

        my $dmid = $self->check_domain($args)
            or return $self->err_line('domain_not_found');

        return $self->err_line("plugin_not_active_for_domain")
            unless _check_dmid($dmid);

        return $self->err_line("bad_argcount")
            unless $args->{argcount} >= 2;

        my ($path, $name) = _parse_path($args->{arg1});

        return $self->err_line("badly_formed_path")
            unless defined($path) && length($path) &&
                   defined($name) && length($name);

        my $mtime = $args->{arg2};

        return $self->err_line("badly_formed_mtime") unless defined($mtime);


        my $parentid = load_path($dmid, $path);
        return $self->err_line("file_not_found") unless defined($parentid);

        my $dbh = Mgd::get_dbh();
        return undef unless $dbh;


        $dbh->do('UPDATE plugin_filepaths_paths SET lastmodified = ? WHERE dmid=? AND parentnodeid=? AND nodename=?', undef,
              $mtime , $dmid, $parentid, $name);

        
        
        cache_invalidate($args->{arg1},  $args->{client_id});

        if($dbh->err) {
            #print STDERR "filepaths_rename: rename failed\n";
            return $self->err_line("set_mtime_failed") 
        }

        return $self->ok_line();
    });
    
    #get info on a file/dir
    MogileFS::register_worker_command( 'filepaths_path_info', sub {
        # get parameters
        my MogileFS::Worker::Query $self = shift;
        my $args = shift;

        # verify domain firstly
        my $dmid = $self->check_domain($args)
            or return $self->err_line('domain_not_found');

        return $self->err_line("plugin_not_active_for_domain")
            unless _check_dmid($dmid);

        # verify arguments - only one expected, make sure it starts with a /
        my $path = $args->{arg1};
        return $self->err_line('bad_params')
            unless $args->{argcount} == 1 && $path && $path =~ /^\//;

        # now find the id of the path
        my $nodeid = MogileFS::Plugin::FilePaths::load_path( $dmid, $path );
        return $self->err_line('path_not_found', 'Path provided was not found in database')
            unless defined $nodeid;

        my %res;
        my $dbh = Mgd::get_dbh();
        
        my $sth = $dbh->prepare('SELECT plugin_filepaths_paths.nodename, plugin_filepaths_paths.type, plugin_filepaths_paths.fid, file.length, plugin_filepaths_paths.lastmodified, plugin_filepaths_paths.link, plugin_filepaths_paths.nodeid  FROM plugin_filepaths_paths left join file on(plugin_filepaths_paths.fid = file.fid) ' .
                            'WHERE plugin_filepaths_paths.nodeid = ?');
        $sth->execute($nodeid);

        my ($nodename, $type, $fid, $length, $lastmodified, $link, $nid) = $sth->fetchrow_array;
        $res{'name'} = $nodename;
        $res{'mtime'} = $lastmodified if defined($lastmodified);
        $res{'nid'} = $nid;
        if ($type == 0) {
            #its a file!
            $res{'type'} = "F";
            $res{'size'} = $length if defined($length);
            #my $metadata = MogileFS::Plugin::MetaData::get_metadata($fid);
            #$res{'mtime'} = $metadata->{mtime} if $metadata->{mtime};
        } elsif ($type == 1) {
            $res{'type'} = "D";
        } elsif ($type == 2) {
            $res{'type'} = "L";
            $res{'link'} = $link if defined($link);
        }
        return $self->ok_line( \%res );
    });
    
    
    #delete a node (directory/link) - NOT A FILE!
    MogileFS::register_worker_command( 'filepaths_delete_node', sub {
        # get parameters
        my MogileFS::Worker::Query $self = shift;
        my $args = shift;

        # verify domain firstly
        my $dmid = $self->check_domain($args)
            or return $self->err_line('domain_not_found');

        return $self->err_line("plugin_not_active_for_domain")
            unless _check_dmid($dmid);

        # verify arguments - only one expected, make sure it starts with a /
        my $path = $args->{arg1};
        return $self->err_line('bad_params')
            unless $args->{argcount} >= 1 && $path && $path =~ /^\//;

        # now find the id of the path
        my $nodeid = MogileFS::Plugin::FilePaths::load_path( $dmid, $path );
        return $self->err_line('path_not_found', 'Path provided was not found in database')
            unless defined $nodeid;

        my $dbh = Mgd::get_dbh();
        
        
        my $type = $dbh->selectrow_array('SELECT type FROM plugin_filepaths_paths ' .
                                    'WHERE nodeid = ?',
                                    undef, $nodeid);
        
        if($type == 1) {
          #check diretcory is not empty... 
          my $cnt = $dbh->selectrow_array('SELECT count(*) FROM plugin_filepaths_paths ' .
                                    'WHERE dmid=? AND parentnodeid = ?',
                                    undef, $dmid, $nodeid);
          return $self->err_line("directory_not_empty") if($cnt > 0);
        }
        
        my $deleted_path = resolve_path($args->{arg1},$nodeid);
        
        $dbh->do('DELETE FROM plugin_filepaths_paths WHERE nodeid = ?', undef, $nodeid);
        
        return $self->err_line("delete_node_failed") if $dbh->err;

        cache_invalidate($deleted_path,  $args->{client_id});
        
        return $self->ok_line();
    });
    
    
    #create a node (directory/link) - NOT A FILE!
    MogileFS::register_worker_command( 'filepaths_create_node', sub {
        # get parameters
        my MogileFS::Worker::Query $self = shift;
        my $args = shift;

        # verify domain firstly
        my $dmid = $self->check_domain($args)
            or return $self->err_line('domain_not_found');

        return $self->err_line("plugin_not_active_for_domain")
            unless _check_dmid($dmid);

        # verify arguments - first is always the full path to the node..
        my $path = $args->{arg1};
        return $self->err_line('bad_params', 'missing path')
            unless $args->{argcount} > 1 && $path;
        return $self->err_line('bad_params', "invalid path ($path)")
            unless $path =~ /^\//;

        my $type = $args->{arg2};
        my $ntype = 1;
        
        return $self->err_line('bad_params', "invalid node type: $type")
            unless $args->{argcount} > 1 && $type && ($type eq 'L' || $type eq 'D');
            
        my $link = $args->{arg3};
        if($type eq 'L') {
          $ntype = 2;
          return $self->err_line('bad_params', 'missing link destination')
            unless $args->{argcount} >= 3 && $link;
        }
        
        
        my ($node_path, $node_name) = _parse_path($path);
        
        # great, let's vivify that path and get the node to it
        my $parentnodeid = MogileFS::Plugin::FilePaths::vivify_path( $dmid, $node_path );
        
        return $self->err_line("parent_path_not_found") unless defined $parentnodeid;
        
        my $dbh = Mgd::get_dbh();
        my $nodeid = _find_node($dbh, $dmid, $parentnodeid, $node_name, 1, $ntype, undef, time, $link);
        return $self->err_line("create_node_failed")  unless $nodeid;
    
        
        cache_invalidate(resolve_path($args->{arg1},$nodeid),  $args->{client_id} );
        my %res;
        $res{'nid'} = $nodeid;
        return $self->ok_line(\%res);
    });
    
    
    #get disk usage
    MogileFS::register_worker_command( 'filepaths_stats', sub {
        # get parameters
        my MogileFS::Worker::Query $self = shift;
        my $args = shift;

       
        
        my $dbh = Mgd::get_dbh();
       
        my ($mb_total, $mb_used) = $dbh->selectrow_array('SELECT sum(mb_total), sum(mb_used) FROM device where status="alive"', undef);
        
        
        return $self->err_line("filepaths_stats_failed")  unless defined($mb_total);

        my %res;
        $res{'mb_total'} = $mb_total;
        $res{'mb_used'} = $mb_used;
        return $self->ok_line(\%res);
    });
    
    #check for orphaned directories/files
    MogileFS::register_worker_command( 'filepaths_check_fs', sub {
        # get parameters
        my MogileFS::Worker::Query $self = shift;
        my $args = shift;

         # verify domain firstly
        my $dmid = $self->check_domain($args)
            or return $self->err_line('domain_not_found');

        return $self->err_line("plugin_not_active_for_domain")
            unless _check_dmid($dmid);
            
        my $getcount = $args->{arg1};
            
        my $dbh = Mgd::get_dbh();
        
        my %res;
         
        if($getcount == 1) {
          my ($total) = $dbh->selectrow_array('select count(*) from plugin_filepaths_paths p1 left join plugin_filepaths_paths p2 on(p1.parentnodeid = p2.nodeid and p1.dmid=p2.dmid) where p2.nodeid is null and p1.parentnodeid <> 0 and p1.dmid=?', undef, $dmid);
          $res{'total'} = $total;
        }
        
        
        
        my $sth = $dbh->prepare('select p1.nodeid, p1.parentnodeid, p1.type, p1.nodename from plugin_filepaths_paths p1 left join plugin_filepaths_paths p2 on(p1.parentnodeid = p2.nodeid and p1.dmid=p2.dmid) where p2.nodeid is null and p1.parentnodeid <> 0 and p1.dmid=? limit 1000');
        
        #my $found = 1;
        my $fixed=0;
        #while($found) {
          #$found = 0;
          $sth->execute($dmid);
          while (my ($nodeid, $parentnodeid, $ntype, $nodename) = $sth->fetchrow_array) {
              #$found = 1;
              #we need to make a path for the file in lost & found that can scale
              #path will be /lost+found/[f|d]/[nnn]/[nnn]/[nnn]/[lost file]
              my @nums = $parentnodeid =~ /\d{1,3}/g;
              my $parentpath = join("/", @nums);
              
              
              my $ftypedir = 'f';
              if($ntype == 1) {
                $ftypedir = 'd';
              }
              
              my $destdir = "lost+found/$ftypedir/$parentpath";
              #error("parentnodeid=$parentnodeid, parentpath=$parentpath, destdir=$destdir");
              
              # great, let's vivify that path and get the node to it
              my $destid = MogileFS::Plugin::FilePaths::vivify_path( $dmid, $destdir );
              return $self->err_line("vivify_path failed")  unless defined $destid;
              
              my $renamed=0;
              my $renamecount=1;
              my $renameto = $nodename;
              while(!$renamed) {
                eval {
                  $dbh->do('UPDATE plugin_filepaths_paths SET parentnodeid=?, nodename=? WHERE nodeid=?', undef,
                         $destid, $renameto, $nodeid);
                  $renamed = 1;
                };
                if($@) {
                  my $estr = $@;
                  if($estr =~ /Duplicate/) {
                    $renameto = "$nodename.$renamecount";
                    $renamecount += 1;
                  } else {
                    return $self->err_line("rename_failed", $estr);
                  }
                }
              }
              $fixed += 1;
          }
          #$found=0;
        #}

       
        $res{'fixed'} = $fixed;
        return $self->ok_line(\%res);
    });
    

    return 1;
}

# this sub is called at the end or when the module is being unloaded, this needs to
# unregister any registered methods, etc.  you MUST un-install everything that the
# plugin has previously installed.
sub unload {

    # remove our hooks
    MogileFS::unregister_global_hook( 'cmd_create_open' );
    MogileFS::unregister_global_hook( 'cmd_create_close' );
    MogileFS::unregister_global_hook( 'file_stored' );

    return 1;
}

# called when you want to create a path, this will break down the given argument and
# create any elements needed, returning the nodeid of the final node.  returns undef
# on error, else, 0-N is valid.
sub vivify_path {
    my ($dmid, $path) = @_;
    return undef unless $dmid && $path;
    return _traverse_path($dmid, $path, 1);
}

# called to load the nodeid of the final element in a path, which is useful for finding
# out if a path exists.  does NOT automatically create path elements that don't exist.
sub load_path {
    my ($dmid, $path) = @_;
    return undef unless $dmid && defined($path);
    return _traverse_path($dmid, $path, 0);
}

#make sure the path does not have a |nodeid| in it, if it does, resolve it out 
sub resolve_path {
    my ($path, $nodeid) = @_;
    if($path =~ /\|/) {
      $path = generate_path($nodeid);
    }
    return $path;
}

#generate a path from a node id
sub generate_path {
    my ($nodeid) = @_;
    return undef unless $nodeid;
    
    
    my $dbh = Mgd::get_dbh();
      
    my $sth = $dbh->prepare('SELECT plugin_filepaths_paths.nodename, plugin_filepaths_paths.parentnodeid  FROM plugin_filepaths_paths WHERE plugin_filepaths_paths.nodeid = ?');
    $sth->execute($nodeid);
    
    my ($nodename, $pid) = $sth->fetchrow_array;
    
    if($pid == 0) {
      return "/$nodename";
    } else {
      my $parent_path = generate_path($pid);
      return "/$nodename" unless $parent_path;
      return "$parent_path/$nodename";
    }
}

# does the internal work of traversing a path
sub _traverse_path {
    my ($dmid, $path, $vivify) = @_;
    return undef unless $dmid && defined $path;

    my @paths = grep { defined($_) && $_ ne '' }  split /\//, $path;
    
    return 0 unless @paths; #toplevel

    # FIXME: validate_dbh()? or not needed? assumed done elsewhere? bleh.
    my $dbh = Mgd::get_dbh();
    return undef unless $dbh;

    my $parentnodeid = 0;
    foreach my $node (@paths) {
      if($node =~ /\|(\d*)\|/) { #we accept |nodeid| as a path element (quick lookup that can handle async renames)
        $parentnodeid = $1
      } else {
        # try to get the id for this node
        my $nodeid = _find_node($dbh, $dmid, $parentnodeid, $node, $vivify, 1, undef, time, undef);
        return undef unless $nodeid;

        # this becomes the new parent
        $parentnodeid = $nodeid;
      }
    }

    # we're done, so the parentnodeid is what we return
    return $parentnodeid;
}

# checks to see if a node exists, and if not, creates it if $vivify is set
sub _find_node {
    my ($dbh, $dmid, $parentnodeid, $node, $vivify, $nodetype, $fid, $lastmodified, $link) = @_;
    return undef unless $dbh && $dmid && defined $parentnodeid && defined $node;
    
    my ($nodeid, $created) = _find_node_ex($dbh, $dmid, $parentnodeid, $node, $vivify, $nodetype, $fid, $lastmodified, $link);
    return $nodeid;
}

#find node, and also return if it was created on the fly..
sub _find_node_ex {
    my ($dbh, $dmid, $parentnodeid, $node, $vivify, $nodetype, $fid, $lastmodified, $link) = @_;
    return (undef, 0) unless $dbh && $dmid && defined $parentnodeid && defined $node;

    my $nodeid = $dbh->selectrow_array('SELECT nodeid FROM plugin_filepaths_paths ' .
                                       'WHERE dmid = ? AND parentnodeid = ? AND nodename = ?',
                                       undef, $dmid, $parentnodeid, $node);
    return (undef, 0) if $dbh->err;
    return ($nodeid, 0) if $nodeid;

    if ($vivify) {
        eval {
          $dbh->do('INSERT INTO plugin_filepaths_paths (nodeid, dmid, parentnodeid, nodename, type, fid, lastmodified, link) ' .
                 'VALUES (NULL, ?, ?, ?, ?, ?, ?, ?)', undef, $dmid, $parentnodeid, $node, $nodetype, $fid, $lastmodified, $link);
        };
        if($@) {
          my $estr = $@;
          if($estr =~ /Duplicate/) {
            error("Recoverable Duplicate occured in _find_node_ex when inserting entry $node: $estr");
            my $nodeid = $dbh->selectrow_array('SELECT nodeid FROM plugin_filepaths_paths ' .
                                       'WHERE dmid = ? AND parentnodeid = ? AND nodename = ?',
                                       undef, $dmid, $parentnodeid, $node);
            return ($nodeid, 0) if $nodeid;
          } else {
            error("Unrecoverable Error occured in _find_node_ex when inserting entry $node: $estr");
            return (undef, 0);
          }
        } else {
          $nodeid = $dbh->{mysql_insertid}+0;
        }
    }

    return (undef, 0) unless $nodeid && $nodeid > 0;
    return ($nodeid, 1);
}

# sets the mapping of a file from a name to a fid
sub set_file_mapping {
    my ($dmid, $parentnodeid, $filename, $fid, $mtime) = @_;
    return undef unless $dmid && defined $parentnodeid && defined $filename && $fid;

    my $dbh = Mgd::get_dbh();
    return undef unless $dbh;

    $mtime = time unless defined($mtime) && $mtime > 0;
    
    my ($nodeid, $created) = _find_node_ex($dbh, $dmid, $parentnodeid, $filename, 1, 0, $fid, $mtime, undef);
    return undef unless $nodeid;

    if($created == 0) {
      #print STDERR "set_file_mapping: file already existed\n";
      #it alread existsed... update the last modified...
      $dbh->do("UPDATE plugin_filepaths_paths SET lastmodified = ?, fid = ? WHERE nodeid = ?", undef, $mtime,$fid, $nodeid);
      return undef if $dbh->err;
    } else {
      #print STDERR "set_file_mapping: new file, not setting lastmodified\n";
    }
    #$dbh->do("UPDATE plugin_filepaths_paths SET fid = ? WHERE nodeid = ?", undef, $fid, $nodeid);
    #return undef if $dbh->err;
    return $nodeid;
}

# given a domain and parent node and filename, return the fid
sub get_file_mapping {
    my ($dmid, $parentnodeid, $filename) = @_;
    return undef unless $dmid && defined $parentnodeid && defined $filename;

    my $dbh = Mgd::get_dbh();
    return undef unless $dbh;

    my $fid = $dbh->selectrow_array('SELECT fid FROM plugin_filepaths_paths ' .
                                    'WHERE dmid = ? AND parentnodeid = ? AND nodename = ?',
                                    undef, $dmid, $parentnodeid, $filename);
    return undef if $dbh->err;
    return undef unless $fid && $fid > 0;
    return $fid;
}


sub delete_file_mapping {
    my ($dmid, $parentnodeid, $filename,) = @_;
    return undef unless $dmid && defined $parentnodeid && defined $filename;

    my $dbh = Mgd::get_dbh();
    return undef unless $dbh;

    $dbh->do('DELETE FROM plugin_filepaths_paths WHERE dmid = ? AND parentnodeid = ? AND nodename = ?',
             undef, $dmid, $parentnodeid, $filename);

    return undef if $dbh->err;
    return 1;
}

sub list_directory {
    my ($dmid, $nodeid) = @_;

    my $dbh = Mgd::get_dbh();
    return undef unless $dbh;

    my $sth = $dbh->prepare('SELECT plugin_filepaths_paths.nodename, plugin_filepaths_paths.fid, plugin_filepaths_paths.type, plugin_filepaths_paths.lastmodified, file.length, plugin_filepaths_paths.link, plugin_filepaths_paths.nodeid FROM plugin_filepaths_paths left join file on(plugin_filepaths_paths.fid = file.fid) ' .
                            'WHERE plugin_filepaths_paths.dmid = ? AND parentnodeid = ?');

    $sth->execute($dmid, $nodeid);

    my @return;

    while (my ($nodename, $fid, $type, $lastmodified, $size, $link, $nid) = $sth->fetchrow_array) {
        push @return, [$nodename, $fid, $type, $lastmodified, $size, $link, $nid];
    }

    return @return;
}

# generic sub that converts a file path to a key name that
# MogileFS will understand
sub _path_to_key {
    my $args = shift;

    my $dmid = $args->{dmid};
    return 1 unless _check_dmid($dmid);

    # ensure we got a valid seeming path and filename
    
    my ($path, $filename) = _parse_path($args->{key});
    
    #my ($path, $filename) =
    #    ($args->{key} =~ m!^(/(?:[\~\w\s\-\.\|\#\+]+/)*)([\~\w\s\-\.\|\#\+]+)$!) ? ($1, $2) : (undef, undef);
    return 0 unless $path && defined $filename;

    # now try to get the end of the path
    my $parentnodeid = MogileFS::Plugin::FilePaths::load_path( $dmid, $path );
    unless(defined $parentnodeid) {
      $args->{key} = 'this_key_should_not_exist_so_mogile_fs_will_return_normal_file_not_found';
      return 1; 
    }

    # great, find this file
    my $fid = MogileFS::Plugin::FilePaths::get_file_mapping( $dmid, $parentnodeid, $filename );
    #return 0 unless defined $fid && $fid > 0;
    unless(defined $fid && $fid > 0) {
      $args->{key} = 'this_key_should_not_exist_so_mogile_fs_will_return_normal_file_not_found';
      return 1; 
    }

    #print STDERR "_path_to_key $path$filename = $fid\n";
    # now pretend they asked for it and continue
    $args->{key} = "fid:$fid";
    return 1;
}

my %active_dmids;
my $last_dmid_check = 0;

sub _check_dmid {
    my $dmid = shift;

    return unless defined $dmid;

    my $time = time();
    if ($time >= $last_dmid_check + 15) {
        $last_dmid_check = $time;

        unless (_load_dmids()) {
            warn "Unable to load active domains list for filepaths plugin, using old list";
        }
    }

    return $active_dmids{$dmid};
}

sub _load_dmids {
    my $dbh = Mgd::get_dbh();
    return undef unless $dbh;

    my $sth = $dbh->prepare('SELECT dmid FROM plugin_filepaths_domains');
    $sth->execute();

    return undef if $sth->err;

    %active_dmids = ();

    while (my $dmid = $sth->fetchrow_array) {
        $active_dmids{$dmid} = 1;
    }
    return 1;
}

package MogileFS::Store;

use MogileFS::Store;

use strict;
use warnings;

sub TABLE_plugin_filepaths_paths {
    "CREATE TABLE plugin_filepaths_paths (
        nodeid BIGINT UNSIGNED NOT NULL AUTO_INCREMENT,
        dmid SMALLINT UNSIGNED NOT NULL,
        parentnodeid BIGINT UNSIGNED NOT NULL,
        nodename VARCHAR(255) BINARY NOT NULL,
        type TINYINT UNSIGNED NOT NULL,
        fid BIGINT UNSIGNED,
        lastmodified BIGINT UNSIGNED NOT NULL,
        link VARCHAR(1000) DEFAULT NULL, 
        PRIMARY KEY (nodeid),
        UNIQUE KEY (dmid, parentnodeid, nodename)
)"
};


sub TABLE_plugin_filepaths_domains {
    "CREATE TABLE plugin_filepaths_domains (
        dmid SMALLINT UNSIGNED NOT NULL,
        PRIMARY KEY (dmid)
)"
}

__PACKAGE__->add_extra_tables("plugin_filepaths_paths", "plugin_filepaths_domains");

1;
