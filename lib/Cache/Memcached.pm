# $Id$
#
# Copyright (c) 2003, 2004  Brad Fitzpatrick <brad@danga.com>
#
# See COPYRIGHT section in pod text below for usage and distribution rights.
#

package Cache::Memcached;

use strict;
no strict 'refs';
use Storable ();
use Socket qw( MSG_NOSIGNAL PF_INET IPPROTO_TCP SOCK_STREAM );
use IO::Handle ();
use Time::HiRes ();
use String::CRC32;
use Errno qw( EINPROGRESS EWOULDBLOCK EISCONN );

use fields qw{
    debug no_rehash stats compress_threshold compress_enable stat_callback
    readonly select_timeout namespace namespace_len servers active buckets
    pref_ip
    bucketcount _single_sock _stime
    connect_timeout cb_connect_fail
};

# flag definitions
use constant F_STORABLE => 1;
use constant F_COMPRESS => 2;

# size savings required before saving compressed value
use constant COMPRESS_SAVINGS => 0.20; # percent

use vars qw($VERSION $HAVE_ZLIB $FLAG_NOSIGNAL);
$VERSION = "1.18";

BEGIN {
    $HAVE_ZLIB = eval "use Compress::Zlib (); 1;";
}

$FLAG_NOSIGNAL = 0;
eval { $FLAG_NOSIGNAL = MSG_NOSIGNAL; };

my %host_dead;   # host -> unixtime marked dead until
my %cache_sock;  # host -> socket

my $PROTO_TCP;

our $SOCK_TIMEOUT = 2.6; # default timeout in seconds

sub new {
    my Cache::Memcached $self = shift;
    $self = fields::new( $self ) unless ref $self;

    my ($args) = @_;

    $self->set_servers($args->{'servers'});
    $self->{'debug'} = $args->{'debug'} || 0;
    $self->{'no_rehash'} = $args->{'no_rehash'};
    $self->{'stats'} = {};
    $self->{'pref_ip'} = $args->{'pref_ip'} || {};
    $self->{'compress_threshold'} = $args->{'compress_threshold'};
    $self->{'compress_enable'}    = 1;
    $self->{'stat_callback'} = $args->{'stat_callback'} || undef;
    $self->{'readonly'} = $args->{'readonly'};

    # TODO: undocumented
    $self->{'connect_timeout'} = $args->{'connect_timeout'} || 0.25;
    $self->{'select_timeout'}  = $args->{'select_timeout'}  || 1.0;
    $self->{namespace} = $args->{namespace} || '';
    $self->{namespace_len} = length $self->{namespace};

    return $self;
}

sub set_pref_ip {
    my Cache::Memcached $self = shift;
    $self->{'pref_ip'} = shift;
}

sub set_servers {
    my Cache::Memcached $self = shift;
    my ($list) = @_;
    $self->{'servers'} = $list || [];
    $self->{'active'} = scalar @{$self->{'servers'}};
    $self->{'buckets'} = undef;
    $self->{'bucketcount'} = 0;

    $self->{'_single_sock'} = undef;
    if (@{$self->{'servers'}} == 1) {
        $self->{'_single_sock'} = $self->{'servers'}[0];
    }

    return $self;
}

sub set_cb_connect_fail {
    my Cache::Memcached $self = shift;
    $self->{'cb_connect_fail'} = shift;
}

sub set_connect_timeout {
    my Cache::Memcached $self = shift;
    $self->{'connect_timeout'} = shift;
}

sub set_debug {
    my Cache::Memcached $self = shift;
    my ($dbg) = @_;
    $self->{'debug'} = $dbg || 0;
}

sub set_readonly {
    my Cache::Memcached $self = shift;
    my ($ro) = @_;
    $self->{'readonly'} = $ro;
}

sub set_norehash {
    my Cache::Memcached $self = shift;
    my ($val) = @_;
    $self->{'no_rehash'} = $val;
}

sub set_compress_threshold {
    my Cache::Memcached $self = shift;
    my ($thresh) = @_;
    $self->{'compress_threshold'} = $thresh;
}

sub enable_compress {
    my Cache::Memcached $self = shift;
    my ($enable) = @_;
    $self->{'compress_enable'} = $enable;
}

sub forget_dead_hosts {
    %host_dead = ();
}

sub set_stat_callback {
    my Cache::Memcached $self = shift;
    my ($stat_callback) = @_;
    $self->{'stat_callback'} = $stat_callback;
}

sub _dead_sock {
    my ($sock, $ret, $dead_for) = @_;
    if ($sock =~ /^Sock_(.+?):(\d+)$/) {
        my $now = time();
        my ($ip, $port) = ($1, $2);
        my $host = "$ip:$port";
        $host_dead{$host} = $now + $dead_for
            if $dead_for;
        delete $cache_sock{$host};
    }
    return $ret;  # 0 or undef, probably, depending on what caller wants
}

sub _close_sock {
    my ($sock) = @_;
    if ($sock =~ /^Sock_(.+?):(\d+)$/) {
        my ($ip, $port) = ($1, $2);
        my $host = "$ip:$port";
        close $sock;
        delete $cache_sock{$host};
    }
}

sub _connect_sock { # sock, sin, timeout
    my ($sock, $sin, $timeout) = @_;
    $timeout = 0.25 if not defined $timeout;

    # make the socket non-blocking from now on,
    # except if someone wants 0 timeout, meaning
    # a blocking connect, but even then turn it
    # non-blocking at the end of this function

    if ($timeout) {
        IO::Handle::blocking($sock, 0);
    } else {
        IO::Handle::blocking($sock, 1);
    }

    my $ret = connect($sock, $sin);

    if (!$ret && $timeout && $!==EINPROGRESS) {

        my $win='';
        vec($win, fileno($sock), 1) = 1;

        if (select(undef, $win, undef, $timeout) > 0) {
            $ret = connect($sock, $sin);
            # EISCONN means connected & won't re-connect, so success
            $ret = 1 if !$ret && $!==EISCONN;
        }
    }

    unless ($timeout) { # socket was temporarily blocking, now revert
        IO::Handle::blocking($sock, 0);
    }

    # from here on, we use non-blocking (async) IO for the duration
    # of the socket's life

    return $ret;
}

sub sock_to_host { # (host)
    my Cache::Memcached $self = ref $_[0] ? shift : undef;
    my $host = $_[0];
    return $cache_sock{$host} if $cache_sock{$host};

    my $now = time();
    my ($ip, $port) = $host =~ /(.*):(\d+)/;
    return undef if
        $host_dead{$host} && $host_dead{$host} > $now;
    my $sock = "Sock_$host";

    my $connected = 0;
    my $sin;
    my $proto = $PROTO_TCP ||= getprotobyname('tcp');

    # if a preferred IP is known, try that first.
    if ($self && $self->{pref_ip}{$ip}) {
        socket($sock, PF_INET, SOCK_STREAM, $proto);
        my $prefip = $self->{pref_ip}{$ip};
        $sin = Socket::sockaddr_in($port,Socket::inet_aton($prefip));
        if (_connect_sock($sock,$sin,$self->{connect_timeout})) {
            $connected = 1;
        } else {
            if (my $cb = $self->{cb_connect_fail}) {
                $cb->($prefip);
            }
            close $sock;
        }
    }

    # normal path, or fallback path if preferred IP failed
    unless ($connected) {
        socket($sock, PF_INET, SOCK_STREAM, $proto);
        $sin = Socket::sockaddr_in($port,Socket::inet_aton($ip));
        my $timeout = $self ? $self->{connect_timeout} : 0.25;
        unless (_connect_sock($sock,$sin,$timeout)) {
            my $cb = $self ? $self->{cb_connect_fail} : undef;
            $cb->($ip) if $cb;
            return _dead_sock($sock, undef, 20 + int(rand(10)));
        }
    }

    # make the new socket not buffer writes.
    my $old = select($sock);
    $| = 1;
    select($old);

    return $cache_sock{$host} = $sock;
}

sub get_sock { # (key)
    my Cache::Memcached $self = shift;
    my ($key) = @_;
    return $self->sock_to_host($self->{'_single_sock'}) if $self->{'_single_sock'};
    return undef unless $self->{'active'};
    my $hv = ref $key ? int($key->[0]) : _hashfunc($key);

    $self->init_buckets() unless $self->{'buckets'};

    my $real_key = ref $key ? $key->[1] : $key;
    my $tries = 0;
    while ($tries++ < 20) {
        my $host = $self->{'buckets'}->[$hv % $self->{'bucketcount'}];
        my $sock = $self->sock_to_host($host);
        return $sock if $sock;
        return undef if $self->{'no_rehash'};
        $hv += _hashfunc($tries . $real_key);  # stupid, but works
    }
    return undef;
}

sub init_buckets {
    my Cache::Memcached $self = shift;
    return if $self->{'buckets'};
    my $bu = $self->{'buckets'} = [];
    foreach my $v (@{$self->{'servers'}}) {
        if (ref $v eq "ARRAY") {
            for (1..$v->[1]) { push @$bu, $v->[0]; }
        } else {
            push @$bu, $v;
        }
    }
    $self->{'bucketcount'} = scalar @{$self->{'buckets'}};
}

sub disconnect_all {
    my $sock;
    foreach $sock (values %cache_sock) {
        close $sock;
    }
    %cache_sock = ();
}

# writes a line, then reads result.  by default stops reading after a
# single line, but caller can override the $check_complete subref,
# which gets passed a scalarref of buffer read thus far.
sub _write_and_read {
    my Cache::Memcached $self = shift;
    my ($sock, $line, $check_complete) = @_;
    my $res;
    my ($ret, $offset) = (undef, 0);

    $check_complete ||= sub {
        return (rindex($ret, "\r\n") + 2 == length($ret));
    };

    # state: 0 - writing, 1 - reading, 2 - done
    my $state = 0;

    # the bitsets for select
    my ($rin, $rout, $win, $wout);
    my $nfound;

    my $copy_state = -1;
    local $SIG{'PIPE'} = "IGNORE" unless $FLAG_NOSIGNAL;

    # the select loop
    while(1) {
        if ($copy_state!=$state) {
            last if $state==2;
            ($rin, $win) = ('', '');
            vec($rin, fileno($sock), 1) = 1 if $state==1;
            vec($win, fileno($sock), 1) = 1 if $state==0;
            $copy_state = $state;
        }
        $nfound = select($rout=$rin, $wout=$win, undef,
                         $self->{'select_timeout'});
        last unless $nfound;

        if (vec($wout, fileno($sock), 1)) {
            $res = send($sock, $line, $FLAG_NOSIGNAL);
            next
                if not defined $res and $!==EWOULDBLOCK;
            unless ($res > 0) {
                _close_sock($sock);
                return undef;
            }
            if ($res == length($line)) { # all sent
                $state = 1;
            } else { # we only succeeded in sending some of it
                substr($line, 0, $res, ''); # delete the part we sent
            }
        }

        if (vec($rout, fileno($sock), 1)) {
            $res = sysread($sock, $ret, 255, $offset);
            next
                if !defined($res) and $!==EWOULDBLOCK;
            if ($res == 0) { # catches 0=conn closed or undef=error
                _close_sock($sock);
                return undef;
            }
            $offset += $res;
            $state = 2 if $check_complete->(\$ret);
        }
    }

    unless ($state == 2) {
        _dead_sock($sock); # improperly finished
        return undef;
    }

    return $ret;
}


sub delete {
    my Cache::Memcached $self = shift;
    my ($key, $time) = @_;
    return 0 if ! $self->{'active'} || $self->{'readonly'};
    my $stime = Time::HiRes::time() if $self->{'stat_callback'};
    my $sock = $self->get_sock($key);
    return 0 unless $sock;

    $self->{'stats'}->{"delete"}++;
    $key = ref $key ? $key->[1] : $key;
    $time = $time ? " $time" : "";
    my $cmd = "delete $self->{namespace}$key$time\r\n";
    my $res = _write_and_read($self, $sock, $cmd);

    if ($self->{'stat_callback'}) {
        my $etime = Time::HiRes::time();
        $self->{'stat_callback'}->($stime, $etime, $sock, 'delete');
    }

    return $res eq "DELETED\r\n";
}

sub add {
    _set("add", @_);
}

sub replace {
    _set("replace", @_);
}

sub set {
    _set("set", @_);
}

sub _set {
    my $cmdname = shift;
    my Cache::Memcached $self = shift;
    my ($key, $val, $exptime) = @_;
    return 0 if ! $self->{'active'} || $self->{'readonly'};
    my $stime = Time::HiRes::time() if $self->{'stat_callback'};
    my $sock = $self->get_sock($key);
    return 0 unless $sock;

    use bytes; # return bytes from length()

    $self->{'stats'}->{$cmdname}++;
    my $flags = 0;
    $key = ref $key ? $key->[1] : $key;

    if (ref $val) {
        $val = Storable::nfreeze($val);
        $flags |= F_STORABLE;
    }

    my $len = length($val);

    if ($self->{'compress_threshold'} && $HAVE_ZLIB && $self->{'compress_enable'} &&
        $len >= $self->{'compress_threshold'}) {

        my $c_val = Compress::Zlib::memGzip($val);
        my $c_len = length($c_val);

        # do we want to keep it?
        if ($c_len < $len*(1 - COMPRESS_SAVINGS)) {
            $val = $c_val;
            $len = $c_len;
            $flags |= F_COMPRESS;
        }
    }

    $exptime = int($exptime || 0);

    local $SIG{'PIPE'} = "IGNORE" unless $FLAG_NOSIGNAL;
    my $line = "$cmdname $self->{namespace}$key $flags $exptime $len\r\n$val\r\n";

    my $res = _write_and_read($self, $sock, $line);

    if ($self->{'debug'} && $line) {
        chop $line; chop $line;
        print STDERR "Cache::Memcache: $cmdname $self->{namespace}$key = $val ($line)\n";
    }

    if ($self->{'stat_callback'}) {
        my $etime = Time::HiRes::time();
        $self->{'stat_callback'}->($stime, $etime, $sock, $cmdname);
    }

    return $res eq "STORED\r\n";
}

sub incr {
    _incrdecr("incr", @_);
}

sub decr {
    _incrdecr("decr", @_);
}

sub _incrdecr {
    my $cmdname = shift;
    my Cache::Memcached $self = shift;
    my ($key, $value) = @_;
    return undef if ! $self->{'active'} || $self->{'readonly'};
    my $stime = Time::HiRes::time() if $self->{'stat_callback'};
    my $sock = $self->get_sock($key);
    return undef unless $sock;
    $key = $key->[1] if ref $key;
    $self->{'stats'}->{$cmdname}++;
    $value = 1 unless defined $value;

    my $line = "$cmdname $self->{namespace}$key $value\r\n";
    my $res = _write_and_read($self, $sock, $line);

    if ($self->{'stat_callback'}) {
        my $etime = Time::HiRes::time();
        $self->{'stat_callback'}->($stime, $etime, $sock, $cmdname);
    }

    return undef unless $res =~ /^(\d+)/;
    return $1;
}

sub get {
    my Cache::Memcached $self = shift;
    my ($key) = @_;

    # TODO: make a fast path for this?  or just keep using get_multi?
    my $r = $self->get_multi($key);
    my $kval = ref $key ? $key->[1] : $key;
    return $r->{$kval};
}

sub get_multi {
    my Cache::Memcached $self = shift;
    return undef unless $self->{'active'};
    $self->{'_stime'} = Time::HiRes::time() if $self->{'stat_callback'};
    $self->{'stats'}->{"get_multi"}++;
    my %val;        # what we'll be returning a reference to (realkey -> value)
    my %sock_keys;  # sockref_as_scalar -> [ realkeys ]
    my $sock;

    foreach my $key (@_) {
        $sock = $self->get_sock($key);
        next unless $sock;
        my $kval = ref $key ? $key->[1] : $key;
        push @{$sock_keys{$sock}}, $kval;
    }
    $self->{'stats'}->{"get_keys"} += @_;
    $self->{'stats'}->{"get_socks"} += keys %sock_keys;

    local $SIG{'PIPE'} = "IGNORE" unless $FLAG_NOSIGNAL;

    _load_multi($self, \%sock_keys, \%val);

    if ($self->{'debug'}) {
        while (my ($k, $v) = each %val) {
            print STDERR "MemCache: got $k = $v\n";
        }
    }
    return \%val;
}

sub _load_multi {
    use bytes; # return bytes from length()
    my Cache::Memcached $self = shift;
    my ($sock_keys, $ret) = @_;

    # all keyed by a $sock:
    my %reading; # bool, whether we're reading from this socket
    my %writing; # bool, whether we're writing into this socket
    my %state;   # reading state:
                 # 0 = waiting for a line, N = reading N bytes
    my %buf;     # buffers
    my %offset;  # offsets to read into buffers
    my %key;     # current key per socket
    my %flags;   # flags per socket

    foreach (keys %$sock_keys) {
        print STDERR "processing socket $_\n" if $self->{'debug'} >= 2;
        $writing{$_} = 1;
        $buf{$_} = "get ". join(" ", map { "$self->{namespace}$_" } @{$sock_keys->{$_}}) . "\r\n";
    }

    my $active_changed = 1; # force rebuilding of select sets

    my $dead = sub {
        my $sock = shift;
        print STDERR "killing socket $sock\n" if $self->{'debug'} >= 2;
        delete $reading{$sock};
        delete $writing{$sock};
        delete $ret->{$key{$sock}}
            if $key{$sock};

        if ($self->{'stat_callback'}) {
            my $etime = Time::HiRes::time();
            $self->{'stat_callback'}->($self->{'_stime'}, $etime, $sock, 'get_multi');
        }

        close $sock;
        _dead_sock($sock);
        $active_changed = 1;
    };

    my $finalize = sub {
        my $sock = shift;
        my $k = $key{$sock};

        # remove trailing \r\n
        chop $ret->{$k}; chop $ret->{$k};

        unless (length($ret->{$k}) == $state{$sock}-2) {
            $dead->($sock);
            return;
        }

        $ret->{$k} = Compress::Zlib::memGunzip($ret->{$k})
            if $HAVE_ZLIB && $flags{$sock} & F_COMPRESS;
        if ($flags{$sock} & F_STORABLE) {
            # wrapped in eval in case a perl 5.6 Storable tries to
            # unthaw data from a perl 5.8 Storable.  (5.6 is stupid
            # and dies if the version number changes at all.  in 5.8
            # they made it only die if it unencounters a new feature)
            eval {
                $ret->{$k} = Storable::thaw($ret->{$k});
            };
            # so if there was a problem, just treat it as a cache miss.
            if ($@) {
                delete $ret->{$k};
            }
        }
    };

    my $read = sub {
        my $sock = shift;
        my $res;

        # where are we reading into?
        if ($state{$sock}) { # reading value into $ret
            $res = sysread($sock, $ret->{$key{$sock}},
                           $state{$sock} - $offset{$sock},
                           $offset{$sock});
            return
                if !defined($res) and $!==EWOULDBLOCK;
            if ($res == 0) { # catches 0=conn closed or undef=error
                $dead->($sock);
                return;
            }
            $offset{$sock} += $res;
            if ($offset{$sock} == $state{$sock}) { # finished reading
                $finalize->($sock);
                $state{$sock} = 0; # wait for another VALUE line or END
                $offset{$sock} = 0;
            }
            return;
        }

        # we're reading a single line.
        # first, read whatever's there, but be satisfied with 2048 bytes
        $res = sysread($sock, $buf{$sock},
                       2048, $offset{$sock});
        return
            if !defined($res) and $!==EWOULDBLOCK;
        if ($res == 0) {
            $dead->($sock);
            return;
        }
        $offset{$sock} += $res;


        # Below is a hot path.  In preparation for rewriting it in Perl/C,
        # here are some notes.
        #
        # The while(1) below uses:
        #   %buf
        #   %reading
        #   $active_changed
        #   %key, %flags, %state, %offset
        #   $finalize (CV)
        #   $self->{namespace_len}

      SEARCH:
        while(1) { # may have to search many times
            # do we have a complete END line?
            if ($buf{$sock} =~ /^END\r\n/) {
                # okay, finished with this socket
                delete $reading{$sock};
                $active_changed = 1;
                return;
            }

            # do we have a complete VALUE line?
            if ($buf{$sock} =~ /^VALUE (\S+) (\d+) (\d+)\r\n/) {
                ($key{$sock}, $flags{$sock}, $state{$sock}) =
                    (substr($1, $self->{namespace_len}), int($2), $3+2);
                # Note: we use $+[0] and not pos($buf{$sock}) because pos()
                # seems to have problems under perl's taint mode.  nobody
                # on the list discovered why, but this seems a reasonable
                # work-around:
                my $p = $+[0];
                my $len = length($buf{$sock});
                my $copy = $len-$p > $state{$sock} ? $state{$sock} : $len-$p;
                $ret->{$key{$sock}} = substr($buf{$sock}, $p, $copy)
                    if $copy;
                $offset{$sock} = $copy;
                substr($buf{$sock}, 0, $p+$copy, ''); # delete the stuff we used
                if ($offset{$sock} == $state{$sock}) { # have it all?
                    $finalize->($sock);
                    $state{$sock} = 0; # wait for another VALUE line or END
                    $offset{$sock} = 0;
                    next SEARCH; # look again
                }
                last SEARCH; # buffer is empty now
            }

            # if we're here probably means we only have a partial VALUE
            # or END line in the buffer. Could happen with multi-get,
            # though probably very rarely. Exit the loop and let it read
            # more.

            # but first, make sure subsequent reads don't destroy our
            # partial VALUE/END line.
            $offset{$sock} = length($buf{$sock});
            last SEARCH;
        }

        # we don't have a complete line, wait and read more when ready
        return;
    };

    my $write = sub {
        my $sock = shift;
        my $res;

        $res = send($sock, $buf{$sock}, $FLAG_NOSIGNAL);
        return
            if not defined $res and $!==EWOULDBLOCK;
        unless ($res > 0) {
            $dead->($sock);
            return;
        }
        if ($res == length($buf{$sock})) { # all sent
            $buf{$sock} = "";
            $offset{$sock} = $state{$sock} = 0;
            # switch the socket from writing state to reading state
            delete $writing{$sock};
            $reading{$sock} = 1;
            $active_changed = 1;
        } else { # we only succeeded in sending some of it
            substr($buf{$sock}, 0, $res, ''); # delete the part we sent
        }
        return;
    };

    # the bitsets for select
    my ($rin, $rout, $win, $wout);
    my $nfound;

    # the big select loop
    while(1) {
        if ($active_changed) {
            last unless %reading or %writing; # no sockets left?
            ($rin, $win) = ('', '');
            foreach (keys %reading) {
                vec($rin, fileno($_), 1) = 1;
            }
            foreach (keys %writing) {
                vec($win, fileno($_), 1) = 1;
            }
            $active_changed = 0;
        }
        # TODO: more intelligent cumulative timeout?
        $nfound = select($rout=$rin, $wout=$win, undef,
                         $self->{'select_timeout'});
        last unless $nfound;

        # TODO: possible robustness improvement: we could select
        # writing sockets for reading also, and raise hell if they're
        # ready (input unread from last time, etc.)
        # maybe do that on the first loop only?
        foreach (keys %writing) {
            if (vec($wout, fileno($_), 1)) {
                $write->($_);
            }
        }
        foreach (keys %reading) {
            if (vec($rout, fileno($_), 1)) {
                $read->($_);
            }
        }
    }

    # if there're active sockets left, they need to die
    foreach (keys %writing) {
        $dead->($_);
    }
    foreach (keys %reading) {
        $dead->($_);
    }

    return;
}

sub _hashfunc {
    return (crc32(shift) >> 16) & 0x7fff;
}

sub flush_all {
    my Cache::Memcached $self = shift;

    my $success = 1;

    $self->init_buckets() unless $self->{'buckets'};
    my @hosts = @{$self->{'buckets'}};
    foreach my $host (@hosts) {
        my $sock = $self->sock_to_host($host);
        my @res = $self->run_command($sock, "flush_all\r\n");
        $success = 0 unless (@res);
    }

    return $success;
}

# returns array of lines, or () on failure.
sub run_command {
    my Cache::Memcached $self = shift;
    my ($sock, $cmd) = @_;
    return () unless $sock;
    my $ret;
    my $line = $cmd;
    while (my $res = _write_and_read($self, $sock, $line)) {
        undef $line;
    $ret .= $res;
        last if $ret =~ /(?:END|ERROR)\r\n$/;
    }
    chop $ret; chop $ret;
    return map { "$_\r\n" } split(/\r\n/, $ret);
}

sub stats {
    my Cache::Memcached $self = shift;
    my ($types) = @_;
    return 0 unless $self->{'active'};
    return 0 unless !ref($types) || ref($types) eq 'ARRAY';
    if (!ref($types)) {
        if (!$types) {
            # I don't much care what the default is, it should just
            # be something reasonable.  Obviously "reset" should not
            # be on the list :) but other types that might go in here
            # include maps, cachedump, slabs, or items.
            $types = [ qw( misc malloc sizes self ) ];
        } else {
            $types = [ $types ];
        }
    }

    $self->init_buckets() unless $self->{'buckets'};

    my $stats_hr = { };

    # The "self" stat type is special, it only applies to this very
    # object.
    if (grep /^self$/, @$types) {
        $stats_hr->{'self'} = \%{ $self->{'stats'} };
    }

    # Now handle the other types, passing each type to each host server.
    my @hosts = @{$self->{'buckets'}};
    my %malloc_keys = ( );
  HOST: foreach my $host (@hosts) {
        my $sock = $self->sock_to_host($host);
      TYPE: foreach my $typename (grep !/^self$/, @$types) {
            my $type = $typename eq 'misc' ? "" : " $typename";
            my $lines = _write_and_read($self, $sock, "stats$type\r\n", sub {
                my $bref = shift;
                return $$bref =~ /^(?:END|ERROR)\r?\n/m;
            });
            unless ($lines) {
                _dead_sock($sock);
                next HOST;
            }

            $lines =~ s/\0//g;  # 'stats sizes' starts with NULL?

            # And, most lines end in \r\n but 'stats maps' (as of
            # July 2003 at least) ends in \n. ??
            my @lines = split(/\r?\n/, $lines);

            # Some stats are key-value, some are not.  malloc,
            # sizes, and the empty string are key-value.
            # ("self" was handled separately above.)
            if ($typename =~ /^(malloc|sizes|misc)$/) {
                # This stat is key-value.
                foreach my $line (@lines) {
                    my($key, $value) = $line =~ /^(?:STAT )?(\w+)\s(.*)/;
                    if ($key) {
                        $stats_hr->{'hosts'}{$host}{$typename}{$key} = $value;
                    }
                    $malloc_keys{$key} = 1 if $typename eq 'malloc';
                }
            } else {
                # This stat is not key-value so just pull it
                # all out in one blob.
                $lines =~ s/^END\r?\n//m;
                $stats_hr->{'hosts'}{$host}{$typename} ||= "";
                $stats_hr->{'hosts'}{$host}{$typename} .= "$lines";
            }
        }
    }

    # Now get the sum total of applicable values.  First the misc values.
    foreach my $stat (qw(
        bytes bytes_read bytes_written
        cmd_get cmd_set connection_structures curr_items
        get_hits get_misses
        total_connections total_items
        )) {
        $stats_hr->{'total'}{$stat} = 0;
        foreach my $host (@hosts) {
            $stats_hr->{'total'}{$stat} +=
                $stats_hr->{'hosts'}{$host}{'misc'}{$stat};
        }
    }

    # Then all the malloc values, if any.
    foreach my $malloc_stat (keys %malloc_keys) {
        $stats_hr->{'total'}{"malloc_$malloc_stat"} = 0;
        foreach my $host (@hosts) {
            $stats_hr->{'total'}{"malloc_$malloc_stat"} +=
                $stats_hr->{'hosts'}{$host}{'malloc'}{$malloc_stat};
        }
    }

    return $stats_hr;
}

sub stats_reset {
    my Cache::Memcached $self = shift;
    my ($types) = @_;
    return 0 unless $self->{'active'};

    $self->init_buckets() unless $self->{'buckets'};

  HOST: foreach my $host (@{$self->{'buckets'}}) {
        my $sock = $self->sock_to_host($host);
        my $ok = _write_and_read($self, $sock, "stats reset");
        unless ($ok eq "RESET\r\n") {
            _dead_sock($sock);
        }
    }
    return 1;
}



1;
__END__

=head1 NAME

Cache::Memcached - client library for memcached (memory cache daemon)

=head1 SYNOPSIS

  use Cache::Memcached;

  $memd = new Cache::Memcached {
    'servers' => [ "10.0.0.15:11211", "10.0.0.15:11212",
                   "10.0.0.17:11211", [ "10.0.0.17:11211", 3 ] ],
    'debug' => 0,
    'compress_threshold' => 10_000,
  };
  $memd->set_servers($array_ref);
  $memd->set_compress_threshold(10_000);
  $memd->enable_compress(0);

  $memd->set("my_key", "Some value");
  $memd->set("object_key", { 'complex' => [ "object", 2, 4 ]});

  $val = $memd->get("my_key");
  $val = $memd->get("object_key");
  if ($val) { print $val->{'complex'}->[2]; }

  $memd->incr("key");
  $memd->decr("key");
  $memd->incr("key", 2);

=head1 DESCRIPTION

This is the Perl API for memcached, a distributed memory cache daemon.
More information is available at:

  http://www.danga.com/memcached/

=head1 CONSTRUCTOR

=over 4

=item C<new>

Takes one parameter, a hashref of options.  The most important key is
C<servers>, but that can also be set later with the C<set_servers>
method.  The servers must be an arrayref of hosts, each of which is
either a scalar of the form C<10.0.0.10:11211> or an arrayref of the
former and an integer weight value.  (The default weight if
unspecified is 1.)  It's recommended that weight values be kept as low
as possible, as this module currently allocates memory for bucket
distribution proportional to the total host weights.

Use C<compress_threshold> to set a compression threshold, in bytes.
Values larger than this threshold will be compressed by C<set> and
decompressed by C<get>.

Use C<no_rehash> to disable finding a new memcached server when one
goes down.  Your application may or may not need this, depending on
your expirations and key usage.

Use C<readonly> to disable writes to backend memcached servers.  Only
get and get_multi will work.  This is useful in bizarre debug and
profiling cases only.

Use C<namespace> to prefix all keys with the provided namespace value.
That is, if you set namespace to "app1:" and later do a set of "foo"
to "bar", memcached is actually seeing you set "app1:foo" to "bar".

The other useful key is C<debug>, which when set to true will produce
diagnostics on STDERR.

=back

=head1 METHODS

=over 4

=item C<set_servers>

Sets the server list this module distributes key gets and sets between.
The format is an arrayref of identical form as described in the C<new>
constructor.

=item C<set_debug>

Sets the C<debug> flag.  See C<new> constructor for more information.

=item C<set_readonly>

Sets the C<readonly> flag.  See C<new> constructor for more information.

=item C<set_norehash>

Sets the C<no_rehash> flag.  See C<new> constructor for more information.

=item C<set_compress_threshold>

Sets the compression threshold. See C<new> constructor for more information.

=item C<enable_compress>

Temporarily enable or disable compression.  Has no effect if C<compress_threshold>
isn't set, but has an overriding effect if it is.

=item C<get>

my $val = $memd->get($key);

Retrieves a key from the memcache.  Returns the value (automatically
thawed with Storable, if necessary) or undef.

The $key can optionally be an arrayref, with the first element being the
hash value, if you want to avoid making this module calculate a hash
value.  You may prefer, for example, to keep all of a given user's
objects on the same memcache server, so you could use the user's
unique id as the hash value.

=item C<get_multi>

my $hashref = $memd->get_multi(@keys);

Retrieves multiple keys from the memcache doing just one query.
Returns a hashref of key/value pairs that were available.

This method is recommended over regular 'get' as it lowers the number
of total packets flying around your network, reducing total latency,
since your app doesn't have to wait for each round-trip of 'get'
before sending the next one.

=item C<set>

$memd->set($key, $value[, $exptime]);

Unconditionally sets a key to a given value in the memcache.  Returns true
if it was stored successfully.

The $key can optionally be an arrayref, with the first element being the
hash value, as described above.

The $exptime (expiration time) defaults to "never" if unspecified.  If
you want the key to expire in memcached, pass an integer $exptime.  If
value is less than 60*60*24*30 (30 days), time is assumed to be relative
from the present.  If larger, it's considered an absolute Unix time.

=item C<add>

$memd->add($key, $value[, $exptime]);

Like C<set>, but only stores in memcache if the key doesn't already exist.

=item C<replace>

$memd->replace($key, $value[, $exptime]);

Like C<set>, but only stores in memcache if the key already exists.  The
opposite of C<add>.

=item C<delete>

$memd->delete($key[, $time]);

Deletes a key.  You may optionally provide an integer time value (in seconds) to
tell the memcached server to block new writes to this key for that many seconds.
(Sometimes useful as a hacky means to prevent races.)  Returns true if key
was found and deleted, and false otherwise.

=item C<incr>

$memd->incr($key[, $value]);

Sends a command to the server to atomically increment the value for
$key by $value, or by 1 if $value is undefined.  Returns undef if $key
doesn't exist on server, otherwise it returns the new value after
incrementing.  Value should be zero or greater.  Overflow on server
is not checked.  Be aware of values approaching 2**32.  See decr.

=item C<decr>

$memd->decr($key[, $value]);

Like incr, but decrements.  Unlike incr, underflow is checked and new
values are capped at 0.  If server value is 1, a decrement of 2
returns 0, not -1.

=item C<stats>

$memd->stats([$keys]);

Returns a hashref of statistical data regarding the memcache server(s),
the $memd object, or both.  $keys can be an arrayref of keys wanted, a
single key wanted, or absent (in which case the default value is malloc,
sizes, self, and the empty string).  These keys are the values passed
to the 'stats' command issued to the memcached server(s), except for
'self' which is internal to the $memd object.  Allowed values are:

=over 4

=item C<misc>

The stats returned by a 'stats' command:  pid, uptime, version,
bytes, get_hits, etc.

=item C<malloc>

The stats returned by a 'stats malloc':  total_alloc, arena_size, etc.

=item C<sizes>

The stats returned by a 'stats sizes'.

=item C<self>

The stats for the $memd object itself (a copy of $memd->{'stats'}).

=item C<maps>

The stats returned by a 'stats maps'.

=item C<cachedump>

The stats returned by a 'stats cachedump'.

=item C<slabs>

The stats returned by a 'stats slabs'.

=item C<items>

The stats returned by a 'stats items'.

=back

=item C<disconnect_all>

$memd->disconnect_all;

Closes all cached sockets to all memcached servers.  You must do this
if your program forks and the parent has used this module at all.
Otherwise the children will try to use cached sockets and they'll fight
(as children do) and garble the client/server protocol.

=item C<flush_all>

$memd->flush_all;

Runs the memcached "flush_all" command on all configured hosts,
emptying all their caches.  (or rather, invalidating all items
in the caches in an O(1) operation...)  Running stats will still
show the item existing, they're just be non-existent and lazily
destroyed next time you try to detch any of them.

=back

=head1 BUGS

When a server goes down, this module does detect it, and re-hashes the
request to the remaining servers, but the way it does it isn't very
clean.  The result may be that it gives up during its rehashing and
refuses to get/set something it could've, had it been done right.

=head1 COPYRIGHT

This module is Copyright (c) 2003 Brad Fitzpatrick.
All rights reserved.

You may distribute under the terms of either the GNU General Public
License or the Artistic License, as specified in the Perl README file.

=head1 WARRANTY

This is free software. IT COMES WITHOUT WARRANTY OF ANY KIND.

=head1 FAQ

See the memcached website:
   http://www.danga.com/memcached/

=head1 AUTHORS

Brad Fitzpatrick <brad@danga.com>

Anatoly Vorobey <mellon@pobox.com>

Brad Whitaker <whitaker@danga.com>

Jamie McCarthy <jamie@mccarthy.vg>
