# $Id$
#
# Copyright (c) 2003  Brad Fitzpatrick <brad@danga.com>
#
# See COPYRIGHT section in pod text below for usage and distribution rights.
#

use strict;
use IO::Socket::INET;
use Storable ();

package MemCachedClient;

use vars qw($VERSION);
$VERSION = "1.0.3";

my %host_dead;   # host -> unixtime marked dead until
my %cache_sock;  # host -> socket


sub new {
    my ($class, $args) = @_;
    my $self = {};
    bless $self, ref $class || $class;

    $self->set_servers($args->{'servers'});
    $self->{'debug'} = $args->{'debug'};
    $self->{'stats'} = {};
    return $self;
}

sub set_servers {
    my ($self, $list) = @_;
    $self->{'servers'} = $list || [];
    $self->{'active'} = scalar @{$self->{'servers'}};
    $self->{'buckets'} = undef;
    $self->{'bucketcount'} = 0;
    return $self;
}

sub set_debug {
    my ($self, $dbg) = @_;
    $self->{'debug'} = $dbg;
}

sub forget_dead_hosts {
    %host_dead = ();
}

sub sock_to_host { # (host)
    my $host = shift;
    my $now = time();
    my ($ip, $port) = $host =~ /(.*):(.*)/;
    return undef if 
         $host_dead{$host} && $host_dead{$host} > $now || 
         $host_dead{$ip} && $host_dead{$ip} > $now;
    return $cache_sock{$host} if $cache_sock{$host} && $cache_sock{$host}->connected;
    my $sock = IO::Socket::INET->new(Proto => "tcp",
                                     PeerAddr => $host,
                                     Timeout => 1);
    unless ($sock) {
        $host_dead{$host} = $host_dead{$ip} = $now + 60 + int(rand(10));
        return undef;
    }
    return $cache_sock{$host} = $sock;
}

sub get_sock { # (key)
    my ($self, $key) = @_;
    return undef unless $self->{'active'};
    my $hv = ref $key eq "ARRAY" ? int($key->[0]) : _hashfunc($key);

    unless ($self->{'buckets'}) {
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

    my $tries = 0;
    while ($tries++ < 20) {
        my $host = $self->{'buckets'}->[$hv % $self->{'bucketcount'}];
        my $sock = sock_to_host($host);
        return $sock if $sock;
        $hv += _hashfunc($tries);  # stupid, but works
    }
    return undef;
}

sub disconnect_all {
    $_->close foreach (values %cache_sock);
    %cache_sock = ();
}

sub delete {
    my ($self, $key) = @_;
    return 0 unless $self->{'active'};
    my $sock = $self->get_sock($key);
    return 0 unless $sock;
    $self->{'stats'}->{"delete"}++;
    $key = ref $key eq "ARRAY" ? $key->[1] : $key;
    my $cmd = "delete $key\r\n";
    $sock->print($cmd);
    $sock->flush;
    my $line = <$sock>;
    return 1 if $line eq "DELETED\r\n";
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
    my ($cmdname, $self, $key, $val, $exptime) = @_;
    return 0 unless $self->{'active'};
    my $sock = $self->get_sock($key);
    return 0 unless $sock;
    $self->{'stats'}->{$cmdname}++;
    my $flags = 0;
    $key = ref $key eq "ARRAY" ? $key->[1] : $key;
    my $raw_val = $val;
    if (ref $val) {
        $val = Storable::freeze($val);
        $flags |= 1;
    }
    my $len = length($val);
    $exptime = int($exptime || 0);
    my $cmd = "$cmdname $key $flags $exptime $len\r\n$val\r\n";
    $sock->print($cmd);
    $sock->flush;
    my $line = <$sock>;
    if ($line eq "STORED\r\n") {
        print STDERR "MemCache: $cmdname $key = $raw_val\n" if $self->{'debug'};
        return 1;
    }
    return 0;
}

sub get {
    my ($self, $key) = @_;
    $self->{'stats'}->{"get"}++;
    $self->{'stats'}->{"get_multi"}--;
    my $val = $self->get_multi($key);
    return undef unless $val;
    return $val->{$key};
}

sub get_multi {
    my $self = shift;
    return undef unless $self->{'active'};
    $self->{'stats'}->{"get_multi"}++;
    my %val;        # what we'll be returning a reference to (realkey -> value)
    my %sock_keys;  # sockref_as_scalar -> [ realkeys ]
    my @socks;      # unique socket refs
    foreach my $key (@_) {
        my $sock = $self->get_sock($key);
        next unless $sock;
        $key = ref $key eq "ARRAY" ? $key->[1] : $key;
        unless ($sock_keys{$sock}) {
            $sock_keys{$sock} = [];
            push @socks, $sock;
        }
        push @{$sock_keys{$sock}}, $key;
    }
    $self->{'stats'}->{"get_keys"} += @_;
    $self->{'stats'}->{"get_socks"} += @socks;

    # pass 1: send out requests
    foreach my $sock (@socks) {
        my $cmd = "get @{$sock_keys{$sock}}\r\n";
        $sock->print($cmd);
        $sock->flush;
    }
    # pass 2: parse responses
    foreach my $sock (@socks) {
        _load_items($sock, \%val);
    }
    if ($self->{'debug'}) {
        while (my ($k, $v) = each %val) {
            print STDERR "MemCache: got $k = $v\n";
        }
    }
    return \%val;
}

sub _load_items {
    my $sock = shift;
    my $outref = shift;

    my %flags;
    my %val;
    my %len;   # key -> intended length

  ITEM:
    while (1) {
        my $line = $sock->getline;
        if ($line =~ /^VALUE (\S+) (\d+) (\d+)\r\n$/s) {
            my ($rk, $flags, $len) = ($1, $2, $3);
            $flags{$rk} = $flags if $flags;
            $len{$rk} = $len;
            my $bytes_read = 0;
            my $buf;
            while (defined($line = $sock->getline)) {
                $bytes_read += length($line);
                $buf .= $line;
                if ($bytes_read == $len + 2) {
                    chop $buf; chop $buf;  # kill \r\n
                    $val{$rk} = $buf;
                    next ITEM;
                }
                if ($bytes_read > $len) {
                    # invalid crap from server
                    return 0;
                }
            }
            next ITEM;
        }
        if ($line eq "END\r\n") {
            foreach (@_) {
                next unless exists $val{$_};
                next unless length($val{$_}) == $len{$_};
                $val{$_} = Storable::thaw($val{$_}) if $flags{$_} & 1;
                $outref->{$_} = $val{$_};
            }
            return 1;
        }
        if (length($line) == 0) {
            return 0;
        }
    }
}

sub _hashfunc {
    my $hash = 0;
    foreach (split //, shift) {
        $hash = $hash*33 + ord($_);
    }
    return $hash;
}

1;
__END__

=head1 NAME

MemCachedClient - client library for memcached (memory cache daemon)

=head1 SYNOPSIS

  use MemCached;

  $memc = new MemCached {
    'servers' => [ "10.0.0.15:11211", "10.0.0.15:11212", 
                   "10.0.0.17:11211", [ "10.0.0.17:11211", 3 ] ],
    'debug' => 0,
  };
  $memc->set_servers($array_ref);

  $memc->set("my_key", "Some value");
  $memc->set("object_key", { 'complex' => [ "object", 2, 4 ]});

  $val = $memc->get("my_key");
  $val = $memc->get("object_key");
  if ($val) { print $val->{'complex'}->[2]; }

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
former and an integer weight value.  (the default weight if
unspecified is 1.)  It's recommended that weight values be kept as low
as possible, as this module currently allocates memory for bucket
distribution proportional to the total host weights.

The other useful key is C<debug>, which when set to true will produce
diagnostics on STDERR.

=back

=head1 METHODS

=over 4

=item C<set_servers>

Sets the server list this module distribute key gets and sets between.
The format is an arrayref of identical form as described in the C<new>
constructor.

=item C<get>

my $val = $mem->get($key);

Retrieves a key from the memcache.  Returns the value (automatically
thawed with Storable, if necessary) or undef.

The $key can optionally be an arrayref, with the first element being the
hash value, if you want to avoid making this module calculate a hash
value.  You may prefer, for example, to keep all of a given user's
objects on the same memcache server, so you could use the user's
unique id as the hash value.

=item C<get_multi>

my $hashref = $mem->get_multi(@keys);

Retrieves multiple keys from the memcache doing just one query.
Returns a hashref of key/value pairs that were available.

This method is recommended over regular 'get' as it lowers the number
of total packets flying around your network, reducing total latency,
since your app doesn't have to wait for each round-trip of 'get'
before sending the next one.

=item C<set>

$mem->set($key, $value);

Unconditionally sets a key to a given value in the memcache.  Returns true
if it was stored successfully.

The $key can optionally be an arrayref, with the first element being the
hash value, as described above.

=item C<add>

$mem->add($key, $value);

Like C<set>, but only stores in memcache if the key doesn't already exist.

=item C<replace>

$mem->replace($key, $value);

Like C<set>, but only stores in memcache if the key already exists.  The
opposte if C<add>.

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

=head1 AUTHOR

Brad Fitzpatrick <brad@danga.com>
