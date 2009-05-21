#!/usr/bin/env perl -w

use strict;
use Test::More;
use Cache::Memcached;
use IO::Socket::INET;

my $port = 11311;
my $testaddr = "127.0.0.1:$port";
my $sock = IO::Socket::INET->new(
    LocalAddr => $testaddr,
    Proto     => 'tcp',
    ReuseAddr => 1,
);

my @res = (
    ["OK\r\n", 1],
    ["ERROR\r\n", 0],
    ["\r\nERROR\r\n", 0],
    ["FOO\r\nERROR\r\n", 0],
    ["FOO\r\nOK\r\nERROR\r\n", 0],
    ["\r\n\r\nOK\r\n", 0],
    ["END\r\n", 0],
);

if ($sock) {
    plan tests => scalar @res;
} else {
    plan skip_all => "cannot bind to $testaddr\n";
    exit 0;
}
close $sock;

my $pid = fork;
die "Cannot fork because: '$!'" unless defined $pid;
unless ($pid) {
    
    my $sock = IO::Socket::INET->new(
        LocalAddr  => $testaddr,
        Proto      => 'tcp',
        ReuseAddr  => 1,
        Listen     => 1,
    ) or die "cannot open $testaddr: $!";
    my $csock = $sock->accept();
    while (defined (my $buf = <$csock>)) {
        my $res = shift @res;
        print $csock $res->[0];
    }
    close $csock;
    close $sock;
    exit 0;
}

# give the forked server a chance to startup
sleep 1;

my $memd = Cache::Memcached->new({ servers   => [ $testaddr ] });

for (@res) {
    ($_->[0] =~ s/\W//g);
    is $memd->flush_all, $_->[1], $_->[0];
}
