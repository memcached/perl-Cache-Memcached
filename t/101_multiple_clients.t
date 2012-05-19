#!/usr/bin/env perl -w

use strict;
use Test::More;
use Cache::Memcached;
use IO::Socket::INET;

my $testaddr = "127.0.0.1:11211";
my $sock = IO::Socket::INET->new(
    PeerAddr => $testaddr,
    Proto     => 'tcp',
    ReuseAddr => 1,
);

if ($sock) {
    plan tests => 8;
} else {
    plan skip_all => "cannot connect to $testaddr\n";
    exit 0;
}
close $sock;

my $mc = Cache::Memcached->new(
    servers => [ '127.0.0.1:11211', '127.1:11211' ],
);

$mc->get('1');
pass("1-1");

$mc->get('2');
pass("1-2");

my $mc2 = Cache::Memcached->new(
    servers => [ '127.0.0.1:11211', '127.1:11211' ],
);

$mc2->get('1');
pass("2-1");

$mc->get('1');
pass("2-2");

$mc->disconnect_all();

$mc2->get('1');
pass("2-1");

$mc->get('1');
pass("2-2");

$mc->get('2');
pass("2-3");

$mc2->get('2');
pass("2-4");
