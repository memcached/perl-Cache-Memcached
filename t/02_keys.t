#!/usr/bin/env perl -w

use strict;
use Test::More;
use Cache::Memcached;
use IO::Socket::INET;

my $testaddr = "127.0.0.1:11211";
my $msock = IO::Socket::INET->new(PeerAddr => $testaddr,
                                  Timeout  => 3);
if ($msock) {
    plan tests => 13;
} else {
    plan skip_all => "No memcached instance running at $testaddr\n";
    exit 0;
}

my $memd = Cache::Memcached->new({
    servers   => [ $testaddr ],
    namespace => "Cache::Memcached::t/$$/" . (time() % 100) . "/",
});

isa_ok($memd, 'Cache::Memcached');

ok($memd->set("key1", "val1"), "set key1 as val1");

is($memd->get("key1"), "val1", "get key1 is val1");
ok(! $memd->add("key1", "val-replace"), "add key1 properly failed");
ok($memd->add("key2", "val2"), "add key2 as val2");
is($memd->get("key2"), "val2", "get key2 is val2");

ok($memd->replace("key2", "val-replace"), "replace key2 as val-replace");
is($memd->get("key2"), "val-replace", "get key2 is val-replace");
ok(! $memd->replace("key-noexist", "bogus"), "replace key-noexist properly failed");

ok($memd->delete("key1"), "delete key1");
ok(! $memd->get("key1"), "get key1 properly failed");


# also test creating the object with a list rather than a hash-ref
my $mem2 = Cache::Memcached->new(
                                 servers   => [ ],
                                 debug     => 1,
                                );
isa_ok($mem2, 'Cache::Memcached');
ok($mem2->{debug}, "debug is set on alt constructed instance");
