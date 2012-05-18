#!/usr/bin/env perl -w

use strict;
use Test::More;
use Cache::Memcached;
use IO::Socket::INET;
use Time::HiRes qw(gettimeofday tv_interval);

my $testaddr = "192.0.2.1:11211";

my $stime = [gettimeofday];

my $msock = IO::Socket::INET->new(
    PeerAddr => $testaddr,
    Timeout => 2,
);

my $delta_t = tv_interval($stime);

if ($delta_t >= 1) {
    plan tests => 2;
} else {
    plan skip_all => "Somehow we got a fast return when connecting to $testaddr\n";
    exit 0;
}

my $memd = Cache::Memcached->new({
    servers   => [ $testaddr ],
    namespace => "Cache::Memcached::t/$$/" . (time() % 100) . "/",
});


my $time1 = Time::HiRes::time();
$memd->set("key", "bar");
my $time2 = Time::HiRes::time();
# 100ms is faster than the default connect timeout.
ok($time2 - $time1 > .1, "Expected pause while connecting");

# 100ms should be slow enough that dead socket reconnects happen faster than it.
$memd->set("key", "foo");
my $time3 = Time::HiRes::time();
ok($time3 - $time2 < .1, "Should return fast on retry");
