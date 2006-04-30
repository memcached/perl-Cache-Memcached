use strict;
use Test::More;
use Cache::Memcached;
use IO::Socket::INET;

my $testaddr = "127.0.0.1:11211";
my $msock = IO::Socket::INET->new(PeerAddr => $testaddr,
                                  Timeout  => 3);
if ($msock) {
    plan tests => 9;
} else {
    plan skip_all => "No memcached instance running at $testaddr\n";
    exit 0;
}

my $memd = Cache::Memcached->new({
    servers   => [ $testaddr ],
    namespace => "Cache::Memcached::t/$$/" . (time() % 100) . "/",
});

ok($memd->set("key1", "val1"), "set succeeded");
is($memd->get("key1"), "val1", "get worked");
ok(! $memd->add("key1", "val-replace"), "add properly failed");
ok($memd->add("key2", "val2"), "add worked on key2");
is($memd->get("key2"), "val2", "get worked");

ok($memd->replace("key2", "val-replace"), "replace worked");
ok(! $memd->replace("key-noexist", "bogus"), "replace failed");

my $stats = $memd->stats;
ok($stats, "got stats");
is(ref $stats, "HASH", "is a hashref");


