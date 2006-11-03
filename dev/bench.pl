#!/usr/bin/perl

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

my $keys = 800;

my $memd = Cache::Memcached->new({
#    servers   => [ $testaddr, $testaddr ],
    servers   => [ $testaddr ],
    namespace => "Cache::Memcached::t/$$/" . (time() % 100) . "/",
});

my %correct;
for my $num (1..$keys) {
    $correct{"key$num"} = "key$num " . ("-" x ($num * 50));
    $memd->set("key$num", $correct{"key$num"})
        or die "Failed to init $num";
}

srand(1);
my $to = shift || 3000;
for (1..$to) {
    warn "$_ / $to\n" if $_ % 100 == 0;
    my @multi = map { "key$_" } map { int(rand($keys * 2)) + 1 } (1..40);
    my $get = $memd->get_multi(@multi);
    #use Data::Dumper;
    #print Dumper(\@multi, $get);
    for (0..4) {  # was 4
        my $k = $multi[$_];
        die "no match for '$k': $get->{$k} vs $correct{$k}" unless $get->{$k} eq $correct{$k};
    }
}


__END__

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



