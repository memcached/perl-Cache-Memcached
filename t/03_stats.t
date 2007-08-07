#!/usr/bin/env perl -w

use strict;
use Test::More;
use Cache::Memcached;
use IO::Socket::INET;

my $testaddr = "127.0.0.1:11211";
my $msock = IO::Socket::INET->new(PeerAddr => $testaddr,
                                  Timeout  => 3);

my @misc_stats_keys = qw/ bytes bytes_read bytes_written
                          cmd_get cmd_set connection_structures curr_items
                          get_hits get_misses
                          total_connections total_items
                         /;

if ($msock) {
    plan tests => 24 + scalar(@misc_stats_keys);
} else {
    plan skip_all => "No memcached instance running at $testaddr\n";
    exit 0;
}

my $memd = Cache::Memcached->new({
    servers   => [ $testaddr ],
    namespace => "Cache::Memcached::t/$$/" . (time() % 100) . "/",
});

my $misc_stats = $memd->stats('misc');
ok($misc_stats, 'got misc stats');
isa_ok($misc_stats, 'HASH', 'misc stats');
isa_ok($misc_stats->{'total'}, 'HASH', 'misc stats total');
isa_ok($misc_stats->{'hosts'}, 'HASH', 'misc stats hosts');
isa_ok($misc_stats->{'hosts'}{$testaddr}, 'HASH',
       "misc stats hosts $testaddr");

foreach my $stat_key (@misc_stats_keys) {
    ok(exists $misc_stats->{'total'}{$stat_key},
       "misc stats total contains $stat_key");
    ok(exists $misc_stats->{'hosts'}{$testaddr}{'misc'}{$stat_key},
       "misc stats hosts $testaddr misc contains $stat_key");
}

my $got_malloc = 0;
foreach my $stat_key (keys %{$misc_stats->{'total'}}) {
    if ($stat_key =~ /^malloc/) {
        $got_malloc = 1;
        last;
    }
}
ok(! $got_malloc, 'no malloc stats in misc stats');

my $malloc_stats = $memd->stats('malloc');
ok($malloc_stats, 'got malloc stats');
isa_ok($malloc_stats, 'HASH', 'malloc stats');
isa_ok($malloc_stats->{'total'}, 'HASH', 'malloc stats total');
isa_ok($misc_stats->{'hosts'}, 'HASH', 'malloc stats hosts');
isa_ok($misc_stats->{'hosts'}{$testaddr}, 'HASH',
       "malloc stats host $testaddr");

$got_malloc = 0;
foreach my $stat_key (keys %{$malloc_stats->{'total'}}) {
    if ($stat_key =~ /^malloc/) {
        $got_malloc = 1;
        last;
    }
}
ok($got_malloc, 'malloc stats in malloc stats');

my $got_misc = 0;
foreach my $stat_key (@misc_stats_keys) {
    if (exists $malloc_stats->{'total'}{$stat_key}) {
        $got_misc = 1;
        last;
    }
}
ok(! $got_misc, 'no misc stats in malloc stats');
