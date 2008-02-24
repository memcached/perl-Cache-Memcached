#! /usr/bin/perl
#
use warnings;
use strict;

# Note: you may have to set PERL5LIB to point to the module.
use Cache::Memcached;

use FindBin;

@ARGV == 1 or @ARGV == 2
    or die "Usage: $FindBin::Script HOST:PORT [COUNT]\n";

# Note that it's better to run the test over the wire, because for
# localhost the task may become CPU bound.
my $addr = $ARGV[0];
my $count = $ARGV[1] || 10_000;

my $memd = Cache::Memcached->new({
    servers   => [ $addr ],
    namespace => ''
});

die "$!\n" unless $memd;


# By running 'noreply' test first we also ensure there are no reply
# packets left in the network.
foreach my $noreply (1, 0) {
    use Time::HiRes qw(gettimeofday tv_interval);

    print "'noreply' is ", $noreply ? "enabled" : "disabled", ":\n";
    my $param = $noreply ? 'noreply' : '';
    my $res;

    my $start = [gettimeofday];
    if ($noreply) {
        foreach (1 .. $count) {
            $memd->add("foo", 1);
            $memd->set("foo", 1);
            $memd->replace("foo", 1);
            $memd->incr("foo", 1);
            $memd->decr("foo", 1);
            $memd->delete("foo");
        }
    } else {
        foreach (1 .. $count) {
            $res = $memd->add("foo", 1);
            $res = $memd->set("foo", 1);
            $res = $memd->replace("foo", 1);
            $res = $memd->incr("foo", 1);
            $res = $memd->decr("foo", 1);
            $res = $memd->delete("foo");
        }
    }
    my $end = [gettimeofday];
    printf("update methods: %.2f secs\n\n", tv_interval($start, $end));
}
