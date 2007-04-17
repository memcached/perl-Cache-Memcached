#!/usr/bin/perl

use strict;
use Digest::SHA1 qw(sha1);
use String::CRC32 qw(crc32);;
use Data::Dumper;

my $set = Set::ConsistentHash->new;
$set->modify_targets(
                     A => 1,
                     B => 1,
                     C => 2,
                     );

my $set2 = Set::ConsistentHash->new;
$set2->modify_targets(
                      A => 1,
                      B => 1,
                      C => 1,
                      );

#print Dumper($set->bucket_counts);
#print Dumper($set2->bucket_counts);


if (0) {
    my %matched;
    my $total_trials = 100_000;
    for my $n (1..$total_trials) {
        my $rand = crc32("trial$n");
        my $server = $set->target_of_point($rand);
        #print "matched $rand = $server\n";
        $matched{$server}++;
    }

    foreach my $s ($set->targets) {
        printf("$s: expected=%0.02f%%  actual=%0.02f%%\n", #  space=%0.02f%%\n",
               $set->weight_percentage($s),
               100 * $matched{$s} / $total_trials,
               #($space{$s} / 2**32) * 100,
               );
    }
}

if (1) {
    my $total_trials = 100_000;
    my %tran;
    for my $n (1..$total_trials) {
        my $rand = crc32("trial$n");
        #my $s1 = $set->target_of_point($rand);
        #my $s2 = $set2->target_of_point($rand);

        my $s1 = $set->target_of_bucket($rand);
        my $s2 = $set2->target_of_bucket($rand);
        $tran{"$s1-$s2"}++;
        $tran{"$s1-"}++;
        $tran{"-$s2"}++;
    }

    print Dumper(\%tran);
}


############################################################################

package Set::ConsistentHash;
use strict;
use Digest::SHA1 qw(sha1);

# creates a new consistent hashing set with no targets.  you'll need to add targets.
sub new {
    my ($class) = @_;
    return bless {
        weights => {},  # $target => integer $weight
        points  => {},  # 32-bit value points on 'circle' => \$target
        order   => [],  # 32-bit points, sorted
        buckets      => undef, # when requested, arrayref of 1024 buckets mapping to targets
        total_weight => undef, # when requested, total weight of all targets
    }, $class;
}

# returns sorted list of all configured $targets
sub targets {
    my $self = shift;
    return sort keys %{$self->{weights}};
}


# returns sum of all target's weight
sub total_weight {
    my $self = shift;
    return $self->{total_weight} if defined $self->{total_weight};
    my $sum = 0;
    foreach my $val (values %{$self->{weights}}) {
        $sum += $val;
    }
    return $self->{total_weight} = $sum;
}

# returns the configured weight percentage [0,100] of a target.
sub weight_percentage {
    my ($self, $target) = @_;
    return 0 unless $self->{weights}{$target};
    return 100 * $self->{weights}{$target} / $self->total_weight;
}

# remove all targets
sub reset_targets {
    my $self = shift;
    $self->modify_targets(map { $_ => 0 } $self->targets);
}

# add/modify targets.  parameters are %weights:  $target -> $weight
sub modify_targets {
    my ($self, %weights) = @_;

    # uncache stuff:
    $self->{total_weight} = undef;
    $self->{buckets}      = undef;

    while (my ($target, $weight) = each %weights) {
        if ($weight) {
            $self->{weights}{$target} = $weight;
        } else {
            delete $self->{weight}{$target};
        }
    }
    $self->_redo_circle;
}
*modify_target = \&modify_targets;

sub _redo_circle {
    my $self = shift;

    my $pts = $self->{points} = {};
    while (my ($target, $weight) = each %{$self->{weights}}) {
        my $num_pts = $weight * 100;
        foreach my $ptn (1..$num_pts) {
            my $key = "$target-$ptn";
            my $val = unpack("L", substr(sha1($key), 0, 4));
            $pts->{$val} = \$target;
        }
    }

    $self->{order} = [ sort { $a <=> $b } keys %$pts ];
}

# returns arrayref of 1024 buckets.  each array element is the $target for that bucket index.
sub buckets {
    my $self = shift;
    return $self->{buckets} if $self->{buckets};
    my $buckets = $self->{buckets} = [];
    my $by = 2**22;  # 2**32 / 2**10 (1024)
    for my $n (0..1023) {
        my $pt = $n * $by;
        $buckets->[$n] = $self->target_of_point($pt);
    }

    return $buckets;
}

# returns hashref of $target -> $number of occurences in 1024 buckets
sub bucket_counts {
    my $self = shift;
    my $ct = {};
    foreach my $t (@{ $self->buckets }) {
        $ct->{$t}++;
    }
    return $ct;
}

# given an integer, returns $target (after modding on 1024 buckets)
sub target_of_bucket {
    my ($self, $bucketpos) = @_;
    return ($self->{buckets} || $self->buckets)->[$bucketpos % 1024];
}

# given a $point [0,2**32), returns the $target that's next going around the circle
sub target_of_point {
    my ($self, $pt) = @_;  # $pt is 32-bit unsigned integer

    my $order = $self->{order};
    my $circle_pt = $self->{points};

    my ($lo, $hi) = (0, scalar(@$order)-1);  # inclusive candidates

    while (1) {
        my $mid           = int(($lo + $hi) / 2);
        my $val_at_mid    = $order->[$mid];
        my $val_one_below = $mid ? $order->[$mid-1] : 0;

        # match
        return ${ $circle_pt->{$order->[$mid]} } if
            $pt <= $val_at_mid && $pt > $val_one_below;

        # wrap-around match
        return ${ $circle_pt->{$order->[0]} } if
            $lo == $hi;

        # too low, go up.
        if ($val_at_mid < $pt) {
            $lo = $mid + 1;
            $lo = $hi if $lo > $hi;
        }
        # too high
        else {
            $hi = $mid - 1;
            $hi = $lo if $hi < $lo;
        }

        next;
    }
};
