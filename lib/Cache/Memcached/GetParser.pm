package Cache::Memcached::GetParser;
use strict;
use warnings;
use integer;

use Errno qw( EINPROGRESS EWOULDBLOCK EISCONN );

use constant DEST    => 0;  # destination hashref we're writing into
use constant NSLEN   => 1;  # length of namespace to ignore on keys
use constant ON_ITEM => 2;
use constant BUF     => 3;  # read buffer
use constant STATE   => 4;  # 0 = waiting for a line, N = reading N bytes
use constant OFFSET  => 5;  # offsets to read into buffers
use constant FLAGS   => 6;
use constant KEY     => 7;  # current key we're parsing (without the namespace prefix)

sub new {
    my ($class, $dest, $nslen, $on_item) = @_;
    return bless [$dest, $nslen, $on_item, '', 0, 0], $class;
}

sub current_key {
    return $_[0][KEY];
}

# returns 1 on success, -1 on failure, and 0 if still working.
sub parse_from_sock {
    my ($self, $sock) = @_;
    my $res;

    # where are we reading into?
    if ($self->[STATE]) { # reading value into $ret
        my $ret = $self->[DEST];
        $res = sysread($sock, $ret->{$self->[KEY]},
                       $self->[STATE] - $self->[OFFSET],
                       $self->[OFFSET]);

        return 0
            if !defined($res) and $!==EWOULDBLOCK;

        if ($res == 0) { # catches 0=conn closed or undef=error
            $self->[ON_ITEM] = undef;
            return -1;
        }

        $self->[OFFSET] += $res;
        if ($self->[OFFSET] == $self->[STATE]) { # finished reading
            $self->[ON_ITEM]->($self->[KEY], $self->[FLAGS]);
            $self->[OFFSET] = 0;
            $self->[STATE]  = 0;
            # wait for another VALUE line or END...
        }
        return 0; # still working, haven't got to end yet
    }

    # we're reading a single line.
    # first, read whatever's there, but be satisfied with 2048 bytes
    $res = sysread($sock, $self->[BUF],
                   128*1024, $self->[OFFSET]);
    return 0
        if !defined($res) and $!==EWOULDBLOCK;
    if ($res == 0) {
        $self->[ON_ITEM] = undef;
        return -1;
    }

    $self->[OFFSET] += $res;

    return $self->parse_buffer;
}

# returns 1 on success, -1 on failure, and 0 if still working.
sub parse_buffer {
    my ($self) = @_;
    my $ret = $self->[DEST];

  SEARCH:
    while (1) { # may have to search many times

        # do we have a complete END line?
        if ($self->[BUF] =~ /^END\r\n/) {
            $self->[ON_ITEM] = undef;
            return 1;  # we're done successfully, return 1 to finish
        }

        # do we have a complete VALUE line?
        if ($self->[BUF] =~ /^VALUE (\S+) (\d+) (\d+)\r\n/) {
            ($self->[KEY], $self->[FLAGS], $self->[STATE]) =
                (substr($1, $self->[NSLEN]), int($2), $3+2);
            # Note: we use $+[0] and not pos($self->[BUF]) because pos()
            # seems to have problems under perl's taint mode.  nobody
            # on the list discovered why, but this seems a reasonable
            # work-around:
            my $p = $+[0];
            my $len = length($self->[BUF]);
            my $copy = $len-$p > $self->[STATE] ? $self->[STATE] : $len-$p;
            $ret->{$self->[KEY]} = substr($self->[BUF], $p, $copy)
                if $copy;
            $self->[OFFSET] = $copy;
            substr($self->[BUF], 0, $p+$copy, ''); # delete the stuff we used

            if ($self->[OFFSET] == $self->[STATE]) { # have it all?
                $self->[ON_ITEM]->($self->[KEY], $self->[FLAGS]);
                $self->[OFFSET] = 0;
                $self->[STATE]  = 0;
                next SEARCH; # look again
            }

            last SEARCH; # buffer is empty now
        }

        # if we're here probably means we only have a partial VALUE
        # or END line in the buffer. Could happen with multi-get,
        # though probably very rarely. Exit the loop and let it read
        # more.

        # but first, make sure subsequent reads don't destroy our
        # partial VALUE/END line.
        $self->[OFFSET] = length($self->[BUF]);
        last SEARCH;
    }
    return 0;
}

1;
