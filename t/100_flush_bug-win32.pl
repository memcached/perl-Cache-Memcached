
use strict;
use IO::Socket::INET;

my @res = (
    ["OK\r\n", 1],
    ["ERROR\r\n", 0],
    ["\r\nERROR\r\n", 0],
    ["FOO\r\nERROR\r\n", 0],
    ["FOO\r\nOK\r\nERROR\r\n", 0],
    ["\r\n\r\nOK\r\n", 0],
    ["END\r\n", 0],
);


my $testaddr = shift || die;
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
