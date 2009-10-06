use ExtUtils::MakeMaker;
WriteMakefile( 'NAME' => 'Cache::Memcached',
               'VERSION_FROM' => 'lib/Cache/Memcached.pm',
               'PREREQ_PM'              => {
                   'Storable' => 0,
                   'Time::HiRes' => 0,
                   'String::CRC32' => 0,
                   'Encode'        => 0, 
               },
               ($] >= 5.005 ?
                (ABSTRACT_FROM => 'lib/Cache/Memcached.pm',
                 AUTHOR     => 'Brad Fitzpatrick <brad@danga.com>') : ()),
               );

