use ExtUtils::MakeMaker;
WriteMakefile( 'NAME' => 'Cache::Memcached',
               'VERSION_FROM' => 'Memcached.pm',
               'PREREQ_PM'		=> {
                   'Storable' => 0,
               },
               ($] >= 5.005 ?
                (ABSTRACT_FROM => 'Memcached.pm',
                 AUTHOR     => 'Brad Fitzpatrick <brad@danga.com>') : ()),
               );

