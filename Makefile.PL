use ExtUtils::MakeMaker;
WriteMakefile( 'NAME' => 'MemCachedClient',
               'VERSION_FROM' => 'MemCachedClient.pm',
               'PREREQ_PM'		=> {
                   'Storable' => 0,
               },
               ($] >= 5.005 ?
                (ABSTRACT_FROM => 'MemCachedClient.pm',
                 AUTHOR     => 'Brad Fitzpatrick <brad@danga.com>') : ()),
               );

