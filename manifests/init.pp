class mccouchdb {

  package { 'couchrest':
    ensure   => installed,
    provider => 'gem',
  }

  mcollective::plugins::plugin { 'couchdb':
    ddl           => false,
    type          => 'agent',
    module_source => 'puppet:///modules/mccouchdb',
    require       => Package['couchrest'],
  }
}