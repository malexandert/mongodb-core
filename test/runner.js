"use strict";

require('bson')

var Runner = require('integra').Runner
  , Cover = require('integra').Cover
  , RCover = require('integra').RCover
  , f = require('util').format
  , m = require('mongodb-version-manager')
  , path = require('path')
  , NodeVersionFilter = require('./filters/node_version_filter')
  , MongoDBVersionFilter = require('./filters/mongodb_version_filter')
  , MongoDBTopologyFilter = require('./filters/mongodb_topology_filter')
  , ES6PromisesSupportedFilter = require('./filters/es6_promises_supported_filter')
  , ES6GeneratorsSupportedFilter = require('./filters/es6_generators_supported_filter')
  , TravisFilter = require('./filters/travis_filter')
  , FileFilter = require('integra').FileFilter
  , TestNameFilter = require('integra').TestNameFilter
  , Configuration = require('./test_configuration');

var detector = require('gleak')();
var smokePlugin = require('../lib/tools/smoke_plugin.js');
var argv = require('optimist')
    .usage('Usage: $0 -t [target] -e [environment] -n [name] -f [filename] -r [smoke report file]')
    .demand(['t'])
    .argv;

// MongoDB Topology Manager
var ServerManager = require('mongodb-topology-manager').Server,
  ReplSetManager = require('mongodb-topology-manager').ReplSet,
  ShardingManager = require('./test_topologies.js').Sharded;

// Skipping parameters
var startupOptions = {
    skipStartup: true
  , skipRestart: true
  , skipShutdown: true
  , skip: false
}

// Skipping parameters
var startupOptions = {
    skipStartup: false
  , skipRestart: false
  , skipShutdown: false
  , skip: false
}

// Skipping parameters
if(argv.s) {
  var startupOptions = {
      skipStartup: true
    , skipRestart: true
    , skipShutdown: true
    , skip: false
  }
}

/**
 * Standalone MongoDB Configuration
 */
var f = require('util').format;
var Logger = require('../lib/connection/logger');

// Set up the runner
var runner = new Runner({
    logLevel:'error'
  , runners: 1
  , failFast: true
});

var testFiles = [
  // Functional tests
  '/test/tests/functional/pool_tests.js',
  '/test/tests/functional/server_tests.js',
  '/test/tests/functional/cursor_tests.js',
  '/test/tests/functional/extend_cursor_tests.js',
  '/test/tests/functional/undefined_tests.js',
  '/test/tests/functional/tailable_cursor_tests.js',
  '/test/tests/functional/error_tests.js',
  '/test/tests/functional/operations_tests.js',
  '/test/tests/functional/operation_example_tests.js',
  '/test/tests/functional/basic_single_server_auth_tests.js',
  '/test/tests/functional/basic_replset_server_auth_tests.js',
  '/test/tests/functional/replset_tests.js',
  // Replicaset SDAM tests
  '/test/tests/functional/replset_state_tests.js',
  // Replicaset Server selection tests
  '/test/tests/functional/replset_server_selection_tests.js',
  '/test/tests/functional/mongos_server_selection_tests.js',
  // Replicaset max staleness tests
  '/test/tests/functional/max_staleness_tests.js',
  // Client Metadata test
  '/test/tests/functional/client_metadata_tests.js'
]

// Check if we support es6 generators
try {
  eval("(function *(){})");

  // Single server Mock Tests
  testFiles.push('/test/tests/functional/single_mocks/compression_tests.js');
  testFiles.push('/test/tests/functional/single_mocks/timeout_tests.js');

  // Replicaset Mock Tests
  testFiles.push('/test/tests/functional/rs_mocks/add_remove_tests.js');
  testFiles.push('/test/tests/functional/rs_mocks/connection_tests.js');
  testFiles.push('/test/tests/functional/rs_mocks/failover_tests.js');
  testFiles.push('/test/tests/functional/rs_mocks/step_down_tests.js');
  testFiles.push('/test/tests/functional/rs_mocks/all_servers_close_tests.js');
  testFiles.push('/test/tests/functional/rs_mocks/read_preferences_tests.js');
  testFiles.push('/test/tests/functional/rs_mocks/monitoring_tests.js');
  testFiles.push('/test/tests/functional/rs_mocks/maintanance_mode_tests.js');
  testFiles.push('/test/tests/functional/rs_mocks/operation_tests.js');
  testFiles.push('/test/tests/functional/rs_mocks/no_primary_found_tests.js');
  testFiles.push('/test/tests/functional/rs_mocks/primary_loses_network_tests.js');

  // SDAM Tests
  testFiles.push('/test/tests/functional/sdam_monitoring_mocks/single_topology_tests.js');
  testFiles.push('/test/tests/functional/sdam_monitoring_mocks/replset_topology_tests.js');

  // Mongos Mock Tests
  testFiles.push('/test/tests/functional/mongos_mocks/single_proxy_connection_tests.js');
  testFiles.push('/test/tests/functional/mongos_mocks/multiple_proxies_tests.js');
  testFiles.push('/test/tests/functional/mongos_mocks/proxy_failover_tests.js');
  testFiles.push('/test/tests/functional/mongos_mocks/proxy_read_preference_tests.js');
  testFiles.push('/test/tests/functional/mongos_mocks/mixed_seed_list_tests.js');
} catch(err) {}

// Add all the tests to run
testFiles.forEach(function(t) {
  if(t != "") runner.add(t);
});

// // Add the Coverage plugin
// runner.plugin(new Cover({
//  logLevel: "info"
//  , filters: [
//      /_tests.js/
//    , "js-bson"
//    , "/tests/"
//    , "/tools/"
//  ]
// }));

// // Add the RCoverage plugin
// runner.plugin(new RCover({
//    logLevel: "info"
//  , filters: [
//      /_tests.js/
//    , "js-bson"
//    , "/tests/"
//    , "/tools/"
//  ]
// }));

// Add a Node version plugin
runner.plugin(new NodeVersionFilter(startupOptions));

if (!argv.s) {
  // Add a MongoDB version plugin
  runner.plugin(new MongoDBVersionFilter(startupOptions));
}

// Add a Topology filter plugin
runner.plugin(new MongoDBTopologyFilter(startupOptions));
// Add a Filter allowing us to specify that a function requires Promises
runner.plugin(new ES6PromisesSupportedFilter())
// Add a Filter allowing us to validate if generators are available
runner.plugin(new ES6GeneratorsSupportedFilter())

// Exit when done
runner.on('exit', function(errors, results) {
  process.exit(0)
});

// Set Logger level for driver
// Logger.setLevel('info');
Logger.setLevel('error');
// Logger.setLevel('debug');
// Logger.filter('class', ['ReplSet', 'Server', 'Connection']);
// Logger.filter('class', ['ReplSet', 'Server', 'Pool', 'Connection']);
// Logger.filter('class', ['ReplSet', 'Server', 'Cursor']);
//Logger.filter('class', ['Mongos', 'Server']);
//Logger.filter('class', ['Mongos', 'Server']);
// Logger.filter('class', ['Mongos']);
// Logger.filter('class', ['ReplSet']);

// We want to export a smoke.py style json file
if(argv.r) {
  console.log("Writing smoke output to " + argv.r);
  smokePlugin.attachToRunner(runner, argv.r);
}

// Are we running a functional test
if(argv.t == 'functional') {
  console.log(path.join(path.resolve('db'), f("data-%d", 27017)))
  //
  // Single server
  var config = {
      host: 'localhost'
    , port: 27017
    , skipStart: startupOptions.skipStartup
    , skipTermination: startupOptions.skipShutdown
    , manager: new ServerManager('mongod', {
      dbpath: path.join(path.resolve('db'), f("data-%d", 27017))
    })
  }

  if(argv.e == 'replicaset') {
    config = {
        host: 'localhost', port: 31000, setName: 'rs'
      , topology: function(self, _mongo) {
        return new _mongo.ReplSet([{
            host: 'localhost', port: 31000
        }], { setName: 'rs' });
      }
      , manager: new ReplSetManager('mongod', [{
        tags: {loc: 'ny'},
        // mongod process options
        options: {
          bind_ip: 'localhost',
          port: 31000,
          dbpath: f('%s/../db/31000', __dirname)
        }
      }, {
        tags: {loc: 'sf'},
        options: {
          bind_ip: 'localhost',
          port: 31001,
          dbpath: f('%s/../db/31001', __dirname)
        }
      }, {
        tags: {loc: 'sf'},
        priority:0,
        options: {
          bind_ip: 'localhost',
          port: 31002,
          dbpath: f('%s/../db/31002', __dirname)
        }
      }, {
        tags: {loc: 'sf'},
        options: {
          bind_ip: 'localhost',
          port: 31003,
          dbpath: f('%s/../db/31003', __dirname)
        }
      }, {
        arbiter: true,
        options: {
          bind_ip: 'localhost',
          port: 31004,
          dbpath: f('%s/../db/31004', __dirname)
        }
      }], {
        replSet: 'rs'
      })
    }
  }

  if(argv.e == 'sharded') {
    //
    // Sharded
    config = {
        host: 'localhost'
      , port: 51000
      , skipStart: startupOptions.skipStartup
      , skipTermination: startupOptions.skipShutdown
      , topology: function(self, _mongo) {
        return new _mongo.Mongos([{
            host: 'localhost'
          , port: 51000
        }]);
      }, manager: new ShardingManager({

      })
    }
  }

  //
  // Authentication Configuration
  if(argv.e == 'auth') {
    config = {
        host: 'localhost'
      , port: 27017
      , skipStart: startupOptions.skipStartup
      , skipTermination: startupOptions.skipShutdown
      , manager: new ServerManager('mongod', {
        dbpath: path.join(path.resolve('db'), f("data-%d", 27017)),
        auth:null
      })
    }
  }

  // If we have a test we are filtering by
  if(argv.f) {
    runner.plugin(new FileFilter(argv.f));
  }

  if(argv.n) {
    runner.plugin(new TestNameFilter(argv.n));
  }

  // Add travis filter
  runner.plugin(new TravisFilter());

  // Skip startup
  if(startupOptions.skipStartup) {
    return runner.run(Configuration(config));
  }

  // Skip the version download and use local mongod in PATH
  if(argv.l) {
    return runner.run(Configuration(config));
  }

  // Kill any running MongoDB processes and
  // `install $MONGODB_VERSION` || `use existing installation` || `install stable`
  m(function(err){
    if(err) return console.error(err) && process.exit(1);

    m.current(function(err, version){
      if(err) return console.error(err) && process.exit(1);
      console.log('Running tests against MongoDB version `%s`', version);
      // Run the configuration
      runner.run(Configuration(config));
    });
  });
}
