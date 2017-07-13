'use strict';

var Mocha = require('mocha')
  , Suite = Mocha.Suite
  , Runner = Mocha.Runner
  , Test = Mocha.Test
  , fs = require('fs')
  , path = require('path')
  , f = require('util').format
  , recursiveReadSync = require('recursive-readdir-sync')
  , metadata_ui = require('metamocha')
  , Context = require('mocha/lib/context')
  , Configuration = require('./test_configuration')
  , ServerManager = require('mongodb-topology-manager').Server
  , ReplSetManager = require('mongodb-topology-manager').ReplSet
  , ShardingManager = require('./test_topologies').Sharded;

var argv = require('optimist')
    .usage('Usage: $0 -f [file] -e [environment]')
    .demand('f')
    .argv;

// Instantiate new Mocha instance
var mocha = new Mocha({ui: 'metadata_ui'});

// Parsing files and adding tests
if (fs.lstatSync(argv.f).isDirectory()) {
  // Load the tests from the directory
  recursiveReadSync(argv.f).filter(function(file) {
    // Filter out the non-js files
    return file.substr(-3) === '.js';
  }).forEach(function(file) {
    mocha.addFile(file);
  });
} else {
  mocha.addFile(argv.f);
}

// Load files
mocha.loadFiles();

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

// Configure mongod instance
var config;
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
} else if(argv.e == 'sharded') {
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
} else if(argv.e == 'auth') {
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
} else { // Default
  config = {
      host: 'localhost'
    , port: 27017
    , skipStart: startupOptions.skipStartup
    , skipTermination: startupOptions.skipShutdown
    , manager: new ServerManager('mongod', {
      dbpath: path.join(path.resolve('db'), f("data-%d", 27017))
    })
  }
}

// Filter tests based on argv.e
if (argv.e) {
  var rootSuite = mocha.suite;
  rootSuite.suites.forEach(function(suite) {
    suite.tests = suite.tests.filter(function(test) {
      if (test.metadata && test.metadata.requires && test.metadata.requires.topology) {
        if (Array.isArray(test.metadata.requires.topology)) {
          return test.metadata.requires.topology.includes(argv.e);
        } else return test.metadata.requires.topology === argv.e;
      } else return false;
    });
  }); 
}

// Setup database
var dbConfig = Configuration(config)({});
dbConfig.start(function() {
  /**
   * Monkey patch to add the configuration details to the tests
   */
  Context.prototype.runnable = function(runnable) {
    if (!arguments.length) {
      return this._runnable;
    }

    if (runnable && runnable.metadata) {
      this.metadata = runnable.metadata;
    }

    this.test = this._runnable = runnable;
    this.configuration = dbConfig;
    return this;
  };

  // Run the tests
  mocha.run(function(failures) {
    process.on('exit', function() {
      process.exit(failures);
    })

    dbConfig.stop(function() {
      process.exit();
    });
  });
});