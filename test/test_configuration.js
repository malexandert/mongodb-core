var f = require('util').format
  , path = require('path')
  , bson = require('bson')
  , Logger = require('mongodb-topology-manager').Logger
  , Server = require('mongodb-core/lib/topologies/server')
  , ServerManager = require('mongodb-topology-manager').Server;

var Configuration = function(options) {
  options = options || {};
  var host = options.host || 'localhost';
  var port = options.port || 27017;
  var db = options.db || 'integration_tests';
  var mongo = null;
  var manager = options.manager;
  var skipStart = typeof options.skipStart == 'boolean' ? options.skipStart : false;
  var skipTermination = typeof options.skipTermination == 'boolean' ? options.skipTermination : false;
  var setName = options.setName || 'rs';

  // Default function
  var defaultFunction = function(self, _mongo) {
    return new _mongo.Server({
        host: self.host
      , port: self.port
    });
  };

  // Create a topology function
  var topology = options.topology || defaultFunction;

  return function(context) {
    mongo = require('..');

    return {
      start: function(callback) {
        var self = this;
        if(skipStart) return callback();

        // Purge the database
        manager.purge().then(function() {
          console.log("[purge the directories]");

          manager.start().then(function() {
            console.log("[started the topology]");

            // Create an instance
            var server = topology(self, mongo);
            console.log("[get connection to topology]");

            // Set up connect
            server.once('connect', function() {
              console.log("[connected to topology]");
              
              // Drop the database
              server.command(f("%s.$cmd", self.db), {dropDatabase: 1}, function(err, r) {
                console.log("[dropped database]");
                server.destroy();
                callback();
              });
            });

            // Connect
            console.log("[connecting to topology]");
            server.connect();
          }).catch(function(err) {
            // console.log(err.stack);
          });
        }).catch(function(err) {
          // console.log(err.stack);
        });
      },

      stop: function(callback) {
        if(skipTermination) return callback();
        // Stop the servers
        manager.stop().then(function() {
          callback();
        });
      },

      restart: function(options, callback) {
        if(typeof options == 'function') callback = options, options = {purge:true, kill:true};
        if(skipTermination) return callback();

        // Stop the servers
        manager.restart().then(function() {
          callback();
        });
      },

      setup: function(callback) {
        callback();
      },

      teardown: function(callback) {
        callback();
      },

      newTopology: function(options, callback) {
        if(typeof options == 'function') {
          callback = options;
          options = {};
        }

        callback(null, topology(this, mongo));
      },

      newConnection: function(options, callback) {
        if(typeof options == 'function') {
          callback = options;
          options = {};
        }

        var server = topology(this, mongo);
        // Set up connect
        server.once('connect', function() {
          callback(null, server);
        });

        // Connect
        server.connect();
      },

      // Additional parameters needed
      require: mongo,
      port: port,
      host: host,
      setName: setName,
      db: db,
      manager: manager,
      writeConcern: function() { return {w: 1} }
    }
  }
}

module.exports = Configuration;