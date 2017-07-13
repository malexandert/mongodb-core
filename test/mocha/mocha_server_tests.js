"use strict";

var f = require('util').format
  , Long = require('bson').Long
  , expect = require('chai').expect
  , Server = require('../../lib/topologies/server')
  , bson = require('bson');

describe('Server tests', function() {
  it('should correctly connect server to single instance', {
    metadata: { requires: { topology: "single" } },

    test: function(done) {
      var self = this;

      // Attempt to connect
      var server = new Server({
          host: this.configuration.host
        , port: this.configuration.port
        , bson: new bson()
      })

      // Add event listeners
      server.on('connect', function(server) {
        server.destroy();
        done();
      });

      // Start connection
      server.connect();
    }
  });

  it('should correctly connect server to single instance and execute ismaster', {
    metadata: { requires: { topology: "single" } },

    test: function(done) {
      var self = this;

      // Attempt to connect
      var server = new Server({
          host: this.configuration.host
        , port: this.configuration.port
        , bson: new bson()
      })

      // Add event listeners
      server.on('connect', function(server) {
        server.command('admin.$cmd', {ismaster:true}, function(err, r) {
          expect(err).to.be.null;
          expect(r.result.ismaster).to.be.true;
          expect(r.connection).to.not.be.null;

          server.destroy();
          done();
        });
      });

      // Start connection
      server.connect();
    }
  });

  it('should correctly connect server to single instance and execute ismaster returning raw', {
    metadata: { requires: { topology: "single" } },

    test: function(done) {
      var self = this;

      // Attempt to connect
      var server = new Server({
          host: this.configuration.host
        , port: this.configuration.port
        , bson: new bson()
      })

      // Add event listeners
      server.on('connect', function(server) {
        server.command('admin.$cmd', {ismaster:true}, {
          raw: true
        }, function(err, r) {
          expect(err).to.be.null;
          expect(r.result).to.be.an.instanceof(Buffer);;
          expect(r.connection).to.not.be.null;

          server.destroy();
          done();
        });
      });

      // Start connection
      server.connect();
    }
  });

  it('should correctly connect server to single instance and execute insert', {
    metadata: { requires: { topology: "single" } },

    test: function(done) {
      var self = this;

      // Attempt to connect
      var server = new Server({
          host: this.configuration.host
        , port: this.configuration.port
        , bson: new bson()
      })

      // Add event listeners
      server.on('connect', function(server) {
        server.insert('integration_tests.inserts', {a:1}, function(err, r) {
          expect(err).to.be.null;
          expect(r.result.n).to.eql(1);

          server.insert('integration_tests.inserts', {a:1}, {ordered:false}, function(err, r) {
            expect(err).to.be.null;
            expect(r.result.n).to.eql(1);

            server.destroy();
            done();
          });
        });
      });

      // Start connection
      server.connect();
    }
  });

  it('should correctly connect server to single instance and execute insert (with compression if supported by the server)', {
    metadata: { requires: { topology: ["single"] } },

    test: function(done) {
      var self = this;

      // Attempt to connect
      var server = new Server({
          host: this.configuration.host
        , port: this.configuration.port
        , bson: new bson()
        , compression: { compressors: ['snappy'] }
      })

      // Add event listeners
      server.on('connect', function(server) {
        server.insert('integration_tests.inserts', {a:1}, function(err, r) {
          expect(err).to.be.null;
          expect(r.result.n).to.eql(1);

          server.insert('integration_tests.inserts', {a:1}, {ordered:false}, function(err, r) {
            expect(err).to.be.null;
            expect(r.result.n).to.eql(1);

            server.destroy();
            done();
          });
        });
      });

      // Start connection
      server.connect();
    }
  });

  it('should correctly connect server to single instance and execute bulk insert', {
    metadata: { requires: { topology: "single" } },

    test: function(done) {
      var self = this;

      // Attempt to connect
      var server = new Server({
          host: this.configuration.host
        , port: this.configuration.port
        , bson: new bson()
      })

      // Add event listeners
      server.on('connect', function(server) {
        server.insert('integration_tests.inserts', [{a:1}, {b:1}], function(err, r) {
          expect(err).to.be.null;
          expect(r.result.n).to.eql(2);

          server.insert('integration_tests.inserts', [{a:1}, {b:1}], {ordered:false}, function(err, r) {
            expect(err).to.be.null;
            expect(r.result.n).to.eql(2);

            server.destroy();
            done();
          });
        });
      });

      // Start connection
      server.connect();
    }
  });

  it('should correctly connect server to single instance and execute insert with w:0', {
    metadata: { requires: { topology: "single" } },

    test: function(done) {
      var self = this;

      // Attempt to connect
      var server = new Server({
          host: this.configuration.host
        , port: this.configuration.port
        , bson: new bson()
      })

      // Add event listeners
      server.on('connect', function(server) {
        server.insert('integration_tests.inserts', {a:1}, {writeConcern: {w:0}}, function(err, r) {
          expect(err).to.be.null;
          expect(r.result.ok).to.eql(1);

          server.insert('integration_tests.inserts', {a:1}, {ordered:false, writeConcern: {w:0}}, function(err, r) {
            expect(err).to.be.null;
            expect(r.result.ok).to.eql(1);

            server.destroy();
            done();
          });
        });
      });

      // Start connection
      server.connect();
    }
  });

  it('should correctly connect server to single instance and execute update', {
    metadata: { requires: { topology: "single" } },

    test: function(done) {
      var self = this;

      // Attempt to connect
      var server = new Server({
          host: this.configuration.host
        , port: this.configuration.port
        , bson: new bson()
      })

      // Add event listeners
      server.on('connect', function(_server) {
        _server.update('integration_tests.inserts_example2', [{
          q: {a: 1}, u: {'$set': {b:1}}, upsert:true
        }], {
          writeConcern: {w:1}, ordered:true
        }, function(err, results) {
          expect(err).to.be.null;
          expect(results.result.n).to.eql(1);

          _server.destroy();
          done();
        });
      });

      // Start connection
      server.connect();
    }
  });

  it('should correctly connect server to single instance and execute remove', {
    metadata: { requires: { topology: "single" } },

    test: function(done) {
      var self = this;

      // Attempt to connect
      var server = new Server({
          host: this.configuration.host
        , port: this.configuration.port
        , bson: new bson()
      })

      // Add event listeners
      server.on('connect', function(_server) {
        server.insert('integration_tests.remove_example', {a:1}, function(err, r) {
          expect(err).to.be.null;
          expect(r.result.ok).to.eql(1);

          _server.remove('integration_tests.remove_example', [{q: {a:1}, limit:1}], {
            writeConcern: {w:1}, ordered:true
          }, function(err, results) {
            expect(err).to.be.null;
            expect(results.result.n).to.eql(1);

            _server.destroy();
            done();
          });
        });
      });

      // Start connection
      server.connect();
    }
  });

  /**
   * @ignore
   */
  it('should correctly recover with multiple restarts', {
    metadata: {
      requires: { topology: ['single'] },
      ignore: { travis: true }
    },

    // The actual test we wish to run
    test: function(done) {
      this.timeout(0);
      var self = this;

      var testDone = false;

      // Attempt to connect
      var server = new Server({
          host: this.configuration.host
        , port: this.configuration.port
        , bson: new bson()
      })

      // Add event listeners
      server.on('connect', function(_server) {
        var count = 1;
        var allDone = 0;
        var ns = "integration_tests.t";

        var execute = function() {
          if(!testDone) {
            server.insert(ns, {a:1, count: count}, function(err, r) {
              count = count + 1;

              // Execute find
              var cursor = _server.cursor(ns, {
                find: ns, query: {}, batchSize: 2
              });

              // Execute next
              cursor.next(function(err, d) {
                setTimeout(execute, 500);
              });
            })
          } else {
            server.insert(ns, {a:1, count: count}, function(err, r) {
              expect(err).to.be.null;

              // Execute find
              var cursor = _server.cursor(ns, {
                find: ns, query: {}, batchSize: 2
              });

              // Execute next
              cursor.next(function(err, d) {
                expect(err).to.be.null;
                server.destroy();
                done();
              });
            })
          }
        }

        setTimeout(execute, 500);
      });

      var count = 2

      var restartServer = function() {
        if(count == 0) {
          testDone = true;
          return;
        }

        count = count - 1;

        self.configuration.manager.stop().then(function() {
          setTimeout(function() {
            self.configuration.manager.start().then(function() {
              setTimeout(restartServer, 1000);
            });
          }, 2000);
        });
      }

      setTimeout(restartServer, 1000);
      server.connect();
    }
  });

  it('should correctly reconnect to server with automatic reconnect enabled', {
    metadata: {
      requires: {
        topology: "single"
      }
      // ignore: { travis:true }
    },

    test: function(done) {
      var self = this;
      var Server = this.configuration.require.Server
        , ReadPreference = this.configuration.require.ReadPreference;

      // Attempt to connect
      var server = new Server({
          host: this.configuration.host
        , port: this.configuration.port
        , reconnect: true
        , size: 1
        , reconnectInterval: 50
      })

      // Test flags
      var emittedClose = false;

      // Add event listeners
      server.on('connect', function(_server) {
        // Execute the command
        _server.command("system.$cmd", {ismaster: true}, {readPreference: new ReadPreference('primary')}, function(err, result) {
          expect(err).to.be.null;
          _server.s.currentReconnectRetry = 10;

          // Write garbage, force socket closure
          try {
            var a = new Buffer(100);
            for(var i = 0; i < 100; i++) a[i] = i;
            result.connection.write(a);
          } catch(err) {}

          // Ensure the server died
          setTimeout(function() {
            // Attempt a proper command
            _server.command("system.$cmd", {ismaster: true}, {readPreference: new ReadPreference('primary')}, function(err, result) {
              expect(err).to.not.be.null;
            });
          }, 100);
        });
      });

      server.once('close', function() {
        emittedClose = true;
      });

      server.once('reconnect', function() {
        expect(emittedClose).to.be.true;
        expect(server.isConnected()).to.be.true;
        expect(server.s.pool.retriesLeft).to.eql(30);
        server.destroy();
        done();
      });

      // Start connection
      server.connect();
    }
  });

  it('should correctly reconnect to server with automatic reconnect disabled', {
    metadata: {
      requires: {
        topology: "single"
      },
      // ignore: { travis:true }
    },

    test: function(done) {
      var self = this;
      var Server = this.configuration.require.Server
        , ReadPreference = this.configuration.require.ReadPreference;

      // Attempt to connect
      var server = new Server({
          host: this.configuration.host
        , port: this.configuration.port
        , reconnect: false
        , size: 1
      })

      // Test flags
      var emittedClose = false;
      var emittedError = false;

      // Add event listeners
      server.on('connect', function(_server) {
        // Execute the command
        _server.command("system.$cmd", {ismaster: true}, {readPreference: new ReadPreference('primary')}, function(err, result) {
          expect(err).to.be.null;
          // Write garbage, force socket closure
          try {
            result.connection.destroy();
          } catch(err) {}

          process.nextTick(function() {
            // Attempt a proper command
            _server.command("system.$cmd", {ismaster: true}, {readPreference: new ReadPreference('primary')}, function(err, result) {
              expect(err).to.not.be.null;
            });
          });
        });
      });

      server.on('close', function() {
        emittedClose = true;
      });

      server.on('error', function() {
        emittedError = true;
      });

      setTimeout(function() {
        expect(emittedClose).to.be.true;
        expect(server.isConnected()).to.be.false;
        server.destroy();
        done();
      }, 500);

      // Start connection
      server.connect();
    }
  });

  it('should reconnect when initial connection failed', {
    metadata: {
      requires: {
        topology: 'single'
      },
      ignore: { travis:true }
    },

    test: function(done) {
      var self = this;
      var Server = this.configuration.require.Server
        , ReadPreference = this.configuration.require.ReadPreference
        , manager = this.configuration.manager;

      manager.stop('SIGINT').then(function() {
        // Attempt to connect while server is down
        var server = new Server({
            host: self.configuration.host
          , port: self.configuration.port
          , reconnect: true
          , reconnectTries: 2
          , size: 1
          , emitError: true
        });

        var errors = [];

        server.on('connect', function() {
          done();
          server.destroy();
        });

        server.on('reconnect', function() {
          done();
          server.destroy();
        });

        server.on('error', function(err) {
          expect(err).to.not.be.null;;
          expect(err.message.indexOf('failed to')).to.not.eql(-1);
          manager.start().then(function() {});
        });

        server.connect();
      })
    }
  });

  it('should correctly place new connections in available list on reconnect', {
    metadata: {
      requires: {
        topology: "single"
      },
      // ignore: { travis:true }
    },

    test: function(done) {
      var self = this;
      var Server = this.configuration.require.Server
        , ReadPreference = this.configuration.require.ReadPreference;

      // Attempt to connect
      var server = new Server({
          host: this.configuration.host
        , port: this.configuration.port
        , reconnect: true
        , size: 1
        , reconnectInterval: 50
      })

      // Test flags
      var emittedClose = false;

      // Add event listeners
      server.on('connect', function(_server) {
        // Execute the command
        _server.command("system.$cmd", {ismaster: true}, {readPreference: new ReadPreference('primary')}, function(err, result) {
          expect(err).to.be.null;
          _server.s.currentReconnectRetry = 10;

          // Write garbage, force socket closure
          try {
            var a = new Buffer(100);
            for(var i = 0; i < 100; i++) a[i] = i;
            result.connection.write(a);
          } catch(err) {}
        });
      });

      server.once('close', function() {
        emittedClose = true;
      });

      server.once('reconnect', function() {
        for(var i = 0; i < 100; i++) {
          server.command("system.$cmd", {ismaster: true}, function(err, result) {
            expect(err).to.be.null;
          });
        }

        server.command("system.$cmd", {ismaster: true}, function(err, result) {
          expect(err).to.be.null;

          setTimeout(function() {
            expect(server.s.pool.availableConnections.length).to.be.greaterThan(0);
            expect(server.s.pool.inUseConnections.length).to.eql(0);
            expect(server.s.pool.connectingConnections.length).to.eql(0);

            server.destroy();
            done();
          }, 1000);
        });
      });

      // Start connection
      server.connect();
    }
  });

  it('should not overflow the poolSize due to concurrent operations', {
    metadata: {
      requires: {
        topology: 'single'
      },
      ignore: { travis:true }
    },

    test: function(done) {
      var self = this;
      var Server = this.configuration.require.Server
        , ReadPreference = this.configuration.require.ReadPreference
        , manager = this.configuration.manager;

      // Attempt to connect while server is down
      var server = new Server({
          host: this.configuration.host
        , port: this.configuration.port
        , reconnect: true
        , reconnectTries: 2
        , size: 50
        , emitError: true
      });

      server.on('connect', function() {
        var left = 5000;

        for(var i = 0; i < 5000; i++) {
          server.insert(f("%s.massInsertsTest", self.configuration.db), [{a:1}], {
            writeConcern: {w:1}, ordered:true
          }, function(err, results) {
            left = left - 1;

            if(!left) {
              expect(server.connections().length).to.eql(50);

              done();
              server.destroy();
            }
          });
        }
      });

      server.connect();
    }
  });

  it('should correctly connect execute 5 evals in parallel', {
    metadata: { requires: { topology: "single" } },

    test: function(done) {
      var self = this;

      // Attempt to connect
      var server = new Server({
          host: this.configuration.host
        , port: this.configuration.port
        , size: 10
        , bson: new bson()
      })

      // Add event listeners
      server.on('connect', function(server) {
        var left = 5;
        var start = new Date().getTime();

        for (var i = 0; i < left; i++) {
          server.command('system.$cmd', {eval: 'sleep(100);'}, function(err, r) {
            left = left - 1;

            if(left == 0) {
              var total = new Date().getTime() - start;
              expect(total).to.be.greaterThan(5*100);
              expect(total).to.be.lessThan(1000);

              server.destroy();
              done();
            }
          });
        }
      });

      // Start connection
      server.connect();
    }
  });

  it('should correctly promoteValues when calling getMore on queries', {
    metadata: {
      requires: {
        node: ">0.8.0",
        topology: ['single', 'ssl', 'wiredtiger']
      }
    },

    // The actual test we wish to run
    test: function(done) {
      var self = this;

      // Attempt to connect
      var server = new Server({
          host: this.configuration.host
        , port: this.configuration.port
        , size: 10
        , bson: new bson()
      });
      // Namespace
      var ns = 'integration_tests.remove_example';

      // Add event listeners
      server.on('connect', function(server) {
        var docs = new Array(150).fill(0).map(function(_, i) {
          return {
            _id: 'needle_' + i,
            is_even: i % 2,
            long: bson.Long.fromString('1234567890'),
            double: 0.23456,
            int: 1234
          };
        });

        server.insert(ns, docs, function(err, r) {
          expect(err).to.be.null;
          expect(r.result.ok).to.eql(1);

          // Execute find
          var cursor = server.cursor(ns, {
              find: ns
            , query: {}
            , limit: 102          
          }, {
            promoteValues: false
          });

          function callNext(cursor) {
            cursor.next(function(err, doc) {
              if(!doc) {
                return done();
              }

              expect(doc.int).to.be.an('object');
              expect(doc.int._bsontype).to.eql('Int32');
              expect(doc.long).to.be.an('object');
              expect(doc.long._bsontype).to.eql('Long');
              expect(doc.double).to.be.an('object');

              // Call next
              callNext(cursor);
            });
          }

          callNext(cursor);
        });
      });

      // Start connection
      server.connect();
    }
  });
});
