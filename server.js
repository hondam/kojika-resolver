/**
 * Node.js resolver with redis
 *
 * @author hondam
 * @version 0.0.1
 */

'use strict';

var ndns = require('native-dns');
var crypto = require('crypto');
var client = require('redis').createClient();
var async = require('async');
var cluster = require('cluster');
var numCPUs = require('os').cpus().length;

if (cluster.isMaster) {
  for (var i = 0; i < 2; i++) {
    var worker = cluster.fork();
  }
  cluster.on('death', function(worker) {
    //console.log('worker ' + worker.pid + ' died. restart...');
    cluster.fork();
  });
} else {
  var cache = true;
  var server = ndns.createServer();
  var tData = {
    address: '210.152.137.115',
    port: 53,
  };
  server.on('request', function (req, res) {
    var domain, rrtype, rdata, packet_hash;
    domain = req.question[0].name;
    rrtype = ndns.consts.qtypeToName(req.question[0].type);

    async.waterfall([
      function (callback) {
        if (cache) {
          var md5 = crypto.createHash('md5');
          md5.update(domain + '|' + rrtype, 'utf8');
          packet_hash = md5.digest('hex');
    
          client.get(packet_hash, function(err, reply) {
            if (err == null && reply == null) {
              //console.log('no cache! >>>', domain, rrtype);
            } else {
              //console.log('cache hit! >>> ', reply);
              rdata = JSON.parse(reply);
            }
            callback(null, rdata);
          });
        } else {
          callback(null, rdata);
        }
      }, function(rdata, callback) {
        if (!rdata) {
          ndns.resolve(domain, rrtype, false, function(err, ret) {
            //console.log('RESOLVE==================');
            //console.log(ret);
            callback(null, ret);
          });
        } else {
          callback(null, rdata);
        }
      }, function(rdata, callback) {
        res = pushRdata(res, domain, rrtype, rdata);
        try {
          res.send();
        } catch (e) {
          res.clearResources();
          res.send();
          callback(e);
        }
        if (cache && rdata !== undefined) {
          //console.log(packet_hash, rdata);
          client.set(packet_hash, JSON.stringify(rdata));
        }
      }], function(err, result) {
        console.log('err >>> ', err.message, domain, rrtype);
      });
  });
  server.serve(tData.port, tData.address);
}

//
function pushRdata(res, domain, rrtype, data) {
  switch(rrtype) {
    case 'A': case 'AAAA':
      for (var idx in data) {
        res.answer.push(ndns.A({
          name: domain,
          address: data[idx],
        }));
      }
      break;
    case 'MX':
      for (var idx in data) {
        res.answer.push(ndns.MX({
          name: domain,
          priority: data[idx].priority,
          exchange: data[idx].exchange,
        }));
      }
      break;
    case 'TXT': case 'PTR': case 'NS': case 'CNAME':
      for (var idx in data) {
        res.answer.push(ndns.NS({
          name: domain,
          data: data[idx],
        }));
      }
      break;
    case 'SRV':
      for (var idx in data) {
        res.answer.push(ndns.SRV({
          name: domain,
          priority: data[idx].priority,
          weight: data[idx].port,
          port: data[idx].port,
          target: data[idx].name,
        }));
      }
      break;
    default:
      break;
  }
  return res;
}
