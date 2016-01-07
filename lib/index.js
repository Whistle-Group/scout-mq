var _       = require('lodash')
  , Promise = require('bluebird')
  , amqp    = require('amqplib')
  , log     = require('winston')
  , whenConnected;

// ## TODO: RabbitMQ connection drop and recovery
// ## See heartbeat and timeout thingey here:
// ## http://www.scriptscoop.net/t/7488eab3ab91/node.js-amqp-node-wont-detect-a-connection-drop.html

// ## TODO: amqp address configurable - either directly from env vars, or passed in options on init()


function openChannel() {
  return whenConnected
    .then(function (conn) { return conn.createChannel(); });
}


exports.sendToQueue = function sendToQueue(queue, message, options) {
  var ch;
  options = options || {};
  return openChannel()
    .then(function (channel) { ch = channel; })
    .then(function () { return ch.assertQueue(queue, options.qOpts); })
    .then(function () { return ch.sendToQueue(queue, new Buffer(message), options.msgOpts); })
    .then(function () { return ch.close(); });
};


exports.publish = function publish(exchange, exchangeType, routingKey, message, options) {
  var ch;
  options = options || {};

  return openChannel()
    .then(function (channel) { ch = channel; })
    .then(function () { return ch.assertExchange(exchange, exchangeType, options.exOpts); })
    .then(function () { return ch.publish(exchange, routingKey, new Buffer(message), options.msgOpts);  })
    .then(function () { return ch.close(); });
};


exports.consume = function consume(queue, processMessage, options) {
  var ch;
  options = options || {};

  return openChannel()
    .then(function (channel) { ch = channel; })
    .then(function () {
      if (options.bindings) {
        var bindOpts = options.bindings;
        return Promise.resolve()
          .then(function () { return ch.assertExchange(bindOpts.ex, bindOpts.exType, bindOpts.exOpts); })
          .then(function () { return ch.assertQueue(bindOpts.q, bindOpts.qOpts ); })
          .then(function () {
            return Promise.map(bindOpts.bindingKeys, function (bindingKey) {
              return ch.bindQueue(bindOpts.q, bindOpts.ex, bindingKey);
            });
          });
      }
      else {
        return ch.assertQueue(queue, options.qOpts);
      }
    })
    .then(function () {
      return new Promise(function (resolve, reject) {
        var shouldAck = !options || true !== options.noAck
          , consumeMessage;

        consumeMessage = function (msg) {
          processMessage(msg, function (err) {
            if (!err && shouldAck) { ch.ack(msg); }
            resolve();
          });
        };

        ch.consume(queue, consumeMessage, options.consumeOpts);
      });
    });
};


exports.init = function init(amqpConnString) {
  exports.whenConnected = whenConnected = amqp.connect(amqpConnString);
  return exports;
};
