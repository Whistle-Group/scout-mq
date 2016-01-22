var _       = require('lodash')
  , Promise = require('bluebird')
  , amqp    = require('amqplib')
  , log     = require('winston')
  , EXCHANGE_TYPE_MAPPER
  , clientRequestedClose
  , whenConnected;

EXCHANGE_TYPE_MAPPER = {
  dx: 'direct',
  fx: 'fanout',
  tx: 'topic',
};


// ## TODO: RabbitMQ connection drop and recovery
// ## See heartbeat and timeout thingey here:
// ## http://www.scriptscoop.net/t/7488eab3ab91/node.js-amqp-node-wont-detect-a-connection-drop.html

function reconnect(amqpConnString) {
  return exports.whenConnected = whenConnected = new Promise(function (resolve, reject) {
    setTimeout(function () {
      connect(amqpConnString).then(resolve);
    }, 500);
  });
}

function connect(amqpConnString) {
  return amqp.connect(amqpConnString)
    .tap(function (conn) {
      log.info(':: [ScoutMQ] Client connected');

      conn.on('close', function () {
        log.info(':: [ScoutMQ] Connection closed...');
        if (clientRequestedClose) {
          exports.whenConnected = whenConnected = new Promise(function (resolve, reject) {});
        }
        else {
          reconnect(amqpConnString);
        }
      });

      conn.on('error', function (err) {
        log.error(':: [ScoutMQ] Connection error:', err.message);
      });
    })
    .catch(function (err) {
      return reconnect(amqpConnString);
    });
}


function openChannel() {
  return whenConnected
    .then(function (conn) { return conn.createChannel(); });
}


exports.detectExchangeType = function detectExchangeType(exchangeName) {
  return EXCHANGE_TYPE_MAPPER[exchangeName.split('.')[0]];
};


exports.sendToQueue = function sendToQueue(queue, message, options) {
  var ch;
  options = options || {};
  return openChannel()
    .then(function (channel) { ch = channel; })
    .then(function () { return ch.assertQueue(queue, options.queueOpts); })
    .then(function () { return ch.sendToQueue(queue, new Buffer(message), options.messageOpts); })
    .then(function () { return ch.close(); });
};


exports.publish = function publish(exchange, exchangeType, routingKey, message, options) {
  var ch;
  options = options || {};

  return openChannel()
    .then(function (channel) { ch = channel; })
    .then(function () { return ch.assertExchange(exchange, exchangeType, options.exchangeOpts); })
    .then(function () { return ch.publish(exchange, routingKey, new Buffer(message), options.messageOpts);  })
    .then(function () { return ch.close(); });
};


exports.consume = function consume(queue, processMessage, options) {
  var ch;
  options = options || {};

  return openChannel()
    .then(function (channel) { ch = channel; })
    .then(function () {
      if (options.bindOpts) {
        var bindOpts = options.bindOpts;
        return Promise.resolve()
          .then(function () { return ch.assertExchange(bindOpts.exchange, bindOpts.exchangeType, bindOpts.exchangeOpts); })
          .then(function () { return ch.assertQueue(bindOpts.queue, bindOpts.queueOpts ); })
          .then(function () {
            return Promise.map(bindOpts.bindingKeys, function (bindingKey) {
              return ch.bindQueue(bindOpts.queue, bindOpts.exchange, bindingKey);
            });
          });
      }
      else {
        return ch.assertQueue(queue, options.queueOpts);
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
  exports.whenConnected = whenConnected = new Promise(function (resolve, reject) {
    connect(amqpConnString).then(resolve);
  });

  return exports;
};


exports.close = function close() {
  if (!whenConnected) { return Promise.resolve(); }

  clientRequestedClose = true;
  return whenConnected
    .then(function (conn) { return conn.close(); });
};
