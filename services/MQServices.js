const amqp = require("amqplib/callback_api");
const couchbase = require("couchbase");
const config =require('../config.json');

amqp.connect(
  config.mqUrl,
  function (error0, connection) {
    if (error0) {
      throw error0;
    }
    connection.createChannel(function (error1, channel) {
      if (error1) {
        throw error1;
      }
      var queue = "outbox_messages";

      channel.assertQueue(queue, {
        durable: true,
      });
      channel.prefetch(1);

      console.log("Waiting for messages in %s", queue);
      channel.consume(queue, function (msg) {
        var key = JSON.parse(msg.content).messageId;

        let cluster = new couchbase.Cluster(config.cb, {
          username: config.cbUser,
          password: config.cbPass,
        });

        var bucket = cluster.bucket("outbox-messages");
        var t = bucket.defaultCollection();
        t.upsert(key, msg.content, (e) => {
          if (e) {
            console.log(e);
          }

          console.log("test");
        });

        console.log("Received '%s'", msg.content.toString());

        setTimeout(function () {
          channel.ack(msg);
        }, 1000);
      });
    });
  }
);
