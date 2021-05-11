'use strict';

const {
  MQClient,
  MessageProperties
} = require('@aliyunmq/mq-http-sdk');

const topicPullingFlag = { };

module.exports = agent => {
  agent.messenger.on('@egg-aliyunmq/init', async (auth) => {
    const config = agent.config.aliyunmq;
    // const { auth } = config;
    agent.logger.info('@egg-aliyunmq/consumer/polling');

    const { endpoint, accessKeyId, accessKeySecret } = auth;
    const client = new MQClient(endpoint, accessKeyId, accessKeySecret);

    await initConsumer(agent, client);
    await initProducer(agent, client);
  });
  // 轮询结果
  agent.messenger.on('@egg-aliyunmq/consumer/polling', async (auth) => {
    agent.logger.info('@egg-aliyunmq/consumer/polling');
    const config = agent.config.aliyunmq;
    // const { auth } = config;
    const { endpoint, accessKeyId, accessKeySecret } = auth;
    const client = new MQClient(endpoint, accessKeyId, accessKeySecret);
    await initConsumer(agent, client);
  });

}

async function initProducer(agent, client) {
  // 获取配置
  const config = agent.config.aliyunmq;
  if (!config && !config.producer) return;

  const { options, topicsList } = config.producer;
  for (let i = 0, len = topicsList.length; i < len; i++) {
    const { instanceId, groupId, topic } = topicsList[i];
    const producer = client.getProducer(instanceId, topic);

    agent.messenger.removeAllListeners(`@egg-aliyunmq/producer/${topic}`);

    agent.messenger.on(`@egg-aliyunmq/producer/${topic}`, async payload => {
      const { body, tag } = payload;
      try {
        const msg = JSON.stringify(body);
        const res = await producer.publishMessage(msg, tag);
        agent.logger.info(`${topic}: Publish message: MessageID:%s,BodyMD5:%s`, res.body.MessageId, res.body.MessageBodyMD5);
      } catch (error) {
        agent.logger.error(`${topic} send error`, error);
      }
    });
  }
}

function pending() {
  return new Promise((resole) => {
    setTimeout(() => {
      resole(true);
    }, 10 * 1000);
  })
}

async function initConsumer(agent, client) {
  const config = agent.config.aliyunmq;
  if (!config && config.consumer) return;

  const { options, topicsList } = config.consumer;

  const pollingSignature = Symbol();

  for (let i = 0, len = topicsList.length; i < len; i++) {
    try {
      const { groupId, topic, instanceId, serviceFolderName, fileNameWithMethod } = topicsList[i];
      const consumer = client.getConsumer(instanceId, topic, groupId);
      topicPullingFlag[topic] = pollingSignature;
      (async function _polling() {
        if (topicPullingFlag[topic] !== pollingSignature) return;
        agent.logger.info('polling... ... ...');
        let res = {};
        try {
          res = await consumer.consumeMessage(1, 10);
        }
        catch (e) {

          if (e && e.Code === 'MessageNotExist') {
            agent.logger.error('MQ is empty!!!!!');
          } else {
            agent.logger.error('consumeMessage error:', JSON.stringify(e));
            agent.logger.error('consumeMessage error:', e);
          }
          // agent.logger.error('consumeMessage error:', e);
          // 只有在异常的时候，也就是没有消息的时候才会获取
          await pending();
          _polling();
        }

        if (res.code == 200) {
          // 消费消息，处理业务逻辑
          agent.logger.info("Consume Messages, requestId:%s", res.requestId);
          agent.logger.info("Consume MessagesBody:%s", JSON.stringify(res.body));
          const handles = res.body.map((message) => message.ReceiptHandle);
          const ackResult = await consumer.ackMessage(handles);
          if (ackResult.code != 204) {
            // 某些消息的句柄可能超时了会导致确认不成功
            agent.logger.info("Ack Message Fail:");
            const failHandles = ackResult.body.map((error) => error.ReceiptHandle);
            handles.forEach((handle) => {
              if (failHandles.indexOf(handle) < 0) {
                agent.logger.info("\tSucHandle:%s\n", handle);
              }
            });
          } else {
            // 消息确认消费成功
            agent.logger.info(`${topic}: Ack Message suc, RequestId:%s\n\t`, ackResult.requestId);
            const data = res.body.filter((m) => handles.includes(m.ReceiptHandle)).map((message) => {
              try {
                return JSON.parse(message.MessageBody);
              } catch (e) {
                return message.MessageBody;
              }
            });
            await agent.messenger.sendRandom('@egg-aliyunmq/consumer', {
              topic,
              options: {
                serviceFolderName,
                fileNameWithMethod
              },
              payload: data,
            });
          }
        } else {
          agent.logger.info(`${topic} res: not 200, ${JSON.stringify(res)}`);
        }
      })();
    } catch (e) {
      _polling();
      agent.logger.info(e);
    }

  }
}
