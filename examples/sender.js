const { ServiceBroker } = require('moleculer');
const ESBMixin = require('../src');

const SenderService = {
  name: 'sender',

  mixins: [
    ESBMixin
  ],

  settings: {
    esb: {
      operationTimeoutInSeconds: 5,
      connection: {
        amqp: { reconnect: { reconnect_limit: 5 } },
      },
      sender: {
        amqp: {},
      },
    }
  },

  applications: {
    'portal-trade': {
      url: 'http://localhost:9090/applications/portal-trade',
      clientKey: '',
      clientSecret: '',
      channels: {
        'Основной::ВыгрузкаЗаказов.from_portal': {
          direction: 'out',
          options: { amqp: {} },
        }
      }
    }
  },

  // OR:
  // applications: [
  //   {
  //     // id: 'portal-trade', // if several 1C:ESB servers
  //     url: 'http://localhost:9090/applications/portal-trade',
  //     clientKey: '',
  //     clientSecret: '',
  //     channels: [
  //       {
  //         name: 'Основной::ВыгрузкаЗаказов.from_portal',
  //         direction: 'out',
  //         options: { amqp: {} },
  //       }
  //     ],
  //   },
  // ],

  actions: {
    async unloadOrder(ctx) {
      const { order } = ctx.params;
      const application = 'portal-trade';
      const channel = 'Основной::ВыгрузкаЗаказов.from_portal';
      const options = {
        application_properties: { ContentType: 'application/json' },
      };

      return this.sendToChannel(application, channel, order, options);
    }
  },
};

const broker = new ServiceBroker({
  logLevel: 'debug'
});

broker.createService(SenderService);

const order = { id: 1, number: '1', description: 'Order #1' };

broker
  .start()
  .then(() => broker.call('sender.unloadOrder', { order }))
  .delay(5 * 1000)
  .then(() => broker.stop())
  .catch((err) => console.log('Error', err)); // eslint-disable-line no-console
