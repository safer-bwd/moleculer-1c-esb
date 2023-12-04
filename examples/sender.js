const { ServiceBroker } = require('moleculer');
const ESBMixin = require('../src');

const SenderService = {
  name: 'sender',

  mixins: [
    ESBMixin
  ],

  settings: {
    esb: { operationTimeoutInSeconds: 5 }
  },

  applications: {
    'portal-trade': {
      url: 'http://localhost:9090/applications/portal-trade',
      clientKey: '',
      clientSecret: '',
      channels: {
        'Основной::ВыгрузкаЗаказов.from_portal': {
          direction: 'out'
        }
      }
    }
  },

  actions: {
    async unloadOrder(ctx) {
      const { order } = ctx.params;
      const application = 'portal-trade';
      const channel = 'Основной::ВыгрузкаЗаказов.from_portal';
      return this.sendToChannel(application, channel, order);
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
