const { ServiceBroker } = require('moleculer');
const { delay } = require('rhea-promise');
const ESBMixin = require('../src');

const SenderService = {
  name: 'sender',

  mixins: [
    ESBMixin
  ],

  settings: {
    esb: {}
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
    unloadOrder(ctx) {
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

(async () => {
  await broker.start();

  await broker.call('sender.unloadOrder', {
    order: { id: 1, number: '1', description: 'Order #1' }
  });

  await delay(5 * 1000);
  await broker.stop();
})().catch((err) => console.log('Error', err)); // eslint-disable-line no-console
