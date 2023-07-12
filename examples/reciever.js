const { ServiceBroker } = require('moleculer');
const ESBMixin = require('../src');
const { get, isString } = require('../src/utils');

const RecieverService = {
  name: 'reciever',

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
        'Основной::ВыгрузкаЗаказов.to_trade': {
          direction: 'in',
          handler(message) {
            const payload = get(message.body, 'content', message.body);
            const json = isString(payload) ? payload : payload.toString('utf8');
            const order = JSON.parse(json);
            this.logger.warn('Upload order', order);
          }
        }
      }
    }
  },
};

const broker = new ServiceBroker({
  logLevel: 'debug'
});

broker.createService(RecieverService);

broker
  .start()
  .delay(5 * 1000)
  .then(() => broker.stop())
  .catch((err) => console.log('Error', err)); // eslint-disable-line no-console
