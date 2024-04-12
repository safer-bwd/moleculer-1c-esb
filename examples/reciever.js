const { ServiceBroker } = require('moleculer');
const ESBMixin = require('../src');

const RecieverService = {
  name: 'reciever',

  mixins: [
    ESBMixin
  ],

  settings: {
    esb: {
      operationTimeoutInSeconds: 5,
      connection: {
        amqp: { reconnect: { reconnect_limit: 5 } },
      },
      receiver: {
        amqp: { credit_window: 10 },
      },
    }
  },

  applications: {
    'portal-trade': {
      url: 'http://localhost:9090/applications/portal-trade',
      clientKey: '',
      clientSecret: '',
      channels: {
        'Основной::ВыгрузкаЗаказов.to_trade': {
          direction: 'in',
          options: { amqp: { credit_window: 1 } },
          handler(message) {
            if (message.application_properties.ContentType !== 'application/json') {
              throw new Error('Not supported content type!');
            }

            const payload = Buffer.isBuffer(message.body) ? message.body.toString('utf8') : message.body;
            const order = JSON.parse(payload);
            this.logger.warn('Upload order:', order);
          },
        },
      },
    },
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
  //         name: 'Основной::ВыгрузкаЗаказов.to_trade',
  //         direction: 'in',
  //         options: { amqp: { credit_window: 1 } },
  //         handler(message) {
  //           if (message.application_properties.ContentType !== 'application/json') {
  //             throw new Error('Not supported content type!')
  //           }
  //           const payload = Buffer.isBuffer(message.body)
  //             ? message.body.toString('utf8')
  //             : message.body;
  //           const order = JSON.parse(payload);
  //           this.logger.warn(`Upload order:`, order);
  //         },
  //       },
  //     ],
  //   },
  // ],
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
