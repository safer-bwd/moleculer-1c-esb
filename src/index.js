const { merge } = require('./utils');
const ApplicationWorker = require('./worker');

module.exports = {
  settings: {
    esb: {
      reconnect: {
        numOfAttempts: Infinity,
        startingDelay: 100,
        maxDelay: 60 * 1000,
        timeMultiple: 2,
      },

      operationTimeoutInSeconds: 60,

      // https://github.com/amqp/rhea
      amqp: {
        // https://github.com/amqp/rhea#connectoptions
        // https://its.1c.ru/db/esbdoc3/content/20006/hdoc
        connection: {
          port: 6698,
          max_frame_size: 1000000,
          channel_max: 7000,
          reconnect: {
            reconnect_limit: 10,
            initial_reconnect_delay: 100,
            max_reconnect_delay: 60 * 1000,
          },
        },

        // https://github.com/amqp/rhea#open_senderaddressoptions
        sender: {},

        // https://github.com/amqp/rhea#open_receiveraddressoptions
        receiver: {}
      },
    },
  },

  applications: {},

  actions: {
    'send-to-channel': {
      params: {
        application: { type: 'string', empty: false },
        channel: { type: 'string', empty: false },
        payload: { type: 'any' },
        options: { type: 'object', optional: true },
      },
      async handler(ctx) {
        const {
          application, channel, payload, options
        } = ctx.params;

        const { message } = await this.sendToChannel(application, channel, payload, options);

        return message;
      }
    }
  },

  methods: {
    async sendToChannel(appId, channelName, payload, options = {}) {
      const worker = this.$workers.get(appId);
      if (!worker) {
        throw new Error(`Worker for 1C:ESB application with id '${appId}' not found!`);
      }

      const res = await worker.send(channelName, payload, options);

      return res;
    },
  },

  created() {
    this.$workers = new Map();

    const appIds = Object.keys(this.schema.applications);
    if (appIds.length === 0) {
      return;
    }

    this.logger.debug('1C:ESB workers are creating...');

    appIds.forEach((id) => {
      const options = merge({}, this.settings.esb, this.schema.applications[id], { id });
      if (!options.channels || Object.keys(options.channels).length === 0) {
        this.logger.debug(`Worker for 1C:ESB application with id '${id}' has not been created: channel list is empty.`);
        return;
      }

      const worker = new ApplicationWorker(this, options);
      this.$workers.set(id, worker);
    });

    this.logger.info('1C:ESB workers created.');
  },

  async started() {
    if (this.$workers.size === 0) {
      return;
    }

    this.logger.debug('1C:ESB workers are starting...');

    const workers = Array.from(this.$workers.values());
    await Promise.all(workers.map((worker) => worker.start()));

    this.logger.info('1C:ESB workers started.');
  },

  async stopped() {
    if (this.$workers.size === 0) {
      return;
    }

    this.logger.debug('1C:ESB workers are stopping...');

    const workers = Array.from(this.$workers.values());
    await Promise.all(workers.map((worker) => worker.stop()));

    this.logger.info('1C:ESB workers stopped.');
  },
};
