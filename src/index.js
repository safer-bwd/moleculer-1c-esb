const ApplicationWorker = require('./worker');
const { asyncPool, isArray, merge } = require('./utils');

module.exports = {
  settings: {
    esb: {
      operationsConcurrency: 5,

      restart: {
        startingDelay: 100,
        maxDelay: 60 * 1000,
        timeMultiple: 2,
      },

      connection: {
        // Use a single session for all links
        singleSession: true,

        // Connection operations timeout
        // https://github.com/safer-bwd/node-1c-esb
        operationTimeoutInSeconds: 30,

        // https://github.com/amqp/rhea#connectoptions
        // https://its.1c.ru/db/esbdoc3/content/20006/hdoc
        amqp: {
          port: 6698,
          max_frame_size: 1000000,
          channel_max: 7000,
          reconnect: {
            reconnect_limit: 5,
            initial_reconnect_delay: 100,
            max_reconnect_delay: 60 * 1000,
          },
        }
      },

      sender: {
        keepAlive: true,

        // Message sending timeout
        timeoutInSeconds: 30,

        // https://github.com/amqp/rhea#open_senderaddressoptions
        amqp: {},
      },

      receiver: {
        // Convert message before call handlers (consumers)
        convertMessage: true,

        // Maximum number of parallel message processings
        parallel: 0, // If parallel === 0 default rhea.js behavior is used

        // Start receiving delay (ms)
        startDelay: 0,

        // https://github.com/amqp/rhea#open_receiveraddressoptions
        amqp: {},
      },
    },
  },

  applications: {}, // object or array

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
    async sendToChannel(applicationID, channelName, payload, options = {}) {
      const worker = this.$workers.get(applicationID);
      if (!worker) {
        throw new Error(`Worker for 1C:ESB application with id '${applicationID}' not found!`);
      }

      const res = await worker.send(channelName, payload, options);

      return res;
    },
  },

  merged(schema) {
    if (!isArray(schema.applications)) {
      const ids = Object.keys(schema.applications);
      schema.applications = Object.values(schema.applications).map((v, i) => {
        v.id = ids[i];
        return v;
      });
    }
  },

  created() {
    this.$workers = new Map();

    if (this.schema.applications.length === 0) {
      return;
    }

    const enabledApplications = this.schema.applications
      .filter((app) => !app.disabled)
      .filter((app) => app.channels.filter((ch) => !ch.disabled).length > 0);

    if (enabledApplications.length === 0) {
      return;
    }

    this.logger.debug('1C:ESB workers are creating...');

    enabledApplications.forEach((opts) => {
      const options = merge({}, this.settings.esb, opts);
      const worker = new ApplicationWorker(this, options);
      this.$workers.set(worker.applicationID, worker);
    });

    this.logger.info('1C:ESB workers created.');
  },

  async started() {
    if (this.$workers.size === 0) {
      return;
    }

    this.logger.debug('1C:ESB workers are starting...');

    const workers = Array.from(this.$workers.values());

    const concurrency = this.settings.esb.operationsConcurrency;
    await asyncPool(concurrency, workers, (worker) => worker.start());

    this.logger.info('1C:ESB workers started.');
  },

  async stopped() {
    if (this.$workers.size === 0) {
      return;
    }

    this.logger.debug('1C:ESB workers are stopping...');

    const workers = Array.from(this.$workers.values());

    const concurrency = this.settings.esb.operationsConcurrency;
    await asyncPool(concurrency, workers, (worker) => worker.stop());

    this.logger.info('1C:ESB workers stopped.');
  },
};
