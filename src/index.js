const { MoleculerError, MoleculerRetryableError } = require('moleculer').Errors;
const { generate_uuid: uuid, message: rheaMessage } = require('rhea-promise');
const { isString, merge } = require('./utils');
const ApplicationWorker = require('./worker');

const createMessage = (payload, options = {}) => {
  const message = merge({}, options);
  message.application_properties = message.application_properties || {};

  if (!message.message_id) {
    message.message_id = uuid();
    message.application_properties.integ_message_id = message.message_id;
  }

  if (Buffer.isBuffer(payload)) {
    message.body = rheaMessage.data_section(payload);
  } else if (isString(payload)) {
    message.body = rheaMessage.data_section(Buffer.from(payload, 'utf8'));
  } else {
    message.body = rheaMessage.data_section(Buffer.from(JSON.stringify(payload), 'utf8'));
    message.content_type = 'application/json';
  }

  if (message.content_type) {
    message.application_properties.contentType = message.content_type;
  }

  if (message.content_encoding) {
    message.application_properties.contentEncoding = message.content_encoding;
  }

  return message;
};

const defaultOperationTimeoutInSeconds = 60;

module.exports = {
  settings: {
    esb: {
      reconnect: {
        initialDelay: 100,
        maxDelay: 60 * 1000,
        limit: Infinity,
      },

      operationTimeoutInSeconds: defaultOperationTimeoutInSeconds,

      // https://github.com/amqp/rhea
      amqp: {
        // https://github.com/amqp/rhea#connectoptions
        connection: {
          // https://its.1c.ru/db/esbdoc3/content/20006/hdoc
          port: 6698,
          max_frame_size: 1000000,
          channel_max: 7000,
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
    async sendToChannel(appId, channel, payload, options = {}) {
      const worker = this.$workers.get(appId);
      if (!worker) {
        throw new MoleculerError(`1C:ESB application '${appId}' is not found`, 400);
      }

      if (!worker.isConnected()) {
        throw new MoleculerRetryableError(`1C:ESB application '${appId}' is not available`, 503);
      }

      const sender = worker.getSender(channel);
      if (!sender) {
        throw new MoleculerError(`1C:ESB application '${appId}': sender for '${channel}' is not found`, 400);
      }

      const message = createMessage(payload, options);
      const delivery = await sender.send(message);

      this.logger.info(`1C:ESB application '${appId}': message ${message.message_id} sent to '${channel}'.`);

      return { message, delivery };
    },
  },

  created() {
    this.$workers = new Map();

    const appIDs = Object.keys(this.schema.applications);
    if (appIDs.length === 0) {
      return;
    }

    this.logger.debug('1C:ESB applications workers creating...');

    appIDs.forEach((id) => {
      const options = merge({}, this.settings.esb, this.schema.applications[id], { id });
      const worker = new ApplicationWorker(this, options);
      this.$workers.set(id, worker);
    });

    this.logger.debug('1C:ESB applications workers created.');
  },

  async started() {
    if (this.$workers.size === 0) {
      return;
    }

    this.logger.debug('1C:ESB applications workers starting...');

    const workers = Array.from(this.$workers.values());
    await Promise.all(workers.map((worker) => worker.start()));

    this.logger.debug('1C:ESB applications workers started.');
  },

  async stopped() {
    if (this.$workers.size === 0) {
      return;
    }

    this.logger.debug('1C:ESB applications workers stopping...');

    const workers = Array.from(this.$workers.values());
    await Promise.all(workers.map((worker) => worker.stop()));

    this.logger.debug('1C:ESB applications workers stopped.');
  },
};
