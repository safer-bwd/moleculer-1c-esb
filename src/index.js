const { MoleculerError, MoleculerRetryableError } = require('moleculer').Errors;
const { generate_uuid: uuid, message: rheaMessage } = require('rhea-promise');
const { isString, merge } = require('./utils');
const Application = require('./application');

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
      handler(ctx) {
        return this.sendToChannel(ctx.params);
      }
    }
  },

  methods: {
    async sendToChannel(params = {}) {
      const { application: appId, channel: channelName } = params;

      const app = this.applications.get(appId);
      if (!app) {
        throw new MoleculerError(`1C:ESB application '${appId}' is not found`, 400);
      }

      if (!app.isConnected()) {
        throw new MoleculerRetryableError(`1C:ESB application '${appId}' is not available`, 503);
      }

      const sender = app.getSender(channelName);
      if (!sender) {
        throw new MoleculerError(`1C:ESB application '${appId}': sender for '${channelName}' is not found`, 400);
      }

      const message = this._createMessage(params);
      const delivery = await sender.send(message);

      this.logger.info(`1C:ESB application '${appId}': message ${message.message_id} sent to '${channelName}'.`);

      return delivery;
    },

    _createMessage(params = {}) {
      const { payload, options = {} } = params;

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
    },
  },

  created() {
    this.applications = new Map();

    const appIDs = Object.keys(this.schema.applications);
    if (appIDs.length === 0) {
      return;
    }

    this.logger.debug('1C:ESB applications creating...');

    appIDs.forEach((id) => {
      const options = merge({}, this.settings.esb, this.schema.applications[id], { id });
      const app = new Application(this, options);
      this.applications.set(id, app);
    });

    this.logger.debug('1C:ESB applications created.');
  },

  async started() {
    if (this.applications.size === 0) {
      return;
    }

    this.logger.debug('1C:ESB applications connecting...');

    const apps = Array.from(this.applications.values());
    await Promise.all(apps.map((app) => app.connect()));

    this.logger.debug('1C:ESB applications connected.');
  },

  async stopped() {
    if (this.applications.size === 0) {
      return;
    }

    this.logger.debug('1C:ESB applications closing...');

    const apps = Array.from(this.applications.values());
    await Promise.all(apps.map((app) => app.close()));

    this.logger.debug('1C:ESB applications closed.');
  },
};
