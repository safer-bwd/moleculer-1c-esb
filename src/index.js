const { merge } = require('./utils');
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

        // https://github.com/amqp/rhea#sender
        sender: {},

        // https://github.com/amqp/rhea#receiver
        receiver: {}
      },
    },
  },

  applications: {},

  methods: {
  },

  created() {
    this._apps = new Map();

    const appIDs = Object.keys(this.schema.applications);
    if (appIDs.length === 0) {
      return;
    }

    this.logger.debug('1C:ESB applications creating...');

    appIDs.forEach((id) => {
      const options = merge({}, this.settings.esb, this.schema.applications[id], { id });
      const app = new Application(this, options);
      this._apps.set(id, app);
    });

    this.logger.debug('1C:ESB applications created.');
  },

  async started() {
    if (this._apps.size === 0) {
      return;
    }

    this.logger.debug('1C:ESB applications connecting...');

    const apps = Array.from(this._apps.values());
    await Promise.all(apps.map((app) => app.connect()));

    this.logger.debug('1C:ESB applications connected.');
  },

  async stopped() {
    if (this._apps.size === 0) {
      return;
    }

    this.logger.debug('1C:ESB applications closing...');

    const apps = Array.from(this._apps.values());
    await Promise.all(apps.map((app) => app.close()));

    this.logger.debug('1C:ESB applications closed.');
  },
};
