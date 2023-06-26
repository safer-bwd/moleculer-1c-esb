const { Connection, ConnectionEvents } = require('1c-esb');
const { merge, get, pick } = require('./utils');

const ChannelDirections = {
  In: 'in',
  Out: 'out'
};

const defaultOperationTimeoutInSeconds = 60;

module.exports = {
  settings: {
    esb: {
      operationTimeoutInSeconds: 60,

      reconnect: {
        initialDelay: 100,
        maxDelay: 60 * 1000,
        limit: Infinity,
      },

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
    async _connectApp(id) {
      const eventPrefix = `1C:ESB application '${id}'`;
      const options = merge(this.settings.esb, this.schema.applications[id]);
      const senders = new Map();
      const recivers = new Map();

      const app =  { 
        id, connection: null, senders, recivers, options, eventPrefix
      };

      await this._openAppConnection(app);
      await this._attachAppChannels(app);

      return app;
    },

    async _openAppConnection(app) {
      const { options, eventPrefix } = app;

      const connOpts = merge(pick(options, [
        'url', 'clientKey', 'clientSecret', 'reconnect', 'defaultOperationTimeoutInSeconds'
      ]), {
        amqp: get(options, 'amqp.connection', {}),
      });

      const connection = new Connection(connOpts);
      connection
        .on(ConnectionEvents.connectionOpen, () => {
          this.logger.info(`${eventPrefix}: connection opened.`);
        })
        .on(ConnectionEvents.connectionError, (ctx) => {
          this.logger.warn(`${eventPrefix}: connection error.`, ctx.error);
        })
        .on(ConnectionEvents.error, (ctx) => {
          this.logger.debug(`${eventPrefix}: error.`, ctx.error);
        })
        .on(ConnectionEvents.protocolError, (ctx) => {
          this.logger.debug(`${eventPrefix}: protocol error.`, ctx.error);
        })
        .on(ConnectionEvents.disconnected, (ctx) => {
          this.logger.warn(`${eventPrefix}: disconnected.`, ctx.error);
        })
        .on(ConnectionEvents.connectionClose, () => {
          this.logger.info(`${eventPrefix}: connection closed.`);
        });

      this.logger.debug(`${eventPrefix}: connecting...`);

      try {
        await connection.open();
      } catch (err) {
        this.logger.error(`${eventPrefix}: fail to open connection.`, err);
        throw err;
      }

      app.connection = connection;
    },

    async _attachAppChannels(app) {
      const { connection, options, eventPrefix } = app;
      const { channels } = options;

      this.logger.debug(`${eventPrefix}: attaching channels...`);

      const promises = channels.map(async(ch) => {    
        const key = `${ch.process}.${ch.channel}`;
        if (ch.direction === ChannelDirections.In) {
          this.logger.debug(`${eventPrefix}: attaching reciever for '${key}'...`);


          this.logger.info(`${eventPrefix}: attached reciever for '${key}'.`);
        } else {
          this.logger.debug(`${eventPrefix}: attaching sender for '${key}'...`);
          const senderOpts = merge(get(options, 'amqp.sender', {}), ch.options); 
          const sender = await connection.createAwaitableSender(ch.process, ch.channel, senderOpts);
          app.senders.set(key, sender);
          this.logger.info(`${eventPrefix}: attached sender for '${key}'.`);
        }
      });

      this.logger.debug(`${eventPrefix}: all channels attached.`);

      await Promise.all(promises);
    }
  },

  created() {
    this._apps = new Map();
  },

  async started() {
    this._apps = new Map();

    const appIds = Object.keys(this.schema.applications);
    if (appIds.length === 0) {
      return;
    }

    this.logger.info('♻ Connect 1C:ESB applications...');

    const promises = appIds.map(async(id) => {
      const app = await this._connectApp(id);
      this._apps.set(id, app);
    });

    await Promise.all(promises);
  },

  async stopped() {
  },
};
