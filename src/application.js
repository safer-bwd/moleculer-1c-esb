const { Connection, ConnectionEvents } = require('1c-esb');
const { merge, get, pick } = require('./utils');

const ChannelDirections = {
  In: 'in',
  Out: 'out'
};

class Application {
  constructor(service, options) {
    this._options = options;

    const { id } = options;
    this._id = id;
    this._connection = null;
    this._senders = new Map();
    this._recievers = new Map();

    this._service = service;
    this._logger = this._createLogger();
  }

  async connect() {
    this._logger.debug('connecting...');
    this._connection = await this._openConnection();
    // TODO: reconnect on close
    await this._createLinks();
    this._logger.info('connected.');
  }

  async close() {
    this._logger.debug('closing...');
    await this._closeLinks();
    await this._closeConnection();
    this._logger.info('closed.');
  }

  getSender(channelName) {
    return this._senders.get(channelName);
  }

  getReciever(channelName) {
    return this._recievers.get(channelName);
  }

  isConnected() {
    return this._connection && this._connection.isOpen();
  }

  async _openConnection() {
    const connOpts = merge(pick(this._options, [
      'url', 'clientKey', 'clientSecret', 'reconnect', 'defaultOperationTimeoutInSeconds'
    ]), {
      amqp: get(this._options, 'amqp.connection', {}),
    });

    const connection = new Connection(connOpts);
    connection
      .on(ConnectionEvents.connectionOpen, () => {
        this._logger.info('connection opened.');
      })
      .on(ConnectionEvents.connectionError, (ctx) => {
        this._logger.debug('connection error.', ctx.error);
      })
      .on(ConnectionEvents.error, (ctx) => {
        this._logger.debug('error.', ctx.error);
      })
      .on(ConnectionEvents.protocolError, (ctx) => {
        this._logger.debug('protocol error.', ctx.error);
      })
      .on(ConnectionEvents.disconnected, (ctx) => {
        this._logger.info('disconnected.', ctx.error);
      })
      .on(ConnectionEvents.connectionClose, () => {
        this._logger.info('connection closed.');
      });

    this._logger.debug('connection opening...');

    try {
      await connection.open();
    } catch (err) {
      this._logger.error('failed to open connection.', err);
      throw err;
    }

    return connection;
  }

  async _closeConnection() {
    this._logger.debug('connection closing...');
    await this._connection.close();
  }

  async _createLinks() {
    this._logger.debug('links creating...');

    const { channels } = this._options;
    const channelNames = Object.keys(channels);

    await Promise.all(channelNames.map(async (name) => {
      if (channels[name].direction === ChannelDirections.In) {
        // TODO: reciever
      } else {
        const sender = await this._createSender(name);
        this._senders.set(name, sender);
      }
    }));

    this._logger.info('links created.');
  }

  async _closeLinks() {
    this._logger.debug('links closing...');

    const links = []
      .concat(Array.from(this._senders.values()))
      .concat(Array.from(this._recievers.values()));

    await Promise.all(links.map((link) => link.close()));

    this._logger.info('links closed.');
  }

  async _createSender(channelName) {
    this._logger.debug(`sender for '${channelName}' creating...`);

    const { channels } = this._options;
    const [process, channel] = channelName.split('.');

    const senderOpts = merge({}, get(this._options, 'amqp.sender', {}), channels[channelName].amqp);
    const sender = await this._connection.createAwaitableSender(process, channel, senderOpts);

    this._logger.info(`sender for '${channelName}' created.`);

    return sender;
  }

  _createLogger() {
    const prefix = `1C:ESB application '${this._id}':`;

    const methods = ['info', 'warn', 'error', 'debug'];
    const logger = methods.reduce((acc, method) => {
      acc[method] = this._service.logger[method].bind(this._service.logger, prefix);
      return acc;
    }, {});

    return logger;
  }
}

module.exports = Application;
