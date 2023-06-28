const { Connection, ConnectionEvents } = require('1c-esb');
const { ReceiverEvents, SenderEvents } = require('rhea-promise');
const { merge, get, pick } = require('./utils');

const ChannelDirections = {
  In: 'in',
  Out: 'out'
};

const ReceiverSettleMode = {
  AutoSettle: 0,
  SettleOnDisposition: 1
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
    this._logger = this._initLogger();
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

    this._senders = new Map();
    this._recievers = new Map();
    this._connection = null;

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
        const reciever = await this._createReciever(name);
        this._recievers.set(name, reciever);
      } else {
        const sender = await this._createSender(name);
        this._senders.set(name, sender);
      }
    }));

    this._logger.info('links created.');
  }

  async _createSender(channelName) {
    this._logger.debug(`sender for '${channelName}' creating...`);

    const { channels } = this._options;
    const { amqp: amqpOpts } = channels[channelName];
    const [process, channel] = channelName.split('.');

    const senderOpts = merge({}, get(this._options, 'amqp.sender', {}), amqpOpts);
    const sender = await this._connection.createAwaitableSender(process, channel, senderOpts);

    sender.on(SenderEvents.senderError, (err) => {
      this._logger.debug(`sender for '${channelName}' error`, err);
    });

    this._logger.info(`sender for '${channelName}' created.`);

    return sender;
  }

  async _createReciever(channelName) {
    this._logger.debug(`reciever for '${channelName}' creating...`);

    const { channels } = this._options;
    const { handler, amqp: amqpOpts = {} } = channels[channelName];
    const [process, channel] = channelName.split('.');

    const recieverOpts = merge({}, get(this._options, 'amqp.reciever', {}), amqpOpts, {
      rcv_settle_mode: ReceiverSettleMode.SettleOnDisposition
    });

    const reciever = await this._connection.createReceiver(process, channel, recieverOpts);

    reciever.on(ReceiverEvents.receiverError, (err) => {
      this._logger.debug(`reciever for '${channelName}' error`, err);
    });

    reciever.on(ReceiverEvents.message, async (ctx) => {
      const { delivery, message } = ctx;

      if (Buffer.isBuffer(message.message_id)) {
        message.correlation_id = message.correlation_id.toString('utf8');
      }

      if (Buffer.isBuffer(message.correlation_id)) {
        message.correlation_id = message.correlation_id.toString('utf8');
      }

      const { message_id: messageId } = message;

      this._logger.debug(`message ${messageId} recieved from '${channelName}'.`);

      try {
        await handler.bind(this._service)(message);
        delivery.accept();
        this._logger.info(`message ${messageId} from '${channelName}' accepted.`);
      } catch (err) {
        delivery.reject();
        this._logger.error(`message ${messageId} from '${channelName}' rejected.`, err);
      }
    });

    this._logger.info(`reciever for '${channelName}' created.`);

    return reciever;
  }

  async _closeLinks() {
    this._logger.debug('links closing...');

    const links = []
      .concat(Array.from(this._senders.values()))
      .concat(Array.from(this._recievers.values()));

    await Promise.all(links.map((link) => link.close()));

    this._logger.info('links closed.');
  }

  _initLogger() {
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
