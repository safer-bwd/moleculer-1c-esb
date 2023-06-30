const { Connection, ConnectionEvents } = require('1c-esb');
const { ReceiverEvents, SenderEvents, message: rheaMessage } = require('rhea-promise');
const {
  backOff, get, merge, noop, pick
} = require('./utils');

const ChannelDirections = {
  In: 'in',
  Out: 'out'
};

const States = {
  Starting: 'starting',
  Started: 'started',
  Stopping: 'stopping',
  Stopped: 'stopped',
};

class ApplicationWorker {
  constructor(service, options) {
    this._options = options;
    this._id = options.id;
    this._state = States.Stopped;

    this._connection = null;
    this._senders = new Map();
    this._recievers = new Map();

    this._service = service;
    this._logger = this._initLogger();
  }

  async start() {
    this._state = States.Starting;
    this._logger.debug('worker starting...');
    await this._connect();
    this._state = States.Started;
    this._logger.info('worker started.');

    return this;
  }

  async stop() {
    this._state = States.Stopping;
    this._logger.debug('worker stopping...');
    await this._disconnect();
    this._state = States.Stopped;
    this._logger.info('worker stopped.');

    return this;
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

  async _connect() {
    this._connection = await this._openConnection();

    const onClose = () => {
      this._connection.removeListener(ConnectionEvents.connectionClose, onClose);
      if (this._state === States.Stopping) {
        return;
      }

      this._connection = null;
      this._disconnect().catch(noop).then(() => this._reconnect());
    };

    this._connection.on(ConnectionEvents.connectionClose, onClose);

    await this._createLinks();
  }

  async _disconnect() {
    await this._closeLinks();
    await this._closeConnection();
    this._senders = new Map();
    this._recievers = new Map();
    this._connection = null;
  }

  async _reconnect() {
    this._logger.debug('start reconnecting...');

    const handler = async () => {
      if (this._state === States.Started) {
        await this._connect();
      }
    };

    backOff(handler, {
      delayFirstAttempt: true,
      startingDelay: this._options.reconnect.initialDelay,
      maxDelay: this._options.reconnect.maxDelay,
      numOfAttempts: Infinity,
      retry: () => this._state === States.Started,
    }).catch(noop);
  }

  async _openConnection() {
    this._logger.debug('connection opening...');

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
        this._logger.warn('disconnected.', ctx.error);
      })
      .on(ConnectionEvents.connectionClose, (ctx) => {
        if (this._state === States.Stopping) {
          this._logger.info('connection closed.');
        } else {
          this._logger.warn('connection closed by peer.', ctx.error || '');
        }
      });

    try {
      await connection.open();
    } catch (err) {
      this._logger.error('failed to open connection.', err);
      throw err;
    }

    return connection;
  }

  async _closeConnection() {
    if (!this._connection) {
      return;
    }

    this._logger.debug('connection closing...');
    await this._connection.close();
  }

  async _createLinks() {
    this._logger.debug('links creating...');

    const { channels } = this._options;
    const channelNames = Object.keys(channels);

    await Promise.all(channelNames.map(async (name) => {
      const direction = channels[name].direction.toLowerCase();
      if (direction === ChannelDirections.In) {
        const reciever = await this._createReciever(name);
        this._recievers.set(name, reciever);
      } else {
        const sender = await this._createSender(name);
        this._senders.set(name, sender);
      }
    }));

    this._logger.debug('links created.');
  }

  async _createSender(channelName) {
    this._logger.debug(`sender for channel '${channelName}' creating...`);

    const { channels } = this._options;
    const { amqp: amqpOpts } = channels[channelName];
    const [process, channel] = channelName.split('.');

    const senderOpts = merge({}, get(this._options, 'amqp.sender', {}), amqpOpts);
    const sender = await this._connection.createAwaitableSender(process, channel, senderOpts);

    sender.on(SenderEvents.senderError, (err) => {
      this._logger.debug(`sender for '${channelName}' error`, err);
    });

    this._logger.info(`sender for channel '${channelName}' created.`);

    return sender;
  }

  async _createReciever(channelName) {
    this._logger.debug(`reciever for channel '${channelName}' creating...`);

    const { channels } = this._options;
    const { handler, amqp: amqpOpts = {} } = channels[channelName];
    const [process, channel] = channelName.split('.');

    const recieverOpts = merge({}, get(this._options, 'amqp.reciever', {}), amqpOpts, {
      autoaccept: false,
    });

    const reciever = await this._connection.createReceiver(process, channel, recieverOpts);

    reciever.on(ReceiverEvents.receiverError, (err) => {
      this._logger.debug(`reciever for channel '${channelName}' error`, err);
    });

    reciever.on(ReceiverEvents.message, async (ctx) => {
      const { delivery, message } = ctx;

      const messageId = Buffer.isBuffer(message.message_id) ? message.message_id.toString('utf8') : message.message_id;

      this._logger.info(`message ${messageId} recieved from '${channelName}'.`);

      try {
        this._logger.debug(`message ${messageId} from '${channelName}' processing...`);
        await handler.bind(this._service)(message, delivery);
        if (!rheaMessage.is_accepted(delivery.state)
          && !rheaMessage.is_rejected(delivery.state)
          && !rheaMessage.is_released(delivery.state)) {
          delivery.accept();
        }
        this._logger.debug(`message ${messageId} from '${channelName}' processed.`);
      } catch (err) {
        delivery.release({ delivery_failed: true });
        this._logger.warn(`message ${messageId} from '${channelName}' processing error.`, err);
      }

      if (rheaMessage.is_accepted(delivery.state)) {
        this._logger.debug(`message ${messageId} from '${channelName}' accepted.`);
      } else if (rheaMessage.is_rejected(delivery.state)) {
        this._logger.debug(`message ${messageId} from '${channelName}' rejected.`);
      } else {
        this._logger.debug(`message ${messageId} from '${channelName}' released.`);
      }
    });

    this._logger.info(`reciever for channel '${channelName}' created.`);

    return reciever;
  }

  async _closeLinks() {
    const links = []
      .concat(Array.from(this._senders.values()))
      .concat(Array.from(this._recievers.values()));

    if (links.length === 0) {
      return;
    }

    this._logger.debug('links closing...');

    await Promise.all(links.map((link) => link.close()));

    this._logger.debug('links closed.');
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

module.exports = ApplicationWorker;
