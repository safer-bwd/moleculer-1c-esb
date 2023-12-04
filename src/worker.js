const { Connection, ConnectionEvents } = require('1c-esb');
const { backOff } = require('exponential-backoff');

const {
  ReceiverEvents,
  SenderEvents,
  SessionEvents,
  generate_uuid: uuid,
  message: rheaMessage
} = require('rhea-promise');

const {
  get, merge, noop, pick, isString
} = require('./utils');

if (!global.AbortController) {
  // eslint-disable-next-line global-require
  global.AbortController = require('node-abort-controller').AbortController;
}

const createMessage = (payload, options = {}) => {
  const message = merge({}, options);
  message.application_properties = message.application_properties || {};

  if (!message.message_id) {
    message.message_id = uuid();
    message.application_properties.integ_message_id = message.message_id;
  }

  let contentType;
  if (Buffer.isBuffer(payload)) {
    message.body = rheaMessage.data_section(payload);
  } else if (isString(payload)) {
    message.body = rheaMessage.data_section(Buffer.from(payload, 'utf8'));
  } else {
    message.body = rheaMessage.data_section(Buffer.from(JSON.stringify(payload), 'utf8'));
    contentType = 'application/json';
  }

  if (!message.application_properties.ContentType && contentType) {
    message.application_properties.ContentType = contentType;
  }

  return message;
};

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

function Logger(worker) {
  this._worker = worker;

  const prefix = `1C:ESB application '${this._worker.applicationId}':`;
  const methods = ['info', 'warn', 'error', 'debug', 'trace'];
  methods.forEach((method) => {
    this[method] = this._worker.service.logger[method].bind(this, prefix);
  });
}

class ApplicationWorker {
  constructor(service, options) {
    this._options = options;

    this._state = States.Stopped;

    // Moleculer service
    this._service = service;

    // 1C:ESB App
    const { id, name } = this._options;
    this._appId = id;
    this._appName = name || id;

    // AMQP
    this._connection = null;
    this._session = null;
    this._senders = new Map();
    this._recievers = new Map();

    // Operation abort controller
    this._abortController = null;

    // Logger
    this._logger = new Logger(this);
    this._logger.debug('worker created.');
  }

  get applicationId() {
    return this._appId;
  }

  get applicationName() {
    return this._appName;
  }

  get service() {
    return this._service;
  }

  start() {
    if (this._state !== States.Stopped) {
      throw new Error('Worker is not stopped!');
    }

    this._state = States.Starting;
    this._logger.debug('worker is starting...');

    this._tryConnect().catch(noop);

    this._state = States.Started;
    this._logger.debug('worker started');

    return Promise.resolve(this);
  }

  async send(channelName, payload, options = {}) {
    this._logger.debug(`sending message to '${channelName}'...`);

    let message;
    let delivery;
    try {
      if (!this._isConnectionOpen()) {
        throw new Error('Connection is not opened!');
      }

      if (!this._isSessionOpen()) {
        throw new Error('Session is not opened!');
      }

      const sender = this._getSender(channelName);
      if (!sender) {
        throw new Error(`Channel '${channelName}' not found!`);
      }

      message = createMessage(payload, options);
      delivery = await sender.send(message);
    } catch (err) {
      this._logger.error(`error send message to '${channelName}'.`, err);
      throw err;
    }

    this._logger.info(`sent message ${message.message_id} to '${channelName}'.`);
    this._logger.trace(`payload of message ${message.message_id}:`, payload);
    this._logger.trace(`message ${message.message_id}:`, message);

    return { message, delivery };
  }

  stop() {
    if (this._state === States.Stopped) {
      return Promise.resolve(this);
    }

    this._state = States.Stopping;
    this._logger.debug('worker is stopping...');

    const onStop = () => {
      this._state = States.Stopped;
      this._logger.info('worker stopped.');
    };

    const onError = (err) => {
      this._logger.error('worker error.', err);
      this._clear();
    };

    return this._close()
      .catch(onError)
      .then(onStop);
  }

  //

  async _tryConnect(options = {}) {
    const { delayFirstAttempt = false } = options;

    const {
      numOfAttempts, startingDelay, maxDelay, timeMultiple
    } = this._options.reconnect;

    return backOff(() => this._connect(), {
      numOfAttempts,
      delayFirstAttempt,
      startingDelay,
      maxDelay,
      timeMultiple,
    }).catch((err) => {
      this._logger.error('Worker fatal error (reconnect cancled).', err);
      throw err;
    });
  }

  async _connect() {
    this._abortController = new AbortController();
    const { signal: abortSignal } = this._abortController;

    this._connection = await this._openConnection({ abortSignal });
    this._session = await this._createSession({ abortSignal });
    await this._createLinks({ abortSignal });

    // reconnect
    this._connection.on(ConnectionEvents.disconnected, (ctx) => {
      if (ctx.reconnecting) {
        this._logger.debug('connection is reconnecting...');
      } else if (this._state === States.Started) {
        this._logger.debug('connection is reopening...');
        this._close()
          .catch(() => this._clear())
          .then(() => this._tryConnect({ delayFirstAttempt: true }))
          .catch(noop);
      }
    });
  }

  async _openConnection(options = {}) {
    this._logger.debug('connection is opening...');

    const connOpts = merge(pick(this._options, [
      'url', 'clientKey', 'clientSecret', 'operationTimeoutInSeconds'
    ]), {
      amqp: get(this._options, 'amqp.connection', {}),
    });

    const connection = new Connection(connOpts);
    connection
      .on(ConnectionEvents.connectionOpen, () => {
        this._logger.info('connection opened.');
      })
      .on(ConnectionEvents.connectionError, (ctx) => {
        this._logger.error('connection error.', ctx.error);
      })
      .on(ConnectionEvents.protocolError, (ctx) => {
        this._logger.error('connection protocol error.', ctx.error);
      })
      .on(ConnectionEvents.error, (ctx) => {
        this._logger.error('connection error.', ctx.error);
      })
      .on(ConnectionEvents.disconnected, (ctx) => {
        this._logger.info('connection disconnected.', ctx.error);
      })
      .on(ConnectionEvents.connectionClose, () => {
        this._logger.info('connection closed.');
      });

    await connection.open(options);

    return connection;
  }

  _isConnectionOpen() {
    return !!(this._connection && this._connection.isOpen());
  }

  _isSessionOpen() {
    return !!(this._session && this._session.isOpen());
  }

  async _createSession(options = {}) {
    this._logger.debug('session is creating...');

    let session;
    try {
      session = await this._connection.createSession(options);
    } catch (err) {
      this._logger.error('failed to create session.', err);
      throw err;
    }

    session.on(SessionEvents.sessionError, (ctx) => {
      const err = ctx.session && ctx.session.error;
      this._logger.error(`session '${session.id}' error.`, err);
    });

    session.on(SessionEvents.sessionOpen, () => {
      this._logger.info(`session '${session.id}' opened.`);
    });

    session.on(SessionEvents.sessionClose, () => {
      this._logger.info(`session '${session.id}' closed.`);
    });

    this._logger.info(`session created: ${session.id}`);

    return session;
  }

  async _createLinks(options = {}) {
    this._logger.debug('links are creating...');

    const { channels } = this._options;
    const channelNames = Object.keys(channels);

    try {
      await Promise.all(channelNames.map(async (name) => {
        const direction = channels[name].direction.toLowerCase();
        if (direction === ChannelDirections.In) {
          const reciever = await this._createReciever(name, options);
          this._recievers.set(name, reciever);
        } else {
          const sender = await this._createSender(name, options);
          this._senders.set(name, sender);
        }
      }));
    } catch (err) {
      this._logger.error('failed to create links.', err);
      throw err;
    }

    this._logger.info('links created.');
  }

  async _createReciever(channelName, options = {}) {
    this._logger.debug(`reciever for channel '${channelName}' is creating...`);

    const { channels } = this._options;
    const { handler, amqp: amqpOpts = {} } = channels[channelName];
    const [process, channel] = channelName.split('.');

    const recieverOpts = merge({}, get(this._options, 'amqp.reciever', {}), amqpOpts, {
      autoaccept: false,
    }, options, {
      session: this._session
    });

    let reciever;
    try {
      reciever = await this._connection.createReceiver(process, channel, recieverOpts);
    } catch (err) {
      this._logger.error(`failed to create reciever for channel '${channelName}'.`, err);
      throw err;
    }

    reciever.on(ReceiverEvents.receiverError, (ctx) => {
      const err = ctx.receiver && ctx.receiver.error;
      this._logger.error(`reciever '${reciever.name}' for channel '${channelName}' error`, err);
    });

    reciever.on(ReceiverEvents.receiverOpen, () => {
      this._logger.info(`reciever '${reciever.name}' for channel '${channelName}' opened.`);
    });

    reciever.on(ReceiverEvents.receiverClose, () => {
      this._logger.info(`reciever '${reciever.name}' for channel '${channelName}' closed.`);
    });

    reciever.on(ReceiverEvents.message, async (ctx) => {
      const { delivery, message } = ctx;

      const payload = get(message.body, 'content', message.body);
      const messageId = Buffer.isBuffer(message.message_id)
        ? message.message_id.toString('utf8') : message.message_id;
      message.message_id = messageId;

      this._logger.info(`recieved message ${messageId} from '${channelName}'.`);
      this._logger.trace(`message ${messageId}:`, message);

      try {
        this._logger.debug(`processing message ${messageId} from '${channelName}'...`);
        await handler.bind(this._service)(message, payload, delivery);
        if (!rheaMessage.is_accepted(delivery.state)
          && !rheaMessage.is_rejected(delivery.state)
          && !rheaMessage.is_released(delivery.state)) {
          delivery.accept();
        }
      } catch (err) {
        delivery.release({ delivery_failed: true });
        this._logger.warn(`processing error for message ${messageId} from '${channelName}'.`, err);
      }

      if (rheaMessage.is_accepted(delivery.state)) {
        this._logger.debug(`accepted message ${messageId} from '${channelName}'.`);
      } else if (rheaMessage.is_rejected(delivery.state)) {
        this._logger.debug(`rejected message ${messageId} from '${channelName}'.`);
      } else {
        this._logger.debug(`released message ${messageId} from '${channelName}'.`);
      }

      this._logger.info(`processed message ${messageId} from '${channelName}'.`);
    });

    this._logger.info(`reciever for channel '${channelName}' created: ${reciever.name}.`);

    return reciever;
  }

  async _createSender(channelName, options = {}) {
    this._logger.debug(`sender for channel '${channelName}' is creating...`);

    const { channels } = this._options;
    const { amqp: amqpOpts } = channels[channelName];
    const [process, channel] = channelName.split('.');

    const senderOpts = merge({}, get(this._options, 'amqp.sender', {}), amqpOpts, options, {
      session: this._session,
      timeoutInSeconds: this._options.operationTimeoutInSeconds,
    });

    let sender;
    try {
      sender = await this._connection.createAwaitableSender(process, channel, senderOpts);
    } catch (err) {
      this._logger.error(`failed to create sender for channel '${channelName}'.`, err);
      throw err;
    }

    sender.on(SenderEvents.senderError, (ctx) => {
      const err = ctx.sender && ctx.sender.error;
      this._logger.error(`sender '${sender.name}' for '${channelName}' error`, err);
    });

    sender.on(SenderEvents.senderOpen, () => {
      this._logger.info(`sender '${sender.name}' for channel '${channelName}' opened.`);
    });

    sender.on(SenderEvents.senderClose, () => {
      this._logger.info(`sender '${sender.name}' for channel '${channelName}' closed.`);
    });

    this._logger.info(`sender '${sender.name}' for channel '${channelName}' created: ${sender.name}.`);

    return sender;
  }

  _getSender(channelName) {
    return this._senders.get(channelName);
  }

  _getReciever(channelName) {
    return this._recievers.get(channelName);
  }

  async _close() {
    if (this._abortController) {
      this._abortController.abort();
      this._abortController = null;
    }

    await this._closeLinks();
    await this._closeSession();
    await this._closeConnection();
  }

  async _closeLinks() {
    const links = []
      .concat(Array.from(this._senders.values()))
      .concat(Array.from(this._recievers.values()))
      .filter((link) => link.isOpen());

    if (links.length === 0) {
      this._recievers = new Map();
      this._senders = new Map();
      return;
    }

    this._logger.debug('links are closing...');

    try {
      await Promise.all(links.map((link) => link.close({ closeSession: false })));
    } catch (err) {
      this._logger.error('failed to close links.', err);
      throw err;
    }

    this._recievers = new Map();
    this._senders = new Map();
    this._logger.info('links closed.');
  }

  async _closeSession() {
    if (!this._session) {
      return;
    }

    if (!this._session.isOpen()) {
      this._session = null;
      return;
    }

    this._logger.debug('session is closing...');

    try {
      await this._session.close();
    } catch (err) {
      this._logger.error('failed to close session.', err);
      throw err;
    }

    this._session = null;
  }

  async _closeConnection() {
    if (!this._connection) {
      return;
    }

    if (!this._connection.isOpen()) {
      this._connection = null;
      return;
    }

    this._logger.debug('connection is closing...');

    try {
      await this._connection.close();
    } catch (err) {
      this._logger.error('failed to close connection.', err);
      throw err;
    }

    this._connection = null;
  }

  async _clear() {
    this._abortController = null;
    this._connection = null;
    this._session = null;
    this._recievers = new Map();
    this._senders = new Map();
  }
}

module.exports = ApplicationWorker;
