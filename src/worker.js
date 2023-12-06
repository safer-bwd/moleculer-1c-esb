const { Connection, ConnectionEvents } = require('1c-esb');
const { backOff } = require('exponential-backoff');
const rheaPromise = require('rhea-promise');

const {
  get, merge, noop, pick, isString
} = require('./utils');

const {
  ReceiverEvents,
  SenderEvents,
  SessionEvents,
  generate_uuid: uuid,
  message: rheaMessage,
} = rheaPromise;

// hack rhea-promise https://github.com/amqp/rhea-promise/issues/109 -->
Object.defineProperty(rheaPromise.Receiver.prototype, 'address', {
  get() {
    return get(this, 'source.address', '');
  }
});

Object.defineProperty(rheaPromise.AwaitableSender.prototype, 'address', {
  get() {
    return get(this, 'target.address', '');
  }
});
// <--

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

  const prefix = `1C:ESB '${this._worker.applicationId}':`;
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
    this._logger.info('worker started.');

    return Promise.resolve(this);
  }

  async send(channelName, payload, options = {}) {
    if (!this._isConnectionOpen()) {
      throw new Error('Connection is not opened!');
    }

    if (!this._isSessionOpen()) {
      throw new Error('Session is not opened!');
    }

    const sender = this._getSender(channelName);
    if (!sender) {
      throw new Error(`Sender by channel '${channelName}' not found!`);
    }

    if (!sender.isOpen()) {
      throw new Error(`Sender '${sender.name}' is not opened!`);
    }

    const message = createMessage(payload, options);

    this._logger.debug(`message ${message.message_id} is sending to '${channelName}'...`);

    let delivery;
    try {
      delivery = await sender.send(message, {
        abortSignal: this._abortController ? this._abortController.signal : null,
        timeoutInSeconds: this._options.operationTimeoutInSeconds,
      });
    } catch (err) {
      this._logger.error(`failed to send message ${message.message_id} to '${channelName}'.`, err);
      throw err;
    }

    this._logger.info(`message ${message.message_id} sent to '${channelName}'.`);

    this._logger.trace(`message ${message.message_id}:`, message);
    this._logger.trace(`message ${message.message_id} payload:`, payload);

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
      retry: (err, attempt) => {
        this._clear();
        this._logger.debug(`open connection attempt ${attempt} failed.`);
        return true;
      }
    }).catch((err) => {
      this._clear();
      this._logger.error('worker fatal error.', err);
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
    let attempt = 0;

    this._connection.on(ConnectionEvents.disconnected, (ctx) => {
      if (ctx.reconnecting) { // reconnect (rhea promise logic)
        if (attempt === 0) {
          this._logger.debug(`connection '${this._connection.id}' reconnect started.`);
        } else {
          this._logger.debug(`connection '${this._connection.id}' reconnect attempt ${attempt} failed.`);
        }
        attempt += 1;
      } else if (this._connection) { // reopen connection (custom logic)
        if (attempt > 0) {
          this._logger.debug(`connection '${this._connection.id}' reconnect canceled.`);
        }
        this._close()
          .catch(() => this._clear())
          .then(() => this._tryConnect({ delayFirstAttempt: true }))
          .catch(noop);
      }
    });

    this._connection.on(ConnectionEvents.connectionOpen, () => {
      attempt = 0;
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
        this._logger.info(`connection '${connection.id}' opened.`);
      })
      .on(ConnectionEvents.connectionError, (ctx) => {
        this._logger.error('connection error.', ctx.error);
      })
      .on(ConnectionEvents.protocolError, (ctx) => {
        this._logger.error('connection protocol error.', ctx.error);
      })
      .on(ConnectionEvents.disconnected, (ctx) => {
        if (connection.id) {
          this._logger.info(`connection '${connection.id}' disconnected.`, ctx.error);
        }
      })
      .on(ConnectionEvents.connectionClose, () => {
        this._logger.info(`connection '${connection.id}' closed.`);
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

    this._logger.debug('links created.');
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
      this._logger.error(`reciever '${reciever.name}' error`, err);
    });

    reciever.on(ReceiverEvents.receiverOpen, () => {
      this._logger.info(`reciever '${reciever.name}' opened.`);
    });

    reciever.on(ReceiverEvents.receiverClose, () => {
      this._logger.info(`reciever '${reciever.name}' closed.`);
    });

    reciever.on(ReceiverEvents.message, async (ctx) => {
      const { delivery, message } = ctx;

      const payload = get(message.body, 'content', message.body);
      const messageId = Buffer.isBuffer(message.message_id)
        ? message.message_id.toString('utf8') : message.message_id;
      message.message_id = messageId;

      this._logger.info(`message ${messageId} recieved from '${channelName}'.`);
      this._logger.trace(`message ${messageId}:`, message);

      try {
        this._logger.debug(`message ${messageId} is processing...`);
        await handler.bind(this._service)(message, payload, delivery);
        if (!rheaMessage.is_accepted(delivery.state)
          && !rheaMessage.is_rejected(delivery.state)
          && !rheaMessage.is_released(delivery.state)) {
          delivery.accept();
        }
      } catch (err) {
        delivery.release({ delivery_failed: true });
        this._logger.error(`failed to process message ${messageId}.`, err);
      }

      if (rheaMessage.is_accepted(delivery.state)) {
        this._logger.debug(`message ${messageId} accepted.`);
      } else if (rheaMessage.is_rejected(delivery.state)) {
        this._logger.debug(`message ${messageId} rejected.`);
      } else {
        this._logger.debug(`message ${messageId} released.`);
      }

      this._logger.info(`message ${messageId} processed.`);
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
      this._logger.error(`sender '${sender.name}' error`, err);
    });

    sender.on(SenderEvents.senderOpen, () => {
      this._logger.info(`sender '${sender.name}' opened.`);
    });

    sender.on(SenderEvents.senderClose, () => {
      this._logger.info(`sender '${sender.name}' closed.`);
    });

    this._logger.info(`sender for channel '${channelName}' created: ${sender.name}.`);

    return sender;
  }

  _getSender(channelName) {
    return this._senders.get(channelName);
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
    const openedLinks = []
      .concat(Array.from(this._senders.values()))
      .concat(Array.from(this._recievers.values()))
      .filter((link) => !link.isClosed());

    if (openedLinks.length === 0) {
      this._recievers = new Map();
      this._senders = new Map();
      return;
    }

    this._logger.debug('links are closing...');

    try {
      await Promise.all(openedLinks.map((link) => link.close({ closeSession: false })));
    } catch (err) {
      this._logger.error('failed to close links.', err);
    }

    this._recievers = new Map();
    this._senders = new Map();
    this._logger.debug('links closed.');
  }

  async _closeSession() {
    if (!this._session) {
      return;
    }

    if (this._session.isClosed()) {
      this._session = null;
      return;
    }

    this._logger.debug('session is closing...');

    try {
      await this._session.close();
    } catch (err) {
      this._logger.error('failed to close session.', err);
    }
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
    }
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
