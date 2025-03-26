const { Connection, ConnectionEvents } = require('1c-esb');
const rheaPromise = require('rhea-promise');
const {
  asyncPool, cloneDeep, isArray, isString, merge, pick, noop
} = require('./utils');

if (!global.AbortController) {
  // eslint-disable-next-line global-require
  global.AbortController = require('node-abort-controller').AbortController;
}

const {
  message: rheaMessage,
  ReceiverEvents,
  SenderEvents,
  SessionEvents,
  generate_uuid: uuid4,
  string_to_uuid: string2uuid,
  uuid_to_string: uuid2string,
} = rheaPromise;

const createMessage = (payload, params = {}) => {
  const message = merge({}, params);
  message.application_properties = message.application_properties || {};

  if (!message.message_id) {
    const messageId = uuid4();
    message.message_id = string2uuid(messageId);
    message.application_properties.integ_message_id = messageId;
  }

  let contentType;
  let contentCharset;
  if (Buffer.isBuffer(payload)) {
    message.body = rheaMessage.data_section(payload);
  } else if (isString(payload)) {
    message.body = rheaMessage.data_section(Buffer.from(payload, 'utf8'));
    contentCharset = 'utf8';
  } else {
    message.body = rheaMessage.data_section(Buffer.from(JSON.stringify(payload), 'utf8'));
    contentType = 'application/json';
    contentCharset = 'utf8';
  }

  if (!message.application_properties.ContentType && contentType) {
    message.application_properties.ContentType = contentType;
  }

  if (!message.application_properties.ContentCharset && contentCharset) {
    message.application_properties.ContentCharset = contentCharset;
  }

  return message;
};

const id2string = (id) => (Buffer.isBuffer(id) ? uuid2string(id) : String(id));

const isDataSection = (obj) => typeof obj === 'object' && obj.constructor.name === 'Section'
  && obj.typecode === 0x75 && !obj.multiple;

const convertReceivedMessage = (message) => {
  const converted = cloneDeep(message);

  if (converted.message_id) {
    converted.message_id = id2string(converted.message_id);
  }

  if (converted.correlation_id) {
    converted.correlation_id = id2string(converted.correlation_id);
  }

  if (isDataSection(converted.body)) {
    converted.body = converted.body.content;
  }

  return converted;
};

const ChannelDirections = {
  In: 'in',
  Out: 'out'
};

const defaultOptions = {
  operationTimeoutInSeconds: 60,
  operationsConcurrency: 5,

  restart: {
    startingDelay: 100,
    maxDelay: 60 * 1000,
    timeMultiple: 2,
  },

  connection: {
    singleSession: true,
    // https://github.com/amqp/rhea#connectoptions
    // https://its.1c.ru/db/esbdoc3/content/20006/hdoc
    amqp: {
      port: 6698,
      max_frame_size: 1000000,
      channel_max: 7000,
      reconnect: {
        reconnect_limit: 1,
        initial_reconnect_delay: 100,
        max_reconnect_delay: 60 * 1000,
      },
    }
  },

  sender: {
    keepAlive: true,
    // https://github.com/amqp/rhea#open_senderaddressoptions
    amqp: {},
  },

  receiver: {
    convertMessage: true,
    // https://github.com/amqp/rhea#open_receiveraddressoptions
    amqp: {},
  }
};

const States = {
  Starting: 'starting',
  Started: 'started',
  Restarting: 'restarting',
  Stopped: 'stopped',
  Stopping: 'stopping',
};

class ApplicationWorker {
  constructor(service, options = {}) {
    this._service = service;

    this._options = merge({}, defaultOptions, options);
    if (isArray(this._options.channels)) {
      this._options.channels = this._options.channels.reduce((acc, channel) => {
        acc[channel.name] = channel;
        return acc;
      }, {});
    }

    this._applicationName = this._options.name || this._options.url.split('/').pop();
    this._applicationID = this._options.id || this._applicationName;

    this._state = States.Stopped;
    this._connection = null;
    this._session = null;
    this._senders = new Map();
    this._receivers = new Map();
    this._abortController = null;

    this._restartTimer = null;
    this._restartDelay = 0;
    this._restartAttempt = 0;
  }

  get applicationID() {
    return this._applicationID;
  }

  get applicationName() {
    return this._applicationName;
  }

  async start() {
    if (this._state !== States.Stopped) {
      throw new Error('Worker is not stopped!');
    }

    await this._start();
  }

  async send(channelName, payload, params = {}, options = {}) {
    const message = createMessage(payload, params);
    const messageId = id2string(message.message_id);

    const logPrefix = `1C:ESB [${this.applicationID}]: [${channelName}]`;
    this._service.logger.debug(`${logPrefix}: message '${messageId}' is sending...`);
    this._service.logger.trace(`${logPrefix}: message '${messageId}' payload:`, payload);

    let delivery;
    let sender;
    try {
      if (this._state !== States.Started) {
        throw new Error('Worker is not started!');
      }

      if (!this._isConnectionOpen()) {
        throw new Error('Connection is not opened!');
      }

      if (this._options.connection.singleSession && !this._isSessionOpen()) {
        throw new Error('Session is not opened!');
      }

      if (this._options.sender.keepAlive) {
        sender = this._senders.get(channelName);
        if (!sender) {
          throw new Error(`Sender for channel '${channelName}' not found!`);
        }
        if (!sender.isOpen()) {
          throw new Error(`Sender '${sender.name}' is not opened!`);
        }
      } else {
        sender = await this._createSender(channelName);
      }

      delivery = await sender.send(message, {
        timeoutInSeconds: this._options.operationTimeoutInSeconds,
        ...options,
      });
    } catch (err) {
      this._service.logger.error(`${logPrefix}: sending message '${messageId}' failed:`, err);
      throw err;
    }

    this._service.logger.debug(`${logPrefix}: message '${messageId}' sent.`);

    if (!this._options.sender.keepAlive) {
      sender.close({ closeSession: false }).catch(noop).then(() => { sender = null; });
    }

    return { message, delivery };
  }

  async stop() {
    await this._stop();
  }

  async _start(attempt = 0) {
    this._state = States.Starting;

    if (attempt) {
      this._service.logger.debug(`1C:ESB [${this.applicationID}]: worker is restarting (attempt = ${attempt})...`);
    } else {
      this._service.logger.debug(`1C:ESB [${this.applicationID}]: worker is starting...`);
    }

    const onDisconnect = (ctx) => {
      if (!ctx.reconnecting && this._state === States.Started) {
        this._stop().catch(noop).then(() => {
          if (this._options.restart) {
            this._scheduleRestart();
          }
        });
      }
    };

    try {
      await this._connect();
      this._connection.on(ConnectionEvents.disconnected, onDisconnect);
      this._restartAttempt = 0;
      this._restartDelay = 0;
      this._state = States.Started;
      this._service.logger.info(`1C:ESB [${this.applicationID}]: worker started.`);
    } catch (err) {
      await this._disconnect().catch(noop);
      this._service.logger.error(`1C:ESB [${this.applicationID}]: worker start error.`, err);
      if (this._state === States.Starting && this._options.restart) {
        this._scheduleRestart();
      } else {
        this._state = States.Stopped;
        throw err;
      }
    }
  }

  async _connect() {
    this._connection = await this._openConnection();

    if (this._options.connection.singleSession) {
      this._session = await this._createSession();
    }

    return this._createLinks();
  }

  _scheduleRestart() {
    this._state = States.Restarting;

    const {
      startingDelay = 100,
      maxDelay = 60 * 1000,
      timeMultiple = 2,
    } = this._options.restart;

    if (!this._restartDelay) {
      this._restartDelay = startingDelay;
    } else if (this._restartDelay < maxDelay) {
      this._restartDelay *= timeMultiple;
      if (this._restartDelay > maxDelay) {
        this._restartDelay = maxDelay;
      }
    }

    this._restartTimer = setTimeout(() => {
      this._restartAttempt += 1;
      this._start(this._restartAttempt).catch(noop);
    }, this._restartDelay);

    this._service.logger.debug(`1C:ESB [${this.applicationID}]: scheduled restart worker in ${this._restartDelay} ms.`);
  }

  async _openConnection() {
    this._service.logger.debug(`1C:ESB [${this.applicationID}]: connection is opening...`);

    const connectionOpts = merge(pick(this._options, [
      'url', 'clientKey', 'clientSecret', 'operationTimeoutInSeconds'
    ]), {
      amqp: this._options.connection.amqp,
    });

    const connection = new Connection(connectionOpts);

    connection
      .on(ConnectionEvents.connectionError, (ctx) => {
        this._service.logger.debug(`1C:ESB [${this.applicationID}]: connection error.`, ctx.error);
      })
      .on(ConnectionEvents.connectionOpen, () => {
        this._service.logger.debug(`1C:ESB [${this.applicationID}]: connection opened: ${connection.id}.`);
      })
      .on(ConnectionEvents.protocolError, (ctx) => {
        this._service.logger.trace(`1C:ESB [${this.applicationID}]: connection protocol error.`, ctx.error);
      })
      .on(ConnectionEvents.settled, () => {
        this._service.logger.trace(`1C:ESB [${this.applicationID}]: connection settled.`);
      })
      .on(ConnectionEvents.disconnected, (ctx) => {
        if (connection.id) {
          this._service.logger.debug(`1C:ESB [${this.applicationID}]: connection '${connection.id}' disconnected.`);
          if (ctx.reconnecting) {
            this._service.logger.debug(`1C:ESB [${this.applicationID}]: connection '${connection.id}' reconnecting...`);
          } else {
            this._service.logger.debug(`1C:ESB [${this.applicationID}]: connection '${connection.id}' reconnection aborted.`);
          }
        }
      })
      .on(ConnectionEvents.connectionClose, () => {
        this._service.logger.debug(`1C:ESB [${this.applicationID}]: connection '${connection.id}' closed.`);
      });

    this._abortController = new AbortController();
    await connection.open({ abortSignal: this._abortController.signal });
    this._abortController = null;

    return connection;
  }

  _isConnectionOpen() {
    return !!(this._connection && this._connection.isOpen());
  }

  async _createSession() {
    this._service.logger.debug(`1C:ESB [${this.applicationID}]: session is creating...`);

    this._abortController = new AbortController();

    let session;
    try {
      session = await this._connection.createSession({ abortSignal: this._abortController.signal });
    } catch (err) {
      this._service.logger.debug(`1C:ESB [${this.applicationID}]: failed to create session.`, err);
      throw err;
    }

    this._abortController = null;

    session
      .on(SessionEvents.sessionError, (ctx) => {
        const err = ctx.session && ctx.session.error;
        this._service.logger.debug(`1C:ESB [${this.applicationID}]: session '${session.id}' error.`, err);
      })
      .on(SessionEvents.sessionOpen, () => {
        this._service.logger.debug(`1C:ESB [${this.applicationID}]: session '${session.id}' opened.`);
      })
      .on(SessionEvents.settled, () => {
        this._service.logger.trace(`1C:ESB [${this.applicationID}]: session '${session.id}' settled.`);
      })
      .on(SessionEvents.sessionClose, () => {
        this._service.logger.debug(`1C:ESB [${this.applicationID}]: session '${session.id}' closed.`);
      });

    this._service.logger.debug(`1C:ESB [${this.applicationID}]: session created: ${session.id}.`);

    return session;
  }

  _isSessionOpen() {
    return !!(this._session && this._session.isOpen());
  }

  async _createLinks() {
    this._receivers = new Map();
    this._senders = new Map();

    const { channels } = this._options;
    const channelNames = Object.keys(channels);
    if (channelNames.length === 0) {
      return;
    }

    const senderChannelNames = [];
    const receiverChannelNames = [];
    channelNames.forEach((channelName) => {
      const channel = channels[channelName];
      if (!channel.disabled) {
        if (channel.direction.toLowerCase() === ChannelDirections.In) {
          receiverChannelNames.push(channelName);
        } else {
          senderChannelNames.push(channelName);
        }
      }
    });

    const concurrency = this._options.operationsConcurrency;
    this._abortController = new AbortController();
    this._abortController.signal.eventEmitter.setMaxListeners(concurrency);

    // First start senders because they could be needed on receive
    if (senderChannelNames.length > 0) {
      await asyncPool(concurrency, senderChannelNames, async (channelName) => {
        const senderOpts = merge({}, this._options.sender, channels[channelName].options);
        if (senderOpts.keepAlive) {
          const sender = await this._createSender(channelName);
          this._senders.set(channelName, sender);
        }
      });
    }

    if (receiverChannelNames.length > 0) {
      await asyncPool(concurrency, receiverChannelNames, async (channelName) => {
        const receiver = await this._createReceiver(channelName);
        this._receivers.set(channelName, receiver);
      });
    }

    this._abortController = null;
  }

  async _createReceiver(channelName) {
    this._service.logger.debug(`1C:ESB [${this.applicationID}]: receiver for '${channelName}' is creating...`);

    const { channels } = this._options;
    const receiverOpts = merge({}, this._options.receiver, channels[channelName].options);
    const receiverRheaOpts = merge({}, receiverOpts.amqp, {
      session: this._session ? this._session : null,
      abortSignal: this._abortController ? this._abortController.signal : null,
      autoaccept: false,
      // https://github.com/amqp/rhea-promise/blob/6ff5c9cf715c7e017ee76ceaefa172764cbec476/lib/session.ts#L264
      onMessage: this._receiverHandler.bind(this, channelName),
      onError: (ctx) => {
        if (ctx.receiver) {
          this._service.logger.debug(`1C:ESB [${this.applicationID}]: receiver for '${channelName}' (${ctx.receiver.name}) error`, ctx.receiver.error);
        }
      }
    });

    let receiver;
    try {
      receiver = await this._connection.createReceiver(channelName, receiverRheaOpts);
    } catch (err) {
      this._service.logger.debug(`1C:ESB [${this.applicationID}]: failed to create receiver for '${channelName}'.`, err);
      throw err;
    }

    receiver
      .on(ReceiverEvents.receiverOpen, () => {
        this._service.logger.debug(`1C:ESB [${this.applicationID}]: receiver for '${channelName}' (${receiver.name}) opened.`);
      })
      .on(ReceiverEvents.receiverDrained, () => {
        this._service.logger.trace(`1C:ESB [${this.applicationID}]: receiver for '${channelName}' (${receiver.name}) drained.`);
      })
      .on(ReceiverEvents.receiverFlow, () => {
        this._service.logger.trace(`1C:ESB [${this.applicationID}]: receiver for '${channelName}' (${receiver.name}) flow.`);
      })
      .on(ReceiverEvents.settled, () => {
        this._service.logger.trace(`1C:ESB [${this.applicationID}]: receiver for '${channelName}' (${receiver.name}) settled.`);
      })
      .on(ReceiverEvents.receiverClose, () => {
        this._service.logger.debug(`1C:ESB [${this.applicationID}]: receiver for '${channelName}' (${receiver.name}) closed.`);
      });

    this._service.logger.debug(`1C:ESB [${this.applicationID}]: receiver for '${channelName}' created: ${receiver.name}.`);

    return receiver;
  }

  async _createSender(channelName) {
    this._service.logger.debug(`1C:ESB [${this.applicationID}]: sender for '${channelName}' is creating...`);

    const { channels } = this._options;
    const senderOpts = merge({}, this._options.sender, channels[channelName].options);
    const senderRheaOpts = merge({}, senderOpts.amqp, {
      session: this._session ? this._session : null,
      abortSignal: this._abortController ? this._abortController.signal : null,
    });

    let sender;
    try {
      sender = await this._connection.createAwaitableSender(channelName, senderRheaOpts);
    } catch (err) {
      this._service.logger.debug(`1C:ESB [${this.applicationID}]: failed to create sender for '${channelName}'.`, err);
      throw err;
    }

    sender
      .on(SenderEvents.senderError, (ctx) => {
        const err = ctx.sender && ctx.sender.error;
        this._service.logger.debug(`1C:ESB [${this.applicationID}]: sender for '${channelName}' (${sender.name}) error`, err);
      })
      .on(SenderEvents.senderOpen, () => {
        this._service.logger.debug(`1C:ESB [${this.applicationID}]: sender for '${channelName}' (${sender.name}) opened.`);
      })
      .on(SenderEvents.senderDraining, () => {
        this._service.logger.trace(`1C:ESB [${this.applicationID}]: sender for '${channelName}' (${sender.name})' draining.`);
      })
      .on(SenderEvents.senderFlow, () => {
        this._service.logger.trace(`1C:ESB [${this.applicationID}]: sender for '${channelName}' (${sender.name}) flow.`);
      })
      .on(SenderEvents.sendable, () => {
        this._service.logger.trace(`1C:ESB [${this.applicationID}]: sender for '${channelName}' (${sender.name}) sendable.`);
      })
      .on(SenderEvents.accepted, () => {
        this._service.logger.trace(`1C:ESB [${this.applicationID}]: sender for '${channelName}' (${sender.name}) accepted.`);
      })
      .on(SenderEvents.modified, () => {
        this._service.logger.trace(`1C:ESB [${this.applicationID}]: sender for '${channelName}' (${sender.name}) modified.`);
      })
      .on(SenderEvents.rejected, () => {
        this._service.logger.trace(`1C:ESB [${this.applicationID}]: sender for '${channelName}' (${sender.name}) rejected.`);
      })
      .on(SenderEvents.released, () => {
        this._service.logger.trace(`1C:ESB [${this.applicationID}]: sender for '${channelName}' (${sender.name}) released.`);
      })
      .on(SenderEvents.senderClose, () => {
        this._service.logger.debug(`1C:ESB [${this.applicationID}]: sender for '${channelName}' (${sender.name}) closed.`);
      });

    this._service.logger.debug(`1C:ESB [${this.applicationID}]: sender for '${channelName}' created: ${sender.name}.`);

    return sender;
  }

  async _receiverHandler(channelName, ctx) {
    const { delivery, message: receivedMsg } = ctx;

    const message = this._options.receiver.convertMessage
      ? convertReceivedMessage(receivedMsg) : receivedMsg;

    const messageId = id2string(message.message_id);

    const logPrefix = `1C:ESB [${this.applicationID}]: [${channelName}]`;
    this._service.logger.debug(`${logPrefix}: message '${messageId}' recieved.`);

    const { handler } = this._options.channels[channelName];
    try {
      await handler.bind(this._service)(message, delivery);
      if (!rheaMessage.is_accepted(delivery.state)
        && !rheaMessage.is_rejected(delivery.state)
        && !rheaMessage.is_released(delivery.state)) {
        delivery.accept();
      }
      this._service.logger.debug(`${logPrefix}: message '${messageId}' processed.`);
    } catch (err) {
      this._service.logger.error(`${logPrefix}: message '${messageId}' processing failed:`, err);
      delivery.release({ delivery_failed: true });
    }

    let deliveryState;
    if (rheaMessage.is_accepted(delivery.state)) {
      deliveryState = 'accepted';
    } else if (rheaMessage.is_rejected(delivery.state)) {
      deliveryState = 'rejected';
    } else {
      deliveryState = 'released';
    }

    this._service.logger.debug(`${logPrefix}: message '${messageId}' delivery state: ${deliveryState}.`);
  }

  async _stop() {
    this._state = States.Stopping;
    this._service.logger.debug(`1C:ESB [${this.applicationID}]: worker is stopping...`);

    if (this._restartTimer) {
      clearTimeout(this._restartTimer);
      this._restartTimer = null;
    }

    await this._disconnect().catch(noop);

    this._restartAttempt = 0;
    this._restartDelay = 0;

    this._state = States.Stopped;
    this._service.logger.info(`1C:ESB [${this.applicationID}]: worker stopped.`);
  }

  _disconnect() {
    if (this._abortController) {
      this._abortController.abort();
      this._abortController = null;
    }

    return new Promise((resolve) => {
      process.nextTick(async () => {
        try {
          await this._closeLinks();
        } catch (err) {
          this._senders = new Map();
          this._receivers = new Map();
        }

        if (this._options.connection.singleSession) {
          try {
            await this._closeSession();
          } catch (err) {
            this._session = null;
          }
        }

        try {
          await this._closeConnection();
        } catch (err) {
          this._connection = null;
        }

        resolve();
      });
    });
  }

  async _closeConnection() {
    if (this._connection) {
      await this._connection.close();
      this._connection = null;
    }
  }

  async _closeSession() {
    if (this._session) {
      await this._session.close();
      this._session = null;
    }
  }

  async _closeLinks() {
    const concurrency = this._options.operationsConcurrency;

    // First close receivers because they could use senders
    if (this._receivers.size > 0) {
      const receivers = Array.from(this._receivers.values());
      await asyncPool(concurrency, receivers, (receiver) => receiver.close({
        closeSession: !this._options.connection.singleSession,
      }));
    }

    if (this._senders.size > 0) {
      const senders = Array.from(this._senders.values());
      await asyncPool(concurrency, senders, (sender) => sender.close({
        closeSession: !this._options.connection.singleSession,
      }));
    }

    this._receivers = new Map();
    this._senders = new Map();
  }
}

module.exports = ApplicationWorker;
