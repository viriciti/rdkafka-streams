_            = require "underscore"
asArray      = require "as-array"
async        = require "async"
debug        = (require "debug") "rdkafka-streams:consumer"
Kafka        = require "node-rdkafka"
util         = require "util"
{ Readable } = require "stream"

class Consumer extends Readable
	constructor: (options) ->
		debug "Options", options
		super objectMode: true

		@total       = 0
		@isBusy      = false
		@isDestroyed = false

		_.each [ "host", "topic", "groupId" ], (attr) ->
			throw new Error "No `#{attr}` in options" unless options[attr]

		kafkaOptions =
			"group.id":                options.groupId
			"metadata.broker.list":    options.host
			"socket.keepalive.enable": true

		kafkaOptions["auto.offset.reset"] = options.fromOffset if options.fromOffset
		kafkaOptions["debug"]             = options.debug      if options.debug

		debug "Kafka options", kafkaOptions

		@consumer = Kafka.KafkaConsumer kafkaOptions

		@onEventLog = (log) ->
			debug "log", log.severity, log.fac, log.message

		@onDisconnected = =>
			debug "Disconnected"
			@emit "disconnected"

		@onUnsubscribe = =>
			debug "Unsubscribe"
			@push null

		@onReady = =>
			@consumer.subscribe asArray options.topic
			@_read()
			@emit "ready"
			debug "Ready"

		@consumer.on   "event.log",    @onEventLog
		@consumer.once "disconnected", @onDisconnected
		@consumer.once "unsubscribe",  @onUnsubscribe
		@consumer.once "ready",        @onReady

		@consumer.connect()

	_read: (size = 16) ->
		console.log "read 1"
		return if @isDestroyed
		console.log "read 2"

		unless @consumer.isConnected()
			debug "Not ready"
			return @once "ready", => @_read size

		console.log "read 3"
		if @isBusy
			# debug "Busy"
			return
		console.log "read 4"

		debug "Read sequence: Started"

		@isBusy = true

		test = => @isBusy

		iteratee = (cb) =>
			if @isDestroyed or @_readableState.ended
				@isBusy = false
				return cb()

			@consumer.consume size, (error, messages) =>
				return cb error if error
				@total +=  messages.length
				debug "New #{messages.length}, total #{@total}"

				for message in messages
					try
						json = message.value.toString()
					catch error
						debug "Error parsing this message:\n#{message.value}\nfrom:\n#{JSON.stringify message}"
						return cb error

					try
						value = JSON.parse json
					catch error
						debug "Error parsing this message:\n#{json}\nfrom:\n#{JSON.stringify json}"
						return cb error

					obj = _.extend {}, message, { value }

					if @isDestroyed or @_readableState.ended
						@isBusy = false
						return

					@isBusy = @push obj

				cb()

		callback = (error) =>
			@emit "error", error if error
			debug "Read sequence: Stopped"

		async.whilst test, iteratee, callback

	_destroy: (error, cb) ->
		return cb() if @isDestroyed
		@isDestroyed = true

		debug "Stopping"

		cleanUp = =>
			debug "Cleaning up!"

			@consumer.removeListener "event.log",    @onEventLog
			@consumer.removeListener "disconnected", @onDisconnected
			@consumer.removeListener "unsubscribe",  @onUnsubscribe
			@consumer.removeListener "ready",        @onReady

			cb?()

		disconnect = =>
			if not @consumer._isConnecting and not @consumer._isConnected
				debug "Stopped: Concumer not connecting and not connected"
				return cleanUp()

			if @consumer._isConnecting
				debug "Stopping: waiting for ready before disconnect"
				return @consumer.once "ready", disconnect

			@consumer.unsubscribe()

			timeout = setTimeout ->
				debug "Stopped: Consumer disconnect timed out"
				cleanUp()
			, 6000

			process.nextTick =>
				debug "Stopping: Consumer disconnect..."
				@consumer.disconnect =>
					clearTimeout timeout
					debug "Stopped: Consumer disconnected"
					cleanUp()

		disconnect()

module.exports = Consumer
