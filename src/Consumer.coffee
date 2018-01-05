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
			# "debug":                  "all"
			# "auto.offset.reset":       "earliest"
			"group.id":                options.groupId
			"metadata.broker.list":    options.host
			"rebalance_cb":            true
			"socket.keepalive.enable": true

		debug "Kafka options", kafkaOptions

		@consumer = Kafka.KafkaConsumer kafkaOptions

		@onEventLog = (log) -> debug "log", log

		@onDisconnected = =>
			debug "Disconnected"
			@emit "disconnected"

		@onUnsubscribe = =>
			debug "Unsubscribe"
			@push null

		@onReady = =>
			debug "Ready"
			@consumer.subscribe asArray options.topic
			@_read()

			@consumer.getMetadata {}, (error, meta) =>
				return debug "Producer could not get topic meta data: #{error.message}" if error
				meta.topics = _.filter meta.topics, (topic) -> topic.name is options.topic
				debug "Topic metadata", util.inspect meta, depth: null
				@emit "ready"

		@onRebalance = (err, assignments) =>
			partitions = _.chain assignments
				.filter (a) => a.topic is options.topic
				.pluck "partition"
				.value()

			debug "Rebalanced to:", partitions
			@emit "partitions", partitions

			@_read()

		@onPartitions = (partitions) =>
			offset = if Number.isInteger options.reset then options.reset else 0
			debug "Resetting to offset #{offset} on partitions", partitions
			async.each partitions, (partition, cb) =>
				@consumer.seek
					topic:     options.topic
					partition: partition
					offset:    offset
				, 0, cb
			, (error) =>
				@emit "error", error if error

		@consumer.on   "event.log",    @onEventLog
		@consumer.once "disconnected", @onDisconnected
		@consumer.once "unsubscribe",  @onUnsubscribe
		@consumer.once "ready",        @onReady
		@consumer.on   "rebalance",    @onRebalance
		@once          "partitions",   @onPartitions if options.reset

		@consumer.connect()

	_read: (size = 16) ->
		return if @isDestroyed

		unless @consumer.isConnected()
			debug "Not ready"
			return

		if @isBusy
			# debug "Busy"
			return

		debug "Read sequence: Started"

		@isBusy = true
		goOn    = true

		test = => goOn and not @isDestroyed

		iteratee = (cb) =>
			return cb() if @isDestroyed

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

					goOn = unless @isDestroyed
						@push obj
					else
						false

				cb()

		callback = (error) =>
			@emit "error", error if error
			debug "Read sequence: Stopped"
			@isBusy = false

		async.whilst test, iteratee, callback

	_destroy: (error, cb) ->
		return cb() if @isDestroyed
		@isDestroyed = true

		cleanUp = =>
			debug "Cleaning up!"

			@consumer.removeListener "event.log",    @onEventLog
			@consumer.removeListener "disconnected", @onDisconnected
			@consumer.removeListener "unsubscribe",  @onUnsubscribe
			@consumer.removeListener "ready",        @onReady
			@consumer.removeListener "rebalance",    @onRebalance
			@removeListener          "partitions",   @onPartitions

			cb?()

		debug "Stopping"
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
