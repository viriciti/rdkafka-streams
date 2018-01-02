_            = require "underscore"
async        = require "async"
debug        = (require "debug") "rdkafka-streams:producer"
Kafka        = require "node-rdkafka"
util         = require "util"
{ Writable } = require "stream"

class Producer extends Writable
	constructor: (options) ->
		debug "Options", options
		super objectMode: true

		@partitions = []

		_.each [ "host", "topic" ], (attr) ->
			throw new Error "No `#{attr}` in options" unless options[attr]

		@total           = 0
		@topic           = options.topic
		@awaitPartitions = not not options.awaitPartitions
		@isDestroyed     = false
		kafkaOptions     =
			"dr_cb":                true
			"metadata.broker.list": options.host

		debug "Kafka options", kafkaOptions

		@producer = new Kafka.Producer kafkaOptions

		@onDisconnected = =>
			debug "Disconnected"
			@emit "disconnected"

		@onReady = =>
			debug "Ready"

			@producer.getMetadata {}, (error, meta) =>
				return debug "Producer could not get topic meta data: #{error.message}" if error
				meta.topics = _.filter meta.topics, (topic) -> topic.name is options.topic
				debug "Topic metadata", util.inspect meta, depth: null
				@emit "ready"

		@onDeliveryReport = (error, dr) =>
			@emit "dr", dr unless error
			@total++

		@producer.once "disconnected",    @onDisconnected
		@producer.once "ready",           @onReady
		@producer.on   "delivery-report", @onDeliveryReport

		@producer.connect (error) =>
			return @emit "error", error if error
			debug "Connected"

		pollLoop = =>
			@pollTimeout = setTimeout =>
				@producer.poll() if @producer.isConnected()
				@emit "total-delivered", @total
				debug "Total delivered:", @total
			, 1000

		pollLoop()

	_write: (message, enc, cb) ->
		unless @producer.isConnected()
			debug "Not connected yet"

			return @once "ready", =>
				debug "Connected"
				@_write message, enc, cb

		if @awaitPartitions
			unless @partitions?.length
				debug "No parttions yet"
				return @once "partitions", =>
					debug "Got partions"
					@_write message, enc, cb

		partition = if @partitions?.length then _.sample @partitions else null

		try
			@producer.produce.apply @producer, [
				@topic
				partition
				Buffer.from JSON.stringify message
			]
		catch error
			if error.code is Kafka.CODES.ERRORS.ERR__QUEUE_FULL
				debug "Kafka queue is full"
				return setTimeout (=> @_write message, enc, cb), 500

			debug "Producer error: #{error.message}"
			return cb error

		cb()

	_destroy: (error, cb) ->
		clearTimeout @pollTimeout
		return cb?() if @isDestroyed
		@isDestroyed = true

		debug "Stopping"

		cleanUp = =>
			@producer.removeListener "disconnected",    @onDisconnected
			@producer.removeListener "ready",           @onReady
			@producer.removeListener "delivery-report", @onDeliveryReport

			cb?()

		disconnect = =>
			if @producer._isConnecting
				debug "Still connecting"
				return @consumer.once "ready", disconnect

			if @producer._isConnected
				@producer.disconnect =>
					debug "Stopped: Producer disconnected"
					cleanUp()
			else
				debug "Stopped: Not connected"
				cleanUp()

		disconnect()

	setPartitions: (partitions) =>
		return unless partitions?.length
		debug "Partitions", partitions
		@partitions = partitions
		@emit "partitions"

module.exports = Producer