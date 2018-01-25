_             = require "underscore"
async         = require "async"
Kafka         = require "node-rdkafka"
pump          = require "pump"
{ Writable }  = require "stream"

Consumer = require "../src/Consumer"

cs    = null
rnd   = _.random 1, 10000000
topic = "test-topic-#{rnd}"

getConfig = ->
	host:       "localhost:9092"
	topic:      topic
	groupId:    "test-groupId-#{rnd}"
	fromOffset: "earliest"

produceData = (options, amount = 100, cb) ->
	cb = if cb then (_.once cb) else (->)

	timeout = null
	total   = 0

	p = new Kafka.Producer
		"dr_msg_cb":            true
		"dr_cb":                true
		"metadata.broker.list": options.host

	p.on "delivery-report", (error, dr) -> total++

	p.once "ready", ->
		console.log "Producer ready"
		setTimeout ->
			async.eachSeries [1..amount], (i, cb) ->
				process.stdout.write "#{i}, "
				p.produce options.topic, null, Buffer.from JSON.stringify count: i, source: "origin"
				setTimeout cb, 15
		, 2000

	p.connect()

describe "Consumer", ->
	describe "Flowing mode", ->
		config = getConfig()
		total  = 100
		count  = 0

		after (done) ->
			@timeout 60000
			cs.destroy null, (error) ->
				throw error if error
				done()

		it "should get data", (done) ->
			@timeout 60000

			produceData config, total

			count  = 0
			config = getConfig()
			cs     = new Consumer config

			onData = (data) ->
				if ++count is total
					cs.removeListener "data", onData
					done()

			cs.on "data", onData

	describe "Back pressure mode", ->
		config = getConfig()
		total  = 100
		count  = 0

		beforeEach ->
			config = getConfig()

		afterEach (done) ->
			@timeout 60000
			cs.destroy null, (error) ->
				throw error if error
				setTimeout done, 5000

		it "should get data - fast consumer", (done) ->
			@timeout 60000

			produceData config, total

			count  = 0
			config = getConfig()
			cs     = new Consumer config

			pump [
				cs
				new Writable objectMode: true, write: (obj, enc, cb) ->
					if ++count is total
						cs.destroy()
					setTimeout cb, 1
			], (error) ->
				throw error if error
				done()

		it "should get data - slow consumer", (done) ->
			@timeout 60000

			produceData config, total

			count  = 0
			config = getConfig()
			cs     = new Consumer config

			pump [
				cs
				new Writable objectMode: true, write: (obj, enc, cb) ->
					if ++count is total
						cs.destroy()
					setTimeout cb, 20
			], (error) ->
				throw error if error
				done()
