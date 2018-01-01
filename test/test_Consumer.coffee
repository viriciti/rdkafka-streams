# _             = require "underscore"
# async         = require "async"
# Kafka         = require "node-rdkafka"
# pump          = require "pump"
# { Writable }  = require "stream"

# Consumer = require "../src/Consumer"

# cs = null

# getConfig = ->
# 	rnd = _.random 1, 10000000

# 	host:    "localhost:9092"
# 	topic:   "test-topic-#{rnd}"
# 	groupId: "test-groupId-#{rnd}"
# 	# reset:   true

# produceData = (options, amount = 100, cb) ->
# 	cb = if cb then (_.once cb) else (->)

# 	timeout = null
# 	total   = 0

# 	p = new Kafka.Producer
# 		"dr_msg_cb":            true
# 		"dr_cb":                true
# 		"metadata.broker.list": options.host

# 	p.on "delivery-report", (error, dr) -> total++

# 	p.once "ready", ->
# 		async.eachSeries [1..amount], (i, cb) ->
# 			p.produce options.topic, null, Buffer.from JSON.stringify count: i, source: "origin"
# 			setTimeout cb, 10

# 	p.connect()

# 	poll = ->
# 		timeout = setTimeout ->
# 			p.poll()
# 			poll()
# 			console.log "total produced", total
# 			if total is amount
# 				clearTimeout timeout
# 				cb()
# 		, 1000

# 	poll()

# describe "Consumer", ->
# 	describe "Flowing mode", ->
# 		config = getConfig()
# 		total  = 100
# 		count  = 0

# 		after (done) ->
# 			@timeout 60000
# 			cs.destroy null, (error) ->
# 				throw error if error
# 				done()

# 		it "should start & buffer some data", (done) ->
# 			@timeout 60000
# 			cs = new Consumer config
# 			cs.once "ready", ->
# 				produceData config, total, done

# 		it "should get data", (done) ->
# 			@timeout 60000

# 			onData = (data) ->
# 				if ++count is total
# 					cs.removeListener "data", onData
# 					done()

# 			cs.on "data", onData

# 	describe "Back pressure mode", ->
# 		total  = 100
# 		count  = 0

# 		afterEach (done) ->
# 			@timeout 60000
# 			cs.destroy null, (error) ->
# 				throw error if error
# 				done()

# 		it "should get data - fast consumer", (done) ->
# 			@timeout 60000

# 			count  = 0
# 			config = getConfig()
# 			cs     = new Consumer config

# 			pump [
# 				cs
# 				new Writable objectMode: true, write: (obj, enc, cb) ->
# 					if ++count is total
# 						cs.destroy()
# 					setTimeout cb, 1
# 			], (error) ->
# 				throw error if error
# 				done()

# 			produceData config, total

# 		it "should get data - slow consumer", (done) ->
# 			@timeout 60000

# 			count  = 0
# 			config = getConfig()
# 			cs     = new Consumer config

# 			pump [
# 				cs
# 				new Writable objectMode: true, write: (obj, enc, cb) ->
# 					if ++count is total
# 						cs.destroy()
# 					setTimeout cb, 20
# 			], (error) ->
# 				throw error if error
# 				done()

# 			produceData config, total

