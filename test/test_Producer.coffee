_             = require "underscore"
async         = require "async"
Kafka         = require "node-rdkafka"
pump          = require "pump"
{ Transform } = require "stream"
{ Readable }  = require "stream"

Producer = require "../src/Producer"

getConfig = ->
	rnd = _.random 1, 10000000

	host:    "localhost:9092"
	topic:   "test-topic-#{rnd}"

class Burst extends Readable
	constructor: (@total) ->
		super objectMode: true

		@count  = 0
		@isBusy = false

		@timeouts = [ 1, 20, 1, 10, 1 ]

	_read: ->
		return if @isBusy
		@isBusy = true

		test     = => @isBusy

		iteratee = (cb) =>
			@count++

			if @count is @total
				@push null
				@isBusy = false
				return cb()

			@isBusy = @push some: data: @count

			index = @count // 100 % @timeouts.length
			timeout = @timeouts[index]
			setTimeout cb, timeout

		callback = (error) =>
			@emit "error", error if error
			@isBusy = false

		async.whilst test, iteratee, callback

describe "Producer", ->
	it "should produce", (done) ->
		@timeout 60000

		ps = new Producer getConfig()

		pump [
			new Burst 1000
			ps
		], (error) ->
			throw error if error

			ps.destroy null, (error) ->
				throw error if error
				done()