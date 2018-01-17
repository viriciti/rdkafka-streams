_             = require "underscore"
async         = require "async"
pump          = require "pump"
{ Readable }  = require "stream"
{ Transform } = require "stream"

{ Producer }  = require "../src"
{ getDuplex } = require "../src"

getConfig = ->
	rnd = _.random 1, 10000000

	host:       "localhost:9092"
	topic:      "test-topic-#{rnd}"
	groupId:    "test-groupId-#{rnd}"
	fromOffset: "earliest"

class Burst extends Readable
	constructor: (@total) ->
		super objectMode: true

		@source = "origin"
		@count  = 0
		@isBusy = false

		@timeouts = [ 5, 20, 5, 20, 5 ]

	_read: ->
		return if @isBusy
		@isBusy = true

		test     = => @isBusy

		iteratee = (cb) =>
			@count++

			index   = @count // 100 % @timeouts.length
			timeout = @timeouts[index]
			@isBusy = @push { @source, @count, index, timeout }

			if @count is @total
				@push null
				@isBusy = false
				return cb()

			setTimeout cb, timeout

		callback = (error) =>
			@emit "error", error if error
			@isBusy = false

		async.whilst test, iteratee, callback

describe "getEventbus", ->
	eb = null

	it "should work", (done) ->
		@timeout 60000

		count  = 0
		total  = 1000
		config = getConfig()

		ps = new Producer config
		eb = getDuplex config

		ts = new Transform objectMode: true, transform: (obj, enc, cb) ->
			return cb() if obj.value.source isnt "origin"
			return eb.destroy() if ++count is 500

			setTimeout ->
				cb null, _.extend {}, obj.value, source: "transform"
			, 5


		pump eb, ts, eb, -> done()

		pump [
			new Burst total
			new Producer config
		]
