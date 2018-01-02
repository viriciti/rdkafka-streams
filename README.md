# rdkafka-streams

## Usage

### Consumer
```javascript
const { Consumer } = require('rdkafka-streams')

const cs = new Consumer({
  host: 'localhost:9092',
  topic: 'test',
  groupId: 'test'
})

const ws = new Writable({
  objectMode: true,
  write: (obj, enc, cb) => {
    if (++count === total) {
      return cs.destroy()
    }
    setTimeout(cb, 1)
  }

cs.pipe(ws)
```
### Producer
```javascript
const { Producer } = require('rdkafka-streams')

const ps = new Producer({
  host: 'localhost:9092',
  topic: 'test'
})

ps.write({ some: 'data' })
```
### Duplex
```
const { getDuplex } = require('rdkafka-streams')

ts = new Transform({
  objectMode: true,
  transform: (obj, enc, cb) => {
    if (obj.value.source !== "origin") {
      return cb()
    }

    if (++count === 1000) {
      eb.destroy()
    }

    setTimeout(() => {
      cb(null, _.extend({}, obj.value, { source: "transform" })
    }, 5)
  }
})

ds = getDuplex({
  host: 'localhost:9092',
  topic: 'test',
  groupId: 'test'
})

ds.pipe(ts).pipe(ds)
```
