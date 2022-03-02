# Strict Event Emitter

`EventEmitter` mirror that restricts emitting/handling events other than specified in an interface.

## Motivation

The type definitions for the native `EventEmitter` class of `events` accepts a general string as the event type, making it hard to ensure an unknown event hasn't been dispatched or subscribed to.

Strict Event Emitter is a 100% compatible extension of the native `EventEmitter` that comes with an enhanced type definitions to support restricting of event types an emitter can emit and create listeners.

```ts
import { StrictEventEmitter } from 'strict-event-emitter'

interface EventsMap {
  'request': (req: Request) => void
  'response': (res: Response) => void
}

const emitter = new StrictEventEmitter<EventsMap>()

emitter.emit('request', new Request()) // OK
emitter.emit('request', 2) // ERROR!
emitter.emit('unknown-event') // ERROR!

emitter.addListener('response', (res) => { ... }) // OK
emitter.addListener('unknown-event') // ERROR!
```

> Note that `strict-event-emitter` is a type extension and does not provide any value validation on runtime.

## Getting started

```bash
$ npm install strict-event-emitter
```

## License

MIT
