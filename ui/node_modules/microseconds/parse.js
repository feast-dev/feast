'use strict'

function toString () {
  const microseconds = this.microseconds
  const milliseconds = this.milliseconds
  const seconds = this.seconds
  const minutes = this.minutes
  const hours = this.hours
  const days = this.days

  const parts = [
    {
      name: 'day',
      value: days
    },
    {
      name: 'hour',
      value: hours
    },
    {
      name: 'minute',
      value: minutes
    },
    {
      name: 'second',
      value: seconds
    },
    {
      name: 'millisecond',
      value: milliseconds
    },
    {
      name: 'microsecond',
      value: microseconds
    }
  ]

  const time = []

  for (let i = 0; i < parts.length; i++) {
    const part = parts[i]
    if (part.value === 0) {
      if (!time.length) continue // nothing was added yet

      let broken = false

      for (let j = i; j < parts.length; j++) {
        const p = parts[j]
        if (p.value) {
          broken = true
          break
        }
      }

      if (!broken) break
    }

    time.push(part.value, part.value === 1 ? part.name : part.name + 's')
  }

  return time.join(' ')
}

module.exports = (nano) => {
  const ms = nano / 1000
  const ss = ms / 1000
  const mm = ss / 60
  const hh = mm / 60
  const dd = hh / 24

  const microseconds = Math.round((ms % 1) * 1000)
  const milliseconds = Math.floor(ms % 1000)
  const seconds = Math.floor(ss % 60)
  const minutes = Math.floor(mm % 60)
  const hours = Math.floor(hh % 24)
  const days = Math.floor(dd)

  return {
    microseconds,
    milliseconds,
    seconds,
    minutes,
    hours,
    days,
    toString
  }
}
