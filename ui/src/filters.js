export default {
  formatTimestamp (timestamp) {
    let datetime = new Date(timestamp);
    return datetime.toLocaleTimeString('en-US');
  }
}

