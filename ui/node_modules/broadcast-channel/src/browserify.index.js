const module = require('./index.es5.js');
const BroadcastChannel = module.BroadcastChannel;
const createLeaderElection = module.createLeaderElection;

window['BroadcastChannel2'] = BroadcastChannel;
window['createLeaderElection'] = createLeaderElection;