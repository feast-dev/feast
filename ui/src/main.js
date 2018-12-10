import Vue from 'vue'
import VueResource from 'vue-resource'
import App from './App.vue'
import router from './router'
import UIkit from 'uikit';
import Icons from 'uikit/dist/js/uikit-icons';


Vue.config.productionTip = false;
UIkit.use(Icons);

Vue.use(VueResource);

new Vue({
  router,
  render: h => h(App)
}).$mount('#app');
