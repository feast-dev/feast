import Vue from 'vue'
import Router from 'vue-router'

import FeatureList from './views/Feature/List.vue'
import FeatureDetails from './views/Feature/Details.vue'
import EntityList from './views/Entity/List.vue'
import StorageList from './views/Storage/List.vue'

Vue.use(Router);

export default new Router({
  routes: [
    {
      path: '/',
      name: 'home',
      component: FeatureList
    },
    {
      path: '/features',
      name: 'features',
      component: FeatureList
    },
    {
      path: '/features/:entity',
      name: 'features-by-entity',
      component: FeatureList
    },
    {
      path: '/features/:entity/:granularity',
      name: 'features-by-entity-granularity',
      component: FeatureList
    },
    {
      path: '/feature/:id',
      name: 'feature-details',
      component: FeatureDetails
    },
    {
      path: '/entities',
      name: 'entities',
      component: EntityList
    },
    {
      path: '/entity/:name',
      name: 'entity-details',
      component: EntityList
    },
    {
      path: '/storage',
      name: 'storage',
      component: StorageList
    },
    {
      path: '/storage/:id',
      name: 'storage-details',
      component: StorageList
    },
  ]
})
