<template>
  <div class="feature-detail">
    <div class="breadcrumb uk-container uk-container-expand">
      <ul class="uk-breadcrumb">
        <li><router-link to="/features">Features</router-link></li>
        <li>
          <router-link class="uk-link-reset" :to="'/features/'+feature.spec.entity">
            {{ feature.spec.entity }}
          </router-link>
        </li>
        <li><span>{{ feature.spec.name }}</span></li>
      </ul>
    </div>
    <div class="uk-container uk-container-expand page">
      <h1 class="uk-heading-primary">{{ feature.spec.id }}</h1>
      <hr>
      <ul uk-tab uk-switcher>
        <li class="uk-active"><a href="#">Information</a></li>
        <li><a href="#">Spec</a></li>
        <li><a href="#">Query</a></li>
      </ul>
      <div class="uk-switcher">
        <table class="feature-info uk-table uk-table-small">
          <tbody>
          <tr>
            <td>Entity:</td>
            <td>
              <router-link :to="{path:'/entity/'+feature.spec.entity,params:{name:feature.spec.entity}}">
                {{ feature.spec.entity }}
              </router-link>
            </td>
          </tr>
          <tr>
            <td>Value type:</td>
            <td>{{ feature.spec.valueType }}</td>
          </tr>
          <tr>
            <td>Owner:</td>
            <td>{{ feature.spec.owner }}</td>
          </tr>
          <tr>
            <td>Description:</td>
            <td>{{ feature.spec.description }}</td>
          </tr>
          <tr>
            <td>URI:</td>
            <td><a :href="feature.spec.uri">{{ feature.spec.uri }}</a></td>
          </tr>
          <tr v-if="feature.bigqueryView">
            <td>BigQuery view:</td>
            <td><a :href="feature.bigqueryView">{{ feature.bigqueryView }}</a></td>
          </tr>
          <tr>
            <td>Created:</td>
            <td>{{ feature.created }}</td>
          </tr>
          <tr>
            <td>Last updated:</td>
            <td>{{ feature.lastUpdated }}</td>
          </tr>
          <tr>
            <td>Serving store:</td>
            <td>
              <router-link :to="{path:'/storage/'+feature.spec.dataStores.serving.id,params:{id:feature.spec.dataStores.serving.id}}">
                {{ feature.spec.dataStores.serving.id }}
              </router-link>
            </td>
          </tr>
          <tr>
            <td>Status:</td>
            <td v-if="feature.enabled"><span class="uk-icon-button uk-button-primary" uk-icon="check"></span> OK</td>
            <td v-if="!feature.enabled"><span class="uk-icon-button uk-button-danger" uk-icon="close"></span> NOT OK</td>
          </tr>
          <!--<tr>-->
            <!--<td></td>-->
            <!--<td><a class="uk-button uk-button-danger" href="#disable-feature" uk-toggle>Disable</a></td>-->
          <!--</tr>-->
          </tbody>
        </table>
        <pre>{{ yaml }}</pre>
        <div class="uk-grid">
          <div class="uk-width-1-3">
            <v-select multiple taggable placeholder="Entity IDs" v-model="entityList" :closeOnSelect=false></v-select>
          </div>
          <div class="uk-width-2-3">
            <pre  id="sample-query">{{ sampleQuery }}</pre>
          <button class="uk-button uk-button-default btn">
            <span uk-icon="icon: copy"></span> Copy
          </button>
          </div>
        </div>
        
      </div>
    </div>
    <div id="disable-feature" class="uk-flex-top" uk-modal>
      <div class="uk-modal-dialog uk-margin-auto-vertical">
        <button class="uk-modal-close-default" type="button" uk-close></button>
        <div class="uk-modal-header">
          <h2 class="uk-modal-title">Disable Feature</h2>
        </div>
        <div class="uk-modal-body">
          <p>After this feature is disabled, you will not be able to query it from the serving layer. This will not stop
            the ingestion job nor remove the current data for you.</p>
        </div>
        <div class="uk-modal-footer uk-text-right">
          <button class="uk-button uk-button-default uk-modal-close" type="button">Cancel</button>
          <button class="uk-button uk-button-danger" type="button">Disable</button>
        </div>
      </div>
    </div>
  </div>
</template>
<style>
  .breadcrumb {
    background: #eee;
    padding-top: 5px;
    padding-bottom: 5px;
  }

  .feature-info tr td:first-child {
    text-align: right;
    color: #333;
    font-size: 0.875rem;
    font-weight: normal;
    text-transform: uppercase;
    width: 155px;
  }

  span.uk-icon-button {
    width: 20px;
    height: 20px;
    position: relative;
    top: -2px;
  }

  .dropdown-menu {
    visibility: hidden;
  }
</style>

<script>
  import vSelect from 'vue-select';
  import filters from '@/filters';
  import json2yaml from 'json2yaml';
  import ClipboardJS from 'clipboard';

  export default {
    name: 'feature-details',

    components: {
      vSelect
    },

    data: function () {
      return {
        feature: {
          spec: {
            id: "",
            name: "",
            owner: "",
            description: "",
            valueType: "",
            entity: "",
            group: "",
            tags: [
              ""
            ],
            dataStores: {
              serving: "",
              warehouse: ""
            },
          },
          rawSpec: {},
          bigqueryView: "",
          enabled: true,
          lastUpdated: ""
        },
        yaml: '',
        entityList: [],
        btn: "Copy"
      }
    },

    created() {
      this.fetchData();
      new ClipboardJS('.btn', {
        text: function(){
          return document.getElementById('sample-query').innerHTML;
        }
      });
    },

    computed: {
      sampleQuery(){
        return `{
  "entityName": "${ this.feature.spec.entity }",
  "entityId": [${ '"' + this.entityList.join('","') + '"' }],
  "requestDetails": [{
    "featureId": "${ this.feature.spec.id }"
  }]
}`
      }
    },

    methods: {
      fetchData() {
        let featureId = this.$route.params.id;
        this.$http.get(process.env.VUE_APP_ROOT_API + '/features/'+featureId).then(response => {
          this.feature = response.body['feature'];
          this.yaml = json2yaml.stringify(response.body['rawSpec']);
        }, response => {
          this.error = response.statusText;
        });
      }
    }
  }
</script>
