<template>
  <div id="search-table">
    <form class="uk-grid-small" uk-grid>
      <div class="uk-width-1-1 uk-inline">
        <v-select :tabindex=1 multiple v-model="search" :options="options" :closeOnSelect=false
                  :onChange="populate" taggable placeholder="Search..."></v-select>
        <button class="clear-button uk-button uk-button-primary uk-button-small" v-if="search.length > 0"
                v-on:click="clearSearch" type="button">Clear
        </button>
      </div>
    </form>
    <table class="uk-table uk-table-small uk-table-striped uk-table-hover uk-table-divider">
      <thead>
      <tr>
        <th v-for="column in columns" v-bind:key="column.name">
          {{ column.name }}
          <span uk-icon="icon: chevron-up"
                v-if="column.sortable && !column.reverse"
                v-on:click="sortByColumn(column)"></span>
          <span uk-icon="icon: chevron-down"
                v-if="column.sortable && column.reverse"
                v-on:click="sortByColumn(column)"></span>
        </th>
      </tr>
      </thead>
      <tbody>
      <tr v-if="listView.length <= 0 && this.list.length > 0">
        <td colspan=4 class="empty"><span uk-icon="icon: warning"></span> Sorry, we could not find the storage you
          are looking for. <a v-on:click="clearSearch">Clear filters.</a></td>
      </tr>
      <tr v-if="this.list.length <= 0 && this.loading">
        <td colspan=4 class="empty loading"><span uk-spinner="ratio:0.5"></span>Loading...</td>
      </tr>
      <tr v-if="this.list.length <= 0 && this.empty">
        <td colspan=4 class="empty loading"><span uk-icon="icon: warning"></span>No storage available</td>
      </tr>
      <tr v-if="this.list.length <= 0 && this.error">
        <td colspan=4 class="empty loading"><span uk-icon="icon: warning"></span>{{ this.error }}</td>
      </tr>
      <tr v-for="item in listView" v-bind:key="item.id">
        <td>
          <router-link class="uk-link-reset" :to="'/storage/'+item.spec.id">
            {{ item.spec.id }}
          </router-link>
        </td>
        <td>{{ item.spec.type }}</td>
        <td>
          <pre v-if="item.spec.options">{{ yaml(item.spec.options) }}</pre>
          <span v-else>-</span>
        </td>
        <td class="status-row">
          <span class="uk-icon-button uk-button-primary" uk-icon="check"></span>{{ item.enabled }}
        </td>
      </tr>
      </tbody>
    </table>
  </div>
</template>

<style>
  .empty {
    text-align: center;
  }

  .loading div {
    margin-right: 10px;
  }

  .detail-link span {
    opacity: 0;
  }

  .detail-link:hover span {
    opacity: 1;
  }

  .v-select .dropdown-menu {
    overflow-y: hidden;
    box-shadow: none;
    border-color: #CDCDCD;
    top: 30px;
  }

  .v-select .vs__actions {
    display: none;
  }

  .clear-button {
    position: absolute;
    z-index: 2;
    top: 0;
    right: 0;
    line-height: 1.5;
    height: 35px;
    border-top-right-radius: 4px;
    border-bottom-right-radius: 4px;
  }

  .status-row a.uk-icon-button {
    width: 20px;
    height: 20px;
  }
</style>

<script>
  import vSelect from 'vue-select';
  import json2yaml from 'json2yaml';

  export default {
    name: 'FeaturesSearchTable',

    components: {
      vSelect
    },

    data: function () {
      return {
        list: [],
        columns: [
          {name: "ID", sortable: true, reverse: false},
          {name: "Type", sortable: false},
          {name: "Options", sortable: false},
          {name: "Enabled", sortable: false}
        ],
        error: null,
        empty: false,
        loading: false,
        search: [],
        searchEntities: ["id", "type"],
        searchSeparator: ':',
        selectedFeature: {},
        options: [],
      }
    },

    created () {
      this.fetchData();
      this.pushSearch();
    },

    watch: {
      '$route': 'pushSearch'
    },

    computed: {
      listView () {
        let self = this;
        if (self.search.length > 0 || self.search.length > 0) {
          return self.list.filter(function (item) {
            return self.search.every(function (query) {
              if (query.indexOf(self.searchSeparator) === -1) {
                return self.searchEntities.some(function (entity) {
                  return item.spec[entity].indexOf(query) > -1;
                });
              } else {
                let entity = query.split(self.searchSeparator)[0];
                let value = query.split(self.searchSeparator)[1];
                if (typeof item.spec[entity] !== "undefined") {
                  return item.spec[entity].indexOf(value) > -1;
                } else {
                  return false;
                }
              }
            })
          });
        } else {
          return self.list;
        }
      }
    },

    methods: {
      fetchData () {
        this.loading = true;
        this.$http.get(process.env.VUE_APP_ROOT_API + '/storage').then(response => {
          this.loading = false;
          if (typeof response.body['storage'] === "undefined") {
            this.empty = true;
          } else {
            this.list = response.body['storage'];
            this.populate();
          }
        }, response => {
          this.loading = false;
          this.error = response.statusText;
        });
      },
      pushSearch () {
        let self = this;
        let entities = self.searchEntities.slice(0);
        entities.forEach(function (entity) {
          if (self.$route.params[entity]) {
            let query = entity + self.searchSeparator + self.$route.params[entity];
            if (typeof self.search !== "undefined") {
              if (self.search.indexOf(query) === -1) {
                self.search.push(query);
              }
            }
          }
        });
      },
      clearSearch () {
        this.search = [];
      },
      updateRoute (route) {
        // TODO: Handle arbitrary search queries
        if (typeof route['id'] !== "undefined") {
          this.$router.push({path: '/storage/' + route['id'], params: route});
        } else {
          this.$router.push({path: '/storage', params: route});
        }
      },
      sortByColumn (column) {
        column.reverse = !column.reverse;
        this.list = this.list.sort(function (a, b) {
          let columnName = column.name.toLowerCase();
          let x = a.spec[columnName];
          let y = b.spec[columnName];
          if (column.reverse) {
            return (x > y) ? 1 : -1;
          } else {
            return (x < y) ? 1 : -1;
          }
        });
      },
      populate () {
        let self = this;
        let entities = self.searchEntities.slice(0);
        let options = [];
        let route = {};
        if (self.search.length > 0) {
          self.search.forEach(function (query) {
            if (query.indexOf(self.searchSeparator) !== -1) {
              entities.forEach(function (entity, i) {
                if (entity.indexOf(query.split(self.searchSeparator)[0]) > -1 && entity !== "tags") {
                  route[entity] = query.split(self.searchSeparator)[1];
                  entities.splice(i, 1);
                }
              });
            }
          });
        }
        self.listView.forEach(function (item) {
          entities.forEach(function (column) {
            if (column === "tags") {
              let tags = item.spec[column];
              tags.forEach(t => self.pushLabel(options, column, t));
            } else {
              self.pushLabel(options, column, item.spec[column]);
            }
          })
        });
        options.sort();
        this.updateRoute(route);
        this.options = options;
      },
      pushLabel (options, column, value) {
        let self = this;
        let label = column + self.searchSeparator + value;
        if (options.indexOf(label) === -1) {
          options.push(label)
        }
      },
      yaml (json) {
        if (typeof(json) !== "undefined") {
          return json2yaml.stringify(json).substring(4);
        }
        return null;
      }
    }
  }
</script>