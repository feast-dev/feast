<template>
  <div class="entity-list">
    <div class="uk-container uk-container-expand page">
      <h1 class="uk-heading-primary">Storage</h1>
      <hr>
      <StorageSearchTable/>
    </div>
  </div>
</template>

<style>
  .page {
    margin-top: 1.5em;
  }
</style>

<script>
  import StorageSearchTable from '@/components/Storage/SearchTable.vue'

  export default {
    name: 'entity-list',
    components: {
      StorageSearchTable
    }
  }
</script>
