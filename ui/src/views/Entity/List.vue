<template>
  <div class="entity-list">
    <div class="uk-container uk-container-expand page">
      <h1 class="uk-heading-primary">Entities</h1>
      <hr>
      <EntitySearchTable/>
    </div>
  </div>
</template>

<style>
  .page {
    margin-top: 1.5em;
  }
</style>

<script>
  import EntitySearchTable from '@/components/Entity/SearchTable.vue'

  export default {
    name: 'entity-list',
    components: {
      EntitySearchTable
    }
  }
</script>
