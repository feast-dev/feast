<template>
  <div class="feature-list">
    <div class="uk-container uk-container-expand page">
      <h1 class="uk-heading-primary">Features</h1>
      <hr>
      <FeatureSearchTable/>
    </div>
  </div>
</template>

<style>
  .page {
    margin-top: 1.5em;
  }
</style>

<script>
  import FeatureSearchTable from '@/components/Feature/SearchTable.vue'

  export default {
    name: 'feature-list',
    components: {
      FeatureSearchTable
    }
  }
</script>
