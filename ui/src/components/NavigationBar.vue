<template>
  <div class="uk-navbar-container" uk-sticky="bottom: #offset">
    <div class="uk-container uk-container-expand">
      <nav class="uk-navbar" uk-navbar>
        <div class="uk-navbar-left">
          <ul class="uk-navbar-nav">
            <li class="uk-logo"><router-link to="/">FEAST</router-link></li>
          </ul>
        </div>
        <div class="uk-navbar-right">
          <ul class="uk-navbar-nav">
            <li><router-link to="/features"><span uk-icon="icon: nut"></span>Features</router-link></li>
            <li><router-link to="/entities"><span uk-icon="icon: grid"></span>Entities</router-link></li>
            <li><router-link to="/storage"><span uk-icon="icon: database"></span>Storage</router-link></li>
            <!--<li><router-link to="/jobs"><span uk-icon="icon: server"></span>Jobs</router-link></li>-->
          </ul>
        </div>
      </nav>
    </div>
  </div>
</template>

<style>
  .uk-icon {
    margin-right: 5px;
  }
</style>