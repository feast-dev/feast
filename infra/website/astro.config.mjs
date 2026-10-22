// @ts-check
import { defineConfig } from 'astro/config';

// https://astro.build/config
export default defineConfig({
  site: 'https://feast.dev',
  integrations: [],
  markdown: {
    shikiConfig: {
      theme: 'github-dark',
      langs: ['python'],
      wrap: true
    }
  },
});
