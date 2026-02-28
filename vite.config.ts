import build from '@hono/vite-build/cloudflare-pages'
import devServer from '@hono/vite-dev-server'
import adapter from '@hono/vite-dev-server/cloudflare'
import { defineConfig, Plugin } from 'vite'
import fs from 'fs'
import path from 'path'

// Plugin to write custom _routes.json after build
function routesPlugin(): Plugin {
  return {
    name: 'custom-routes',
    closeBundle() {
      const routes = JSON.stringify({ version: 1, include: ['/api/*'], exclude: [] })
      fs.writeFileSync(path.resolve('dist/_routes.json'), routes)
    }
  }
}

export default defineConfig({
  plugins: [
    build(),
    routesPlugin(),
    devServer({
      adapter,
      entry: 'src/index.tsx'
    })
  ]
})
