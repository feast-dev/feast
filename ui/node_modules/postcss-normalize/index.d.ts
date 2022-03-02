import type * as PostCSS from 'postcss'

export type PluginOptions = {
	/** Determines whether multiple, duplicate insertions are allowed */
	allowDuplicates?: boolean
	/** Defines an override of the project’s browserslist for this plugin. */
	browsers?: string
	/** Defines whether imports from this plugin will always be inserted at the beginning of a CSS file. */
	forceImport?: boolean
}

export type Plugin = {
	(pluginOptions?: PluginOptions): {
		postcssPlugin: 'postcss-normalize'
		Once(root: PostCSS.Root): void
		postcssImport: {
			load(filename: string, importOptions: any): {}
			resolve(id: string, basedir: string, importOptions: any): {}
		}
	}
	postcss: true
}

declare const plugin: Plugin

export default plugin
