export type PluginOptions = {
	/** Defines whether the original declaration should remain. */
	preserve?: boolean
}

export type Plugin = {
	(pluginOptions?: PluginOptions): {
		postcssPlugin: 'postcss-color-hex-alpha'
	}
	postcss: true
}

declare const plugin: Plugin

export default plugin
