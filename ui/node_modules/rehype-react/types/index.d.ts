// Minimum TypeScript Version: 3.8

import {Transformer} from 'unified'
import {Prefix, CreateElementLike} from 'hast-to-hyperscript'
import {Node} from 'unist'

declare namespace rehypeReact {
  type FragmentLike<T> = (props: any) => T | null

  type ComponentPropsWithoutNode = Record<string, unknown>

  interface ComponentPropsWithNode extends ComponentPropsWithoutNode {
    node: Node
  }

  type ComponentProps = ComponentPropsWithoutNode | ComponentPropsWithNode

  type ComponentLike<
    T,
    P extends ComponentPropsWithoutNode = ComponentPropsWithoutNode
  > = (props: P) => T | null

  interface SharedOptions<H extends CreateElementLike> {
    /**
     * How to create elements or components.
     * You should typically pass `React.createElement`
     */
    createElement: H

    /**
     * Create fragments instead of an outer `<div>` if available
     * You should typically pass `React.Fragment`
     */
    Fragment?: FragmentLike<ReturnType<H>>

    /**
     * React key prefix
     *
     * @defaultValue 'h-'
     */
    prefix?: Prefix
  }

  type ComponentOptions<H extends CreateElementLike> =
    | {
        /**
         * Override default elements (such as `<a>`, `<p>`, etcetera) by passing an object mapping tag names to components
         */
        components?: Record<
          string,
          ComponentLike<ReturnType<H>, ComponentPropsWithNode>
        >
        /**
         * Expose HAST Node objects to `node` prop of react components
         *
         * @defaultValue false
         */
        passNode: true
      }
    | {
        components?: Record<string, ComponentLike<ReturnType<H>>>
        passNode?: false
      }

  type Options<H extends CreateElementLike> = SharedOptions<H> &
    ComponentOptions<H>
}

/**
 * Rehype plugin to transform to React
 */
declare function rehypeReact<H extends CreateElementLike>(
  options: rehypeReact.Options<H>
): Transformer

export = rehypeReact
